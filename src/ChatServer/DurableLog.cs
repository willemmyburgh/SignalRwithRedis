using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace ChatServer;

public record LogEnvelope(string Id, string Room, string User, string Text, string Origin, long SentTicks);
public record DurableAck(string Id, int Partition, long Offset);

/// <summary>
/// The replicated log (Redpanda, Kafka protocol). A send is acknowledged to the sender only after a majority of
/// brokers hold the message (acks=all, topic min.insync.replicas=2).
/// </summary>
public sealed class ChatLog : IDisposable
{
    public const string Topic = "chat.messages";
    private readonly string? _bootstrap;
    private readonly ILogger<ChatLog> _log;
    private readonly Lazy<IProducer<string, byte[]>> _producer;
    private readonly Lazy<IAdminClient> _admin;

    public ChatLog(IConfiguration config, ILogger<ChatLog> log)
    {
        _bootstrap = config["Kafka:Bootstrap"];
        _log = log;
        _producer = new(() => new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = _bootstrap,
            ClientId = "chat-" + ChatHub.ServerName,
            Acks = Acks.All,
            EnableIdempotence = true,       // retries cannot create duplicates or reorder within a partition
            LingerMs = 1,
            MessageTimeoutMs = 8000,        // gives up (and tells the sender) after 8 s
            RequestTimeoutMs = 4000,
            SocketKeepaliveEnable = true,
        }).SetLogHandler((_, m) => { if (m.Level <= SyslogLevel.Warning) _log.LogWarning("kafka producer: {Message}", m.Message); }).Build());
        _admin = new(() => new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrap }).Build());
    }

    public bool Enabled => !string.IsNullOrWhiteSpace(_bootstrap);
    public string Bootstrap => _bootstrap ?? "";

    public ConsumerConfig ConsumerConfig(string groupId) => new()
    {
        BootstrapServers = _bootstrap,
        GroupId = groupId,
        EnableAutoCommit = false,
        AutoOffsetReset = AutoOffsetReset.Latest,
        FetchWaitMaxMs = 10,
        SocketKeepaliveEnable = true,
    };

    public async Task<DurableAck> AppendAsync(LogEnvelope e)
    {
        if (!Enabled) throw new HubException("durable log is not configured");
        var key = e.Room == "" ? "all" : "room:" + e.Room;   // a room maps to one partition, so it stays ordered
        try
        {
            var r = await _producer.Value.ProduceAsync(Topic, new Message<string, byte[]> { Key = key, Value = JsonSerializer.SerializeToUtf8Bytes(e) });
            ServerStats.LogAppended();
            return new DurableAck(e.Id, r.Partition.Value, r.Offset.Value);
        }
        catch (ProduceException<string, byte[]> ex)
        {
            throw new HubException("not persisted: " + ex.Error.Reason);   // the client keeps the message and retries with the same id
        }
    }

    public (bool Up, int Brokers, int Partitions) Status()
    {
        if (!Enabled) return (false, 0, 0);
        try
        {
            var md = _admin.Value.GetMetadata(Topic, TimeSpan.FromSeconds(2));
            var t = md.Topics.FirstOrDefault();
            return (true, md.Brokers.Count, t is { Error.IsError: false } ? t.Partitions.Count : 0);
        }
        catch { return (false, 0, 0); }
    }

    public int[] PartitionIds()
    {
        var md = _admin.Value.GetMetadata(Topic, TimeSpan.FromSeconds(5));
        var t = md.Topics.First();
        if (t.Error.IsError) throw new InvalidOperationException(t.Error.Reason);
        return t.Partitions.Select(p => p.PartitionId).ToArray();
    }

    public void Dispose()
    {
        if (_producer.IsValueCreated) { try { _producer.Value.Flush(TimeSpan.FromSeconds(3)); } catch { } _producer.Value.Dispose(); }
        if (_admin.IsValueCreated) _admin.Value.Dispose();
    }
}

/// <summary>Per-connection state while a resume replays history: live messages wait here so there is no gap and no reordering.</summary>
public sealed class ResumeGate
{
    public readonly object Lock = new();
    public bool Buffering = true;
    public List<(int Partition, long Offset, SerializedHubMessage Message)> Queue = [];
}

/// <summary>
/// One writer per connection. The log tail only enqueues, so a slow or half-dead client can never stall delivery to
/// everyone else. A client whose queue overflows is disconnected and simply resumes from its offset.
/// </summary>
public sealed class ConnectionPump
{
    private readonly Channel<SerializedHubMessage> _queue = Channel.CreateBounded<SerializedHubMessage>(
        new BoundedChannelOptions(20000) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

    public ConnectionPump(HubConnectionContext connection)
    {
        _ = Task.Run(async () =>
        {
            try { await foreach (var m in _queue.Reader.ReadAllAsync()) await connection.WriteAsync(m); }
            catch { /* the connection is going away; the client resumes elsewhere */ }
        });
    }

    public bool TryEnqueue(SerializedHubMessage m) => _queue.Writer.TryWrite(m);
    public ValueTask EnqueueAsync(SerializedHubMessage m) => _queue.Writer.WriteAsync(m);
    public void Complete() => _queue.Writer.TryComplete();
}

/// <summary>The connections attached to THIS server, so log messages can be written without going through Redis.</summary>
public sealed class LocalConnections
{
    public readonly ConcurrentDictionary<string, HubConnectionContext> All = new();
    public readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> Rooms = new();
    public readonly ConcurrentDictionary<string, ResumeGate> Gates = new();
    public readonly ConcurrentDictionary<string, ConnectionPump> Pumps = new();

    public void Join(string room, string id) => Rooms.GetOrAdd(room, _ => new()).TryAdd(id, 0);
    public void Leave(string room, string id) { if (Rooms.TryGetValue(room, out var s)) s.TryRemove(id, out _); }
    public IEnumerable<string> RoomsOf(string id) => Rooms.Where(kv => kv.Value.ContainsKey(id)).Select(kv => kv.Key);
    public void Remove(string id)
    {
        All.TryRemove(id, out _);
        Gates.TryRemove(id, out _);
        if (Pumps.TryRemove(id, out var pump)) pump.Complete();
        foreach (var kv in Rooms) kv.Value.TryRemove(id, out _);
    }
}

/// <summary>Delivers log messages to the local sockets and replays history for reconnecting clients.</summary>
public sealed class LogDelivery(LocalConnections local, ChatLog chatLog, ILogger<LogDelivery> log)
{
    private static readonly SemaphoreSlim ReplayLimit = new(8);

    private static SerializedHubMessage Build(int partition, long offset, DateTime ts, LogEnvelope e) =>
        new(new InvocationMessage("LogMessage",
            [partition, offset, e.Id, e.Room, e.User, e.Text, e.Origin, e.SentTicks, new DateTimeOffset(ts, TimeSpan.Zero).ToUnixTimeMilliseconds()]));

    /// <summary>Called by the tail consumer for every message appended anywhere in the cluster.</summary>
    public async Task DispatchAsync(int partition, long offset, DateTime ts, LogEnvelope e)
    {
        var msg = Build(partition, offset, ts, e);
        IEnumerable<string> ids = e.Room == "" ? local.All.Keys : local.Rooms.TryGetValue(e.Room, out var set) ? set.Keys : [];
        foreach (var id in ids)
        {
            if (!local.All.TryGetValue(id, out var conn)) continue;
            if (local.Gates.TryGetValue(id, out var gate))
            {
                lock (gate.Lock) { if (gate.Buffering) { gate.Queue.Add((partition, offset, msg)); continue; } }
            }
            if (!local.Pumps.TryGetValue(id, out var pump)) continue;
            if (!pump.TryEnqueue(msg)) conn.Abort();   // hopelessly behind: cut it loose, it resumes from its offset
        }
        ServerStats.LogDelivered();
    }

    private async Task Write(string connectionId, SerializedHubMessage msg)
    {
        if (local.Pumps.TryGetValue(connectionId, out var pump)) await pump.EnqueueAsync(msg);
    }

    /// <summary>
    /// A client reconnected and reports the last offset it saw per partition: replay everything after that (for its
    /// audience), then switch to live. Live messages arriving meanwhile are held and merged by offset.
    /// </summary>
    public async Task ResumeAsync(string connectionId, Dictionary<int, long> last)
    {
        if (!chatLog.Enabled || !local.All.ContainsKey(connectionId)) return;
        var gate = new ResumeGate();
        local.Gates[connectionId] = gate;
        try
        {
            var rooms = local.RoomsOf(connectionId).ToHashSet();
            var high = new Dictionary<int, long>();
            var batch = new List<(DateTime Ts, int P, long O, LogEnvelope E)>();
            var truncated = false;
            if (last.Count > 0)
            {
                await ReplayLimit.WaitAsync();
                try { truncated = await Task.Run(() => Collect(last, rooms, high, batch)); }
                finally { ReplayLimit.Release(); }
            }
            foreach (var m in batch.OrderBy(b => b.Ts).ThenBy(b => b.P).ThenBy(b => b.O))
                await Write(connectionId, Build(m.P, m.O, m.Ts, m.E));
            if (batch.Count > 0 || truncated)
                await Write(connectionId, new SerializedHubMessage(new InvocationMessage("Replayed", [batch.Count, truncated])));

            // Flush what arrived live during the replay (skipping what the replay already covered), then go live.
            while (true)
            {
                List<(int Partition, long Offset, SerializedHubMessage Message)> items;
                lock (gate.Lock)
                {
                    if (gate.Queue.Count == 0) { gate.Buffering = false; break; }
                    items = gate.Queue; gate.Queue = [];
                }
                foreach (var q in items)
                    if (!high.TryGetValue(q.Partition, out var h) || q.Offset >= h) await Write(connectionId, q.Message);
            }
            ServerStats.LogReplayed(batch.Count);
        }
        catch (Exception ex)
        {
            log.LogWarning("resume failed for {Id}: {Message}", connectionId, ex.Message);
        }
        finally { local.Gates.TryRemove(connectionId, out _); }
    }

    private bool Collect(Dictionary<int, long> last, HashSet<string> rooms, Dictionary<int, long> high, List<(DateTime, int, long, LogEnvelope)> batch)
    {
        const int cap = 5000;
        var truncated = false;
        using var c = new ConsumerBuilder<string, byte[]>(chatLog.ConsumerConfig("replay-" + Guid.NewGuid().ToString("N"))).Build();
        var assign = new List<TopicPartitionOffset>();
        var targets = new Dictionary<int, long>();
        foreach (var (p, lastOffset) in last)
        {
            var tp = new TopicPartition(ChatLog.Topic, p);
            var wm = c.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(3));
            high[p] = wm.High.Value;
            if (lastOffset + 1 < wm.Low.Value) truncated = true;      // history older than retention is gone
            var start = Math.Max(lastOffset + 1, wm.Low.Value);
            if (start < wm.High.Value) { assign.Add(new TopicPartitionOffset(tp, start)); targets[p] = wm.High.Value; }
        }
        if (assign.Count == 0) return truncated;
        c.Assign(assign);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (targets.Count > 0 && DateTime.UtcNow < deadline && batch.Count < cap)
        {
            var r = c.Consume(TimeSpan.FromMilliseconds(100));
            if (r?.Message is null) continue;
            var p = r.Partition.Value;
            var e = JsonSerializer.Deserialize<LogEnvelope>(r.Message.Value, JsonSerializerOptions.Web);
            if (e is not null && (e.Room == "" || rooms.Contains(e.Room))) batch.Add((r.Message.Timestamp.UtcDateTime, p, r.Offset.Value, e));
            if (targets.TryGetValue(p, out var h) && r.Offset.Value + 1 >= h) targets.Remove(p);
        }
        return truncated;
    }
}

/// <summary>
/// Every server reads the whole log from "now" and writes each message to its own sockets. It remembers the next
/// offset per partition, and a watchdog restarts the consumer from that position if it stalls (for example after the
/// brokers lost quorum), so the messages in between are still delivered, in order, with nothing skipped or repeated.
/// </summary>
public sealed class ChatLogTail(ChatLog chatLog, LogDelivery delivery, ILogger<ChatLogTail> log) : BackgroundService
{
    private readonly Dictionary<int, long> _next = new();
    private DateTime _lastConsumed = DateTime.UtcNow;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (!chatLog.Enabled) return;
        await Task.Yield();
        while (!ct.IsCancellationRequested)
        {
            try { await Run(ct); }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                ServerStats.LogTailRestarted();
                log.LogWarning("log tail restarting from stored offsets: {Message}", ex.Message);
                await Task.Delay(1000, ct);
            }
        }
    }

    private async Task Run(CancellationToken ct)
    {
        var partitions = chatLog.PartitionIds();
        using var c = new ConsumerBuilder<string, byte[]>(chatLog.ConsumerConfig("tail-" + ChatHub.ServerName))
            .SetErrorHandler((_, e) => log.LogWarning("kafka consumer: {Reason}", e.Reason)).Build();
        var assign = new List<TopicPartitionOffset>();
        foreach (var p in partitions)
        {
            var tp = new TopicPartition(ChatLog.Topic, p);
            var wm = c.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
            _next[p] = _next.TryGetValue(p, out var n) ? Math.Max(n, wm.Low.Value) : wm.High.Value;   // first start: from now
            assign.Add(new TopicPartitionOffset(tp, _next[p]));
        }
        c.Assign(assign);
        _lastConsumed = DateTime.UtcNow;
        log.LogWarning("tailing {Topic}: {Count} partitions", ChatLog.Topic, partitions.Length);
        var nextCheck = DateTime.UtcNow.AddSeconds(5);
        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? r = null;
            try { r = c.Consume(TimeSpan.FromMilliseconds(100)); }
            catch (ConsumeException ex) { log.LogDebug("consume: {Reason}", ex.Error.Reason); }
            if (r?.Message is not null)
            {
                var e = JsonSerializer.Deserialize<LogEnvelope>(r.Message.Value, JsonSerializerOptions.Web);
                if (e is not null) await delivery.DispatchAsync(r.Partition.Value, r.Offset.Value, r.Message.Timestamp.UtcDateTime, e);
                _next[r.Partition.Value] = r.Offset.Value + 1;
                _lastConsumed = DateTime.UtcNow;
            }
            if (DateTime.UtcNow < nextCheck) continue;
            nextCheck = DateTime.UtcNow.AddSeconds(5);
            // Watchdog: the brokers hold records we have not read, and nothing has arrived for 5 s => stalled.
            if (DateTime.UtcNow - _lastConsumed < TimeSpan.FromSeconds(5)) continue;
            foreach (var p in partitions)
            {
                WatermarkOffsets wm;
                try { wm = c.QueryWatermarkOffsets(new TopicPartition(ChatLog.Topic, p), TimeSpan.FromSeconds(2)); }
                catch { break; }   // brokers unreachable right now: not a stall, just an outage
                if (wm.High.Value > _next[p]) throw new InvalidOperationException($"partition {p} stalled at {_next[p]}, log end {wm.High.Value}");
            }
        }
    }
}

/// <summary>
/// Wraps the Redis lifetime manager (everything else still goes through the Redis backplane) and remembers which
/// connections are local, so the log can be delivered without Redis.
/// </summary>
public sealed class TrackingLifetimeManager<THub> : HubLifetimeManager<THub>, IDisposable where THub : Hub
{
    private readonly HubLifetimeManager<THub> _inner;
    private readonly LocalConnections _local;

    public TrackingLifetimeManager(IServiceProvider sp, LocalConnections local)
    {
        _local = local;
        _inner = ActivatorUtilities.CreateInstance<Microsoft.AspNetCore.SignalR.StackExchangeRedis.RedisHubLifetimeManager<THub>>(sp);
    }

    public override Task OnConnectedAsync(HubConnectionContext connection) { _local.Pumps[connection.ConnectionId] = new ConnectionPump(connection); _local.All[connection.ConnectionId] = connection; return _inner.OnConnectedAsync(connection); }
    public override Task OnDisconnectedAsync(HubConnectionContext connection) { _local.Remove(connection.ConnectionId); return _inner.OnDisconnectedAsync(connection); }
    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken ct = default) => _inner.SendAllAsync(methodName, args, ct);
    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excluded, CancellationToken ct = default) => _inner.SendAllExceptAsync(methodName, args, excluded, ct);
    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken ct = default) => _inner.SendConnectionAsync(connectionId, methodName, args, ct);
    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken ct = default) => _inner.SendConnectionsAsync(connectionIds, methodName, args, ct);
    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken ct = default) => _inner.SendGroupAsync(groupName, methodName, args, ct);
    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args, CancellationToken ct = default) => _inner.SendGroupsAsync(groupNames, methodName, args, ct);
    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args, IReadOnlyList<string> excluded, CancellationToken ct = default) => _inner.SendGroupExceptAsync(groupName, methodName, args, excluded, ct);
    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken ct = default) => _inner.SendUserAsync(userId, methodName, args, ct);
    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args, CancellationToken ct = default) => _inner.SendUsersAsync(userIds, methodName, args, ct);
    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken ct = default) => _inner.AddToGroupAsync(connectionId, groupName, ct);
    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken ct = default) => _inner.RemoveFromGroupAsync(connectionId, groupName, ct);
    public override Task SetConnectionResultAsync(string connectionId, CompletionMessage result) => _inner.SetConnectionResultAsync(connectionId, result);
    public override bool TryGetReturnType(string invocationId, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Type? type) => _inner.TryGetReturnType(invocationId, out type);
    public void Dispose() => (_inner as IDisposable)?.Dispose();
}
