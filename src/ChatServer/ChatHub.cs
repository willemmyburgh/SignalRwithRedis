using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using StackExchange.Redis;

namespace ChatServer;

public record ClientInfo(string Name, string Server, string Cid = "");

public class ChatHub(IConnectionMultiplexer redis, IHubContext<ChatHub> hubContext, IHostApplicationLifetime lifetime, RedisHa redisHa) : Hub
{
    public static string ServerName { get; } = Environment.MachineName;
    // Lets open pages notice a newer deployment and reload themselves (see app.js).
    public static long BuildStamp { get; } = new DateTimeOffset(File.GetLastWriteTimeUtc(typeof(ChatHub).Assembly.Location)).ToUnixTimeSeconds();
    public const int PollOptions = 4;

    private static int _burstRunning;
    private static readonly string[] Rooms = ["A", "B", "C"];
    private IDatabase Db => redis.GetDatabase();
    private string Name => Context.Items["name"] as string ?? "?";
    private bool IsVirtual => Context.Items["virtual"] is true;

    public override async Task OnConnectedAsync()
    {
        var query = Context.GetHttpContext()!.Request.Query;
        var isVirtual = query["virtual"] == "1";
        Context.Items["virtual"] = isVirtual;
        ServerStats.ConnectionOpened(isVirtual);
        ConnectionTracker.Add(Context.ConnectionId);

        if (!isVirtual)
        {
            Context.Items["cid"] = Clean(query["cid"].ToString(), 40);
            string wanted;
            if (query["auto"] == "1")
            {
                // No name chosen by the user: best effort = the device's network name, else what the browser told us.
                var http = Context.GetHttpContext()!;
                var known = DeviceNames.Known(http.Request.Headers.UserAgent.ToString());
                var host = known is null ? await DeviceNames.ReverseDns(http) : null;
                wanted = known ?? $"{host ?? Clean(query["device"].ToString(), 20)} · {Clean(query["browser"].ToString(), 12)}";
            }
            else wanted = query["name"].ToString().Trim();
            if (wanted.Length == 0) wanted = "Device";
            // Redis-backed extras are best effort: connections must keep working while Redis fails over.
            try { Context.Items["name"] = await UniqueName(Clean(wanted, 24)); } catch { Context.Items["name"] = Clean(wanted, 24); }
            try { await RegisterClient(); } catch { /* roster entry missing until reconnect */ }
            await Clients.Caller.SendAsync("YourName", Name);
            try { await Clients.Caller.SendAsync("PollUpdate", await GetPoll()); } catch { /* poll unavailable */ }
        }
        await Clients.Caller.SendAsync("Connected", ServerName, Context.ConnectionId, BuildStamp);
        if (ServerStats.Draining) await Clients.Caller.SendAsync("Reconnect", "drain"); // landed here during shutdown: move on
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        ServerStats.ConnectionClosed(IsVirtual);
        ConnectionTracker.Remove(Context.ConnectionId);
        if (!IsVirtual) { try { await Db.HashDeleteAsync(DemoKeys.Clients, Context.ConnectionId); } catch { /* Redis down */ } }
        await base.OnDisconnectedAsync(exception);
    }

    private Task RegisterClient() =>
        Db.HashSetAsync(DemoKeys.Clients, Context.ConnectionId, JsonSerializer.Serialize(new ClientInfo(Name, ServerName, Context.Items["cid"] as string ?? "")));

    private static string Clean(string s, int max) => s.Length <= max ? s : s[..max];

    // ---- identity / latency -------------------------------------------------

    public async Task<string> SetName(string name)
    {
        if (IsVirtual) return "";
        Context.Items["name"] = await UniqueName(Clean(name.Trim().Length == 0 ? "Device" : name.Trim(), 24));
        await RegisterClient();
        return Name;
    }

    /// <summary>
    /// Several browsers/tabs on one device would otherwise share a name, so number them: "Mac · Chrome", "Mac · Chrome #2".
    /// The same tab (same cid) reconnecting after a crash keeps its name.
    /// </summary>
    private async Task<string> UniqueName(string wanted)
    {
        var cid = Context.Items["cid"] as string ?? "";
        var taken = new HashSet<string>();
        foreach (var e in await Db.HashGetAllAsync(DemoKeys.Clients))
        {
            if (e.Name == Context.ConnectionId) continue;
            var c = JsonSerializer.Deserialize<ClientInfo>((string)e.Value!, JsonSerializerOptions.Web);
            if (c is null || (cid.Length > 0 && c.Cid == cid)) continue;
            taken.Add(c.Name);
        }
        if (!taken.Contains(wanted)) return wanted;
        for (var n = 2; ; n++)
        {
            var candidate = $"{wanted} #{n}";
            if (!taken.Contains(candidate)) return candidate;
        }
    }

    /// <summary>Round-trip probe: client -> server only (no backplane). Returns the answering instance.</summary>
    public string Ping() => ServerName;

    // ---- chat ---------------------------------------------------------------

    // Broadcast to every client on every server (via the Redis backplane).
    // sentTicks is echoed back untouched so clients can measure end-to-end latency (used by LoadTest).
    public Task SendMessage(string user, string message, long sentTicks = 0) =>
        Clients.All.SendAsync("ReceiveMessage", user, message, ServerName, sentTicks);

    public Task SendToRoom(string room, string user, string message, long sentTicks = 0) =>
        Clients.Group("room:" + Clean(room, 20)).SendAsync("RoomMessage", user, message, ServerName, room, sentTicks);

    public Task JoinRoom(string room) => Groups.AddToGroupAsync(Context.ConnectionId, "room:" + Clean(room, 20));
    public Task LeaveRoom(string room) => Groups.RemoveFromGroupAsync(Context.ConnectionId, "room:" + Clean(room, 20));

    // ---- shared canvas ------------------------------------------------------

    public Task Draw(string color, double[] segments) => Clients.Others.SendAsync("Draw", color, segments);
    public Task ClearCanvas() => Clients.All.SendAsync("ClearCanvas");

    // ---- poll (shared state lives in Redis; the backplane only carries the notifications) ----

    private async Task<long[]> GetPoll()
    {
        var entries = await Db.HashGetAllAsync(DemoKeys.Poll);
        var tallies = new long[PollOptions];
        foreach (var e in entries)
            if (int.TryParse((string?)e.Name, out var i) && i is >= 0 and < PollOptions) tallies[i] = (long)e.Value;
        return tallies;
    }

    public async Task Vote(int option)
    {
        if (option is < 0 or >= PollOptions) return;
        await Db.HashIncrementAsync(DemoKeys.Poll, option);
        await Clients.All.SendAsync("PollUpdate", await GetPoll());
    }

    public async Task ResetPoll()
    {
        await Db.KeyDeleteAsync(DemoKeys.Poll);
        await Clients.All.SendAsync("PollUpdate", new long[PollOptions]);
    }

    public Task React(string emoji) => Clients.All.SendAsync("Reaction", Clean(emoji, 8), ServerName);

    // ---- experiments --------------------------------------------------------

    /// <summary>Tiny message used by the "flood" experiment; the sender times its own copy coming back.</summary>
    public Task Noise(string from, double sentAt) => Clients.All.SendAsync("Noise", from, ServerName, sentAt);
    public Task NoiseToRoom(string room, string from, double sentAt) => Clients.Group("room:" + Clean(room, 20)).SendAsync("Noise", from, ServerName, sentAt);

    /// <summary>Ask every device (that opted in) to run a command, e.g. open virtual users.</summary>
    public Task RunOnAll(string command, int a, int b, string arg = "") => Clients.All.SendAsync("Command", command, a, b, Name, arg);

    /// <summary>
    /// Server-side burst: THIS server publishes <paramref name="count"/> messages to all clients on all servers.
    /// Clients count what arrives (loss / ordering / throughput) and report back.
    /// </summary>
    public string StartBurst(int count, int payloadBytes, int ratePerSec, string audience = "")
    {
        if (audience != "" && audience != "*") audience = Clean(audience, 20);
        count = Math.Clamp(count, 1, 20_000);
        payloadBytes = Math.Clamp(payloadBytes, 0, 16_384);
        ratePerSec = Math.Clamp(ratePerSec, 0, 50_000);
        if (Interlocked.CompareExchange(ref _burstRunning, 1, 0) != 0) return "busy";

        var runId = Guid.NewGuid().ToString("N")[..6];
        var starter = Name;
        _ = Task.Run(async () =>
        {
            try
            {
                var payload = new string('x', payloadBytes);
                var perRoom = new Dictionary<string, int>();
                await hubContext.Clients.All.SendAsync("BurstStart", runId, count, ServerName, starter, payloadBytes, audience);
                await Task.Delay(300);
                var sw = Stopwatch.StartNew();
                for (var i = 1; i <= count; i++)
                {
                    if (audience == "")
                        await hubContext.Clients.All.SendAsync("BurstMsg", runId, i, payload);
                    else
                    {
                        // "*" = a random room per message; otherwise a fixed room name.
                        var room = audience == "*" ? Rooms[Random.Shared.Next(Rooms.Length)] : audience;
                        perRoom[room] = perRoom.GetValueOrDefault(room) + 1;
                        await hubContext.Clients.Group("room:" + room).SendAsync("BurstMsg", runId, i, payload);
                    }
                    if (ratePerSec > 0)
                    {
                        var ahead = TimeSpan.FromSeconds((double)i / ratePerSec) - sw.Elapsed;
                        if (ahead > TimeSpan.FromMilliseconds(5)) await Task.Delay(ahead);
                    }
                }
                await hubContext.Clients.All.SendAsync("BurstEnd", runId, count, sw.ElapsedMilliseconds, perRoom);
            }
            finally { Volatile.Write(ref _burstRunning, 0); }
        });
        return runId;
    }

    public Task ReportBurst(string runId, int received, int expected, int ms, int outOfOrder) =>
        Clients.All.SendAsync("BurstReport", Name, ServerName, runId, received, expected, ms, outOfOrder);

    /// <summary>Turn automatic connection rebalancing on/off for the whole cluster.</summary>
    public Task SetRebalance(bool enabled) => Db.StringSetAsync(DemoKeys.Rebalance, enabled ? "1" : "0");

    /// <summary>Graceful: drain this instance (clients are moved away first), then let it exit. Docker restarts it.</summary>
    public Task GracefulShutdown()
    {
        _ = Task.Run(async () => { await Task.Delay(200); lifetime.StopApplication(); });
        return Task.CompletedTask;
    }

    /// <summary>Redis HA: ask Sentinel to promote a replica to primary right now.</summary>
    public async Task<string> FailoverRedis()
    {
        var result = await redisHa.FailoverAsync();
        await Clients.All.SendAsync("Notice", $"🔁 {result} (requested by {Name})");
        return result;
    }

    /// <summary>Chaos: kill the instance handling this call. Docker restarts it; clients fail over.</summary>
    public async Task CrashServer()
    {
        await Clients.All.SendAsync("Notice", $"💥 {ServerName[..Math.Min(6, ServerName.Length)]} is crashing (requested by {Name})");
        _ = Task.Run(async () => { await Task.Delay(300); Environment.Exit(1); });
    }
}

public static class DemoKeys
{
    public const string Clients = "demo:clients";
    public const string Servers = "demo:servers";
    public const string Poll = "demo:poll";
    public const string Rebalance = "demo:rebalance";
}

public class CountingFilter : IHubFilter
{
    public async ValueTask<object?> InvokeMethodAsync(HubInvocationContext context, Func<HubInvocationContext, ValueTask<object?>> next)
    {
        ServerStats.Invocation();
        return await next(context);
    }
}

public static class ServerStats
{
    private static int _connections, _virtual;
    private static long _invocations;

    public static volatile bool Draining, Stopped;
    public static int Connections => _connections;
    public static int VirtualConnections => _virtual;
    public static long Invocations => Interlocked.Read(ref _invocations);

    public static void ConnectionOpened(bool isVirtual)
    {
        Interlocked.Increment(ref _connections);
        if (isVirtual) Interlocked.Increment(ref _virtual);
    }

    public static void ConnectionClosed(bool isVirtual)
    {
        Interlocked.Decrement(ref _connections);
        if (isVirtual) Interlocked.Decrement(ref _virtual);
    }

    public static void Invocation() => Interlocked.Increment(ref _invocations);
}

/// <summary>Ids of the connections currently attached to THIS server instance.</summary>
public static class ConnectionTracker
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> Ids = new();
    public static void Add(string id) => Ids[id] = 0;
    public static void Remove(string id) => Ids.TryRemove(id, out _);
    public static string[] Sample(int count)
    {
        var all = Ids.Keys.ToArray();
        Random.Shared.Shuffle(all);
        return count >= all.Length ? all : all.Take(count).ToArray();
    }
}

public static class DeviceNames
{
    /// <summary>
    /// devices.json: [{"match": "regex on the User-Agent", "name": "Willem's iPhone"}, ...], first match wins.
    /// Re-read on every connection, so edits apply without a restart.
    /// </summary>
    public static string? Known(string userAgent)
    {
        try
        {
            var path = Path.Combine(AppContext.BaseDirectory, "devices.json");
            if (!File.Exists(path)) return null;
            foreach (var d in JsonSerializer.Deserialize<List<KnownDevice>>(File.ReadAllText(path), JsonSerializerOptions.Web) ?? [])
                if (!string.IsNullOrEmpty(d.Match) && System.Text.RegularExpressions.Regex.IsMatch(userAgent, d.Match)) return d.Name;
        }
        catch { /* bad file: fall back to automatic names */ }
        return null;
    }
    private record KnownDevice(string Match, string Name);

    /// <summary>
    /// Reverse-DNS of the client's address (as seen by the load balancer). On a home/office LAN the router often
    /// knows device names like "Willems-iPhone". Returns null when unknown or when the address is only the Docker gateway.
    /// </summary>
    public static async Task<string?> ReverseDns(HttpContext http)
    {
        var fwd = http.Request.Headers["X-Forwarded-For"].ToString().Split(',')[0].Trim();
        if (!System.Net.IPAddress.TryParse(fwd, out var ip)) ip = http.Connection.RemoteIpAddress;
        if (ip is null || System.Net.IPAddress.IsLoopback(ip)) return null;
        var b = ip.MapToIPv4().GetAddressBytes();
        if (b[0] == 172 && b[1] is >= 16 and <= 31) return null;      // Docker bridge networks
        if (b[0] == 192 && b[1] == 168 && b[2] == 65) return null;    // Docker Desktop gateway
        try
        {
            var host = (await System.Net.Dns.GetHostEntryAsync(ip).WaitAsync(TimeSpan.FromSeconds(1.5))).HostName;
            if (System.Net.IPAddress.TryParse(host, out _)) return null;
            host = host.Split('.')[0];
            return host.Length is > 0 and <= 20 ? host : null;
        }
        catch { return null; }
    }
}
