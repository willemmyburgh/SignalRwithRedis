using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;

// Config via env vars (see docker-compose.yml)
string url = Env("TARGET_URL", "http://loadbalancer/chat");
int connections = int.Parse(Env("CONNECTIONS", "500"));
int senders = int.Parse(Env("SENDERS", "5"));          // connections that publish messages
int msgsPerSecPerSender = int.Parse(Env("MSGS_PER_SEC", "5"));
int durationSec = int.Parse(Env("DURATION_SEC", "30"));
int roomCount = Math.Clamp(int.Parse(Env("ROOMS", "3")), 1, 26);        // rooms A, B, C, ...
bool durable = Env("MODE", "atmostonce").ToLowerInvariant() == "durable";  // durable = replicated log + sequence audit
var receivers = new ConcurrentBag<Receiver>();
long replayedTotal = 0;
string audience = Env("AUDIENCE", "everyone").ToLowerInvariant();         // everyone | rooms | mixed (random per message)
var roomNames = Enumerable.Range(0, roomCount).Select(i => ((char)('A' + i)).ToString()).ToArray();
var members = roomNames.ToDictionary(r => r, _ => 0);                     // intended members per room (for the expected count)

Console.WriteLine($"Target {url}: {connections} connections, {senders} senders x {msgsPerSecPerSender} msg/s, {durationSec}s");
Console.WriteLine($"Audience: {audience}" + (audience == "everyone" ? "" : $" ({roomCount} rooms, each connection joins up to 2 at random)"));

var samples = new List<double>();
var sampleLock = new object();
long received = 0, failed = 0;
var perServerConns = new ConcurrentDictionary<string, int>();
var clients = new List<HubConnection>(connections);

// ---- connect ----
var sw = Stopwatch.StartNew();
var gate = new SemaphoreSlim(50); // ramp-up concurrency
var connectTasks = Enumerable.Range(0, connections).Select(async i =>
{
    await gate.WaitAsync();
    try
    {
        var c = new HubConnectionBuilder()
            .WithUrl(url + (url.Contains('?') ? "&" : "?") + "virtual=1", o => { o.SkipNegotiation = true; o.Transports = HttpTransportType.WebSockets; })
            .WithAutomaticReconnect(new ForeverRetry())
            .Build();
        c.KeepAliveInterval = TimeSpan.FromSeconds(5); // server drops clients silent for 15s: ping at least every 7.5s
        c.ServerTimeout = TimeSpan.FromSeconds(15);
        // The cluster asks clients to move when servers are unbalanced.
        c.On<string>("Reconnect", async _ => { await Task.Delay(Random.Shared.Next(1500)); await c.StopAsync(); await c.StartAsync(); });
        c.On<string, string, long>("Connected", (s, _, _) => perServerConns.AddOrUpdate(s, 1, (_, n) => n + 1));
        // Rooms are per connection: (re)join on every connect, including after reconnects.
        var myRooms = roomNames.OrderBy(_ => Random.Shared.Next()).Take(Math.Min(2, roomCount)).ToArray();
        lock (members) foreach (var r in myRooms) members[r]++;
        c.On<string, string, long>("Connected", async (_, _, _) => { foreach (var r in myRooms) await c.InvokeAsync("JoinRoom", r); });
        c.On<string, string, string, string, long>("RoomMessage", (_, _, _, _, ticks) => OnMessage(ticks));
        c.On<string, string, string, long>("ReceiveMessage", (_, _, _, ticks) => OnMessage(ticks));
        void OnMessage(long ticks)
        {
            Interlocked.Increment(ref received);
            if (ticks == 0) return;
            var ms = Stopwatch.GetElapsedTime(ticks).TotalMilliseconds;
            // Reservoir-ish: keep every 20th sample to bound memory
            if (Interlocked.Read(ref received) % 20 == 0)
                lock (sampleLock) samples.Add(ms);
        }
        if (durable)
        {
            var rcv = new Receiver(); receivers.Add(rcv);
            c.On("LogMessage", new[] { typeof(int), typeof(long), typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(long), typeof(long) },
                a => { var ms = rcv.OnLog((int)a[0]!, (long)a[1]!, (string)a[2]!, (string)a[5]!, (long)a[7]!); if (ms >= 0) lock (sampleLock) samples.Add(ms); return Task.CompletedTask; });
            c.On<int, bool>("Replayed", (n, _) => { Interlocked.Add(ref replayedTotal, n); });
            // Every (re)connect: tell the server how far we got, so it replays what we missed.
            c.On<string, string, long>("Connected", async (_, _, _) => { Dictionary<int, long> snap; lock (rcv) snap = new(rcv.Offsets); await c.InvokeAsync("Resume", snap); });
        }
        await c.StartAsync();
        lock (clients) clients.Add(c);
    }
    catch { Interlocked.Increment(ref failed); }
    finally { gate.Release(); }
}).ToArray();
await Task.WhenAll(connectTasks);
Console.WriteLine($"Connected {clients.Count}/{connections} in {sw.Elapsed.TotalSeconds:F1}s ({failed} failed)");
foreach (var (s, n) in perServerConns.OrderBy(k => k.Key)) Console.WriteLine($"  {s}: {n} connections");

if (durable)
{
    // ---- durable mode: every sender numbers its messages; every receiver must end up holding every CONFIRMED number ----
    var senderCount = Math.Min(senders, clients.Count);
    var acked = Enumerable.Range(0, senderCount).Select(_ => new ConcurrentDictionary<int, byte>()).ToArray();
    long dsent = 0, retries = 0;
    using var dcts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSec));
    var hardStop = DateTime.UtcNow.AddSeconds(durationSec + 120);
    var inflight = new ConcurrentBag<Task>();
    async Task SendOne(int idx, int seq)
    {
        var id = $"{idx}-{seq}";
        while (DateTime.UtcNow < hardStop)
        {
            try
            {
                await clients[idx].InvokeAsync<DurableAck>("SendDurable", id, "", $"bot{idx}", $"#audit|{idx}|{seq}", Stopwatch.GetTimestamp());
                acked[idx][seq] = 0; return;
            }
            catch { Interlocked.Increment(ref retries); await Task.Delay(500); }   // not persisted yet: same id again
        }
    }
    var dsend = Enumerable.Range(0, senderCount).Select(async idx =>
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1.0 / msgsPerSecPerSender));
        var seq = 0;
        try { while (await timer.WaitForNextTickAsync(dcts.Token)) { Interlocked.Increment(ref dsent); inflight.Add(SendOne(idx, seq++)); } }
        catch (OperationCanceledException) { }
    }).ToArray();
    var drun = Stopwatch.StartNew();
    while (!dcts.IsCancellationRequested)
    {
        try { await Task.Delay(5000, dcts.Token); } catch (OperationCanceledException) { break; }
        Console.WriteLine($"[{drun.Elapsed.TotalSeconds,5:F0}s] sent={Interlocked.Read(ref dsent):N0} confirmed={acked.Sum(a => a.Count):N0} retries={Interlocked.Read(ref retries):N0}");
    }
    await Task.WhenAll(dsend);
    await Task.WhenAll(inflight);
    await Task.Delay(8000); // let the last records reach everyone (and any replay finish)

    long confirmed = acked.Sum(a => a.Count), missing = 0, dupRecords = 0, replayDup = 0;
    foreach (var r in receivers)
        lock (r)
        {
            dupRecords += r.DupRecords; replayDup += r.ReplayDup;
            for (var i = 0; i < senderCount; i++)
            {
                r.Got.TryGetValue(i, out var got);
                foreach (var q in acked[i].Keys) if (got is null || !got.Contains(q)) missing++;
            }
        }
    var wanted = confirmed * receivers.Count;
    double[] ls; lock (sampleLock) ls = samples.Order().ToArray();
    double Q(double p) => ls.Length == 0 ? double.NaN : ls[Math.Min(ls.Length - 1, (int)(p * ls.Length))];
    Console.WriteLine();
    Console.WriteLine($"Sent:        {Interlocked.Read(ref dsent):N0} numbered messages from {senderCount} senders, {confirmed:N0} confirmed persisted, {Interlocked.Read(ref retries):N0} send retries");
    Console.WriteLine($"Receivers:   {receivers.Count} connections, each must hold every confirmed message = {wanted:N0} deliveries");
    Console.WriteLine($"MISSING:     {missing:N0}   hidden duplicate records: {dupRecords:N0}   replay overlaps: {replayDup:N0}   replayed after reconnects: {Interlocked.Read(ref replayedTotal):N0}");
    Console.WriteLine($"Latency ms:  p50={Q(.5):F1} p95={Q(.95):F1} p99={Q(.99):F1} max={(ls.Length > 0 ? ls[^1] : double.NaN):F1}  (send to delivery, n={ls.Length})");
    Console.WriteLine(missing == 0 ? "RESULT: PASS, nothing confirmed was lost" : "RESULT: FAIL, confirmed messages are missing");
    await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));
    return;
}

// ---- send ----
Interlocked.Exchange(ref received, 0);
long sent = 0, expected = 0;
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSec));
var delay = TimeSpan.FromSeconds(1.0 / msgsPerSecPerSender);
var sendTasks = clients.Take(senders).Select(async (c, idx) =>
{
    using var timer = new PeriodicTimer(delay);
    try
    {
        while (await timer.WaitForNextTickAsync(cts.Token))
        {
            try
            {
                var toRoom = audience == "rooms" || (audience == "mixed" && Random.Shared.Next(2) == 0);
                if (toRoom)
                {
                    var room = roomNames[Random.Shared.Next(roomNames.Length)];
                    await c.InvokeAsync("SendToRoom", room, $"bot{idx}", "ping", Stopwatch.GetTimestamp());
                    Interlocked.Add(ref expected, members[room]);
                }
                else
                {
                    await c.InvokeAsync("SendMessage", $"bot{idx}", "ping", Stopwatch.GetTimestamp());
                    Interlocked.Add(ref expected, clients.Count);
                }
                Interlocked.Increment(ref sent);
            }
            catch (Exception) when (!cts.IsCancellationRequested) { Interlocked.Increment(ref failed); } // e.g. its server crashed
        }
    }
    catch (OperationCanceledException) { }
}).ToArray();

var run = Stopwatch.StartNew();
long last = 0;
while (!cts.IsCancellationRequested)
{
    try { await Task.Delay(5000, cts.Token); } catch (OperationCanceledException) { break; }
    var r = Interlocked.Read(ref received);
    Console.WriteLine($"[{run.Elapsed.TotalSeconds,5:F0}s] sent={sent:N0} received={r:N0} ({(r - last) / 5.0:N0}/s)");
    last = r;
}
await Task.WhenAll(sendTasks);
await Task.Delay(2000); // drain

// ---- report ----
double[] s2; lock (sampleLock) s2 = samples.Order().ToArray();
double P(double p) => s2.Length == 0 ? double.NaN : s2[Math.Min(s2.Length - 1, (int)(p * s2.Length))];
var total = Interlocked.Read(ref received);
Console.WriteLine();
Console.WriteLine($"Published:   {sent:N0} messages");
Console.WriteLine($"Delivered:   {total:N0} / {Interlocked.Read(ref expected):N0} expected ({100.0 * total / Math.Max(1, Interlocked.Read(ref expected)):F2}%)");
Console.WriteLine($"Throughput:  {total / run.Elapsed.TotalSeconds:N0} deliveries/s");
Console.WriteLine($"Latency ms:  p50={P(.5):F1} p95={P(.95):F1} p99={P(.99):F1} max={(s2.Length > 0 ? s2[^1] : double.NaN):F1}  (n={s2.Length})");

await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));

static string Env(string k, string d) => Environment.GetEnvironmentVariable(k) ?? d;

// Keep retrying every 1-3s so clients survive a server crash.
sealed class ForeverRetry : IRetryPolicy
{
    public TimeSpan? NextRetryDelay(RetryContext c) => TimeSpan.FromMilliseconds(1000 + Random.Shared.Next(2000));
}

record DurableAck(string Id, int Partition, long Offset);

/// <summary>What one connection has received from the log: dedupes by offset and by message id, and keeps the sequence numbers per sender.</summary>
sealed class Receiver
{
    public readonly Dictionary<int, long> Offsets = new();
    public readonly Dictionary<int, HashSet<int>> Got = new();
    private readonly HashSet<(int, long)> _seenOffsets = new();
    private readonly HashSet<string> _seenIds = new();
    public long DupRecords, ReplayDup;
    private long _n;

    /// <returns>latency in ms for a sampled message, else -1</returns>
    public double OnLog(int partition, long offset, string id, string text, long sentTicks)
    {
        lock (this)
        {
            if (!_seenOffsets.Add((partition, offset))) { ReplayDup++; return -1; }
            if (!Offsets.TryGetValue(partition, out var cur) || cur < offset) Offsets[partition] = offset;
            if (!_seenIds.Add(id)) { DupRecords++; return -1; }
            var parts = text.Split('|');
            if (parts.Length == 3 && parts[0] == "#audit")
            {
                var sender = int.Parse(parts[1]); var seq = int.Parse(parts[2]);
                if (!Got.TryGetValue(sender, out var set)) Got[sender] = set = new();
                set.Add(seq);
            }
            return sentTicks != 0 && ++_n % 10 == 0 ? Stopwatch.GetElapsedTime(sentTicks).TotalMilliseconds : -1;
        }
    }
}
