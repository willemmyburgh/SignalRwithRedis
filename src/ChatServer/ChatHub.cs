using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using StackExchange.Redis;

namespace ChatServer;

public record ClientInfo(string Name, string Server);

public class ChatHub(IConnectionMultiplexer redis, IHubContext<ChatHub> hubContext) : Hub
{
    public static string ServerName { get; } = Environment.MachineName;
    public const int PollOptions = 4;

    private static int _burstRunning;
    private IDatabase Db => redis.GetDatabase();
    private string Name => Context.Items["name"] as string ?? "?";
    private bool IsVirtual => Context.Items["virtual"] is true;

    public override async Task OnConnectedAsync()
    {
        var query = Context.GetHttpContext()!.Request.Query;
        var isVirtual = query["virtual"] == "1";
        Context.Items["virtual"] = isVirtual;
        ServerStats.ConnectionOpened(isVirtual);

        if (!isVirtual)
        {
            var name = query["name"].ToString().Trim();
            if (name.Length == 0) name = "guest-" + Context.ConnectionId[..4];
            Context.Items["name"] = Clean(name, 20);
            await RegisterClient();
            await Clients.Caller.SendAsync("PollUpdate", await GetPoll());
        }
        await Clients.Caller.SendAsync("Connected", ServerName, Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        ServerStats.ConnectionClosed(IsVirtual);
        if (!IsVirtual) await Db.HashDeleteAsync(DemoKeys.Clients, Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }

    private Task RegisterClient() =>
        Db.HashSetAsync(DemoKeys.Clients, Context.ConnectionId, JsonSerializer.Serialize(new ClientInfo(Name, ServerName)));

    private static string Clean(string s, int max) => s.Length <= max ? s : s[..max];

    // ---- identity / latency -------------------------------------------------

    public async Task SetName(string name)
    {
        if (IsVirtual) return;
        Context.Items["name"] = Clean(name.Trim(), 20);
        await RegisterClient();
    }

    /// <summary>Round-trip probe: client -> server only (no backplane). Returns the answering instance.</summary>
    public string Ping() => ServerName;

    // ---- chat ---------------------------------------------------------------

    // Broadcast to every client on every server (via the Redis backplane).
    // sentTicks is echoed back untouched so clients can measure end-to-end latency (used by LoadTest).
    public Task SendMessage(string user, string message, long sentTicks = 0) =>
        Clients.All.SendAsync("ReceiveMessage", user, message, ServerName, sentTicks);

    public Task SendToRoom(string room, string user, string message) =>
        Clients.Group("room:" + Clean(room, 20)).SendAsync("RoomMessage", user, message, ServerName, room);

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

    /// <summary>Ask every device (that opted in) to run a command, e.g. open virtual users.</summary>
    public Task RunOnAll(string command, int a, int b) => Clients.All.SendAsync("Command", command, a, b, Name);

    /// <summary>
    /// Server-side burst: THIS server publishes <paramref name="count"/> messages to all clients on all servers.
    /// Clients count what arrives (loss / ordering / throughput) and report back.
    /// </summary>
    public string StartBurst(int count, int payloadBytes, int ratePerSec)
    {
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
                await hubContext.Clients.All.SendAsync("BurstStart", runId, count, ServerName, starter, payloadBytes);
                await Task.Delay(300);
                var sw = Stopwatch.StartNew();
                for (var i = 1; i <= count; i++)
                {
                    await hubContext.Clients.All.SendAsync("BurstMsg", runId, i, payload);
                    if (ratePerSec > 0)
                    {
                        var ahead = TimeSpan.FromSeconds((double)i / ratePerSec) - sw.Elapsed;
                        if (ahead > TimeSpan.FromMilliseconds(5)) await Task.Delay(ahead);
                    }
                }
                await hubContext.Clients.All.SendAsync("BurstEnd", runId, count, sw.ElapsedMilliseconds);
            }
            finally { Volatile.Write(ref _burstRunning, 0); }
        });
        return runId;
    }

    public Task ReportBurst(string runId, int received, int expected, int ms, int outOfOrder) =>
        Clients.All.SendAsync("BurstReport", Name, ServerName, runId, received, expected, ms, outOfOrder);

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
