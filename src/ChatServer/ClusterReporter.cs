using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using StackExchange.Redis;

namespace ChatServer;

public record ServerInfo(string Name, int Connections, int VirtualConnections, double CallsPerSec, long Ts, int Slot, bool Draining = false);

/// <summary>
/// Every server publishes a heartbeat into a Redis hash. The live server with the lowest name
/// (a cheap leader election) aggregates the hash plus the client roster and pushes it to all clients once a second.
/// </summary>
public class ClusterReporter(IConnectionMultiplexer redis, IHubContext<ChatHub> hub, ILogger<ClusterReporter> log) : BackgroundService
{
    private static readonly TimeSpan Stale = TimeSpan.FromSeconds(4);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var db = redis.GetDatabase();
        var me = ChatHub.ServerName;

        // After a crash/restart this container's old roster entries are orphans.
        await RemoveRosterEntries(db, s => s == me);

        var slot = -1;
        long lastCalls = ServerStats.Invocations;
        var last = DateTime.UtcNow;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(ct))
        {
            try
            {
                var now = DateTime.UtcNow;
                var calls = ServerStats.Invocations;
                var rate = (calls - lastCalls) / (now - last).TotalSeconds;
                (lastCalls, last) = (calls, now);

                var live = new List<ServerInfo>();
                var dead = new List<string>();
                var cutoff = DateTimeOffset.UtcNow.Subtract(Stale).ToUnixTimeMilliseconds();
                foreach (var e in await db.HashGetAllAsync(DemoKeys.Servers))
                {
                    var info = JsonSerializer.Deserialize<ServerInfo>((string)e.Value!, JsonSerializerOptions.Web);
                    if (info is null || info.Ts < cutoff) dead.Add(e.Name!);
                    else if (info.Name == me) { if (slot < 0) slot = info.Slot; } // keep my colour across a restart
                    else live.Add(info);
                }

                // Colour slot: unique per live server, stable while it lives. On a clash the lower name keeps the slot.
                if (slot < 0 || live.Any(o => o.Slot == slot && string.CompareOrdinal(o.Name, me) < 0))
                {
                    var used = live.Select(o => o.Slot).ToHashSet();
                    slot = Enumerable.Range(0, int.MaxValue).First(i => !used.Contains(i));
                }

                var mine = new ServerInfo(me, ServerStats.Connections, ServerStats.VirtualConnections, Math.Round(rate, 1), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), slot, ServerStats.Draining);
                if (!ServerStats.Stopped) await db.HashSetAsync(DemoKeys.Servers, me, JsonSerializer.Serialize(mine));
                live.Add(mine);

                var rebalance = (string?)await db.StringGetAsync(DemoKeys.Rebalance) != "0"; // default: on
                if (rebalance && !ServerStats.Draining) await ShedExcess(live.Where(x => !x.Draining).ToList(), mine.Connections);

                if (live.MinBy(s => s.Name, StringComparer.Ordinal)?.Name != me) continue; // not the leader

                if (dead.Count > 0)
                {
                    await db.HashDeleteAsync(DemoKeys.Servers, dead.Select(d => (RedisValue)d).ToArray());
                    await RemoveRosterEntries(db, dead.Contains);
                }

                var clients = (await db.HashGetAllAsync(DemoKeys.Clients))
                    .Select(e => JsonSerializer.Deserialize<ClientInfo>((string)e.Value!, JsonSerializerOptions.Web))
                    .Where(c => c is not null).Take(300).ToList();

                await hub.Clients.All.SendAsync("ClusterStats", new
                {
                    servers = live.OrderBy(s => s.Name, StringComparer.Ordinal),
                    clients,
                    redis = await RedisInfo(),
                    rebalance,
                }, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                log.LogWarning(ex, "cluster report failed");
            }
        }
    }

    private DateTime _lastShed = DateTime.MinValue;

    /// <summary>
    /// Connection rebalancing. WebSockets are long-lived, so a restarted or newly scaled-out server stays empty
    /// while the others stay full. Each server compares its connection count with the cluster average and, when it is
    /// clearly above it, asks a few of its own clients to reconnect. The load balancer (least-connections) then
    /// sends them to the emptiest server. Small batches + a cooldown stop the cluster from over-correcting,
    /// because heartbeats are up to a second stale.
    /// </summary>
    private async Task ShedExcess(List<ServerInfo> live, int mine)
    {
        if (live.Count < 2 || DateTime.UtcNow - _lastShed < TimeSpan.FromSeconds(3)) return;
        var avg = live.Sum(s => s.Connections) / (double)live.Count;
        var min = live.Min(s => s.Connections);
        // Only servers above average shed, and only while the gap to the emptiest server exceeds a small dead band.
        if (mine <= avg || mine - min <= avg * 0.08 + 2) return;

        var excess = mine - (int)Math.Ceiling(avg);
        var shed = Math.Min(50, Math.Max(1, (excess + 1) / 2));
        _lastShed = DateTime.UtcNow;
        log.LogInformation("rebalance: {Mine} connections vs avg {Avg:F0}, asking {Shed} clients to reconnect", mine, avg, shed);
        await hub.Clients.Clients(ConnectionTracker.Sample(shed)).SendAsync("Reconnect");
    }

    private static async Task RemoveRosterEntries(IDatabase db, Func<string, bool> serverPredicate)
    {
        foreach (var e in await db.HashGetAllAsync(DemoKeys.Clients))
        {
            var c = JsonSerializer.Deserialize<ClientInfo>((string)e.Value!, JsonSerializerOptions.Web);
            if (c is not null && serverPredicate(c.Server)) await db.HashDeleteAsync(DemoKeys.Clients, e.Name);
        }
    }

    private async Task<object> RedisInfo()
    {
        try
        {
            // The multiplexer follows the primary (through Sentinel when configured); find it and read its INFO.
            var server = redis.GetEndPoints().Select(ep => redis.GetServer(ep)).FirstOrDefault(sv => sv.IsConnected && !sv.IsReplica)
                         ?? throw new InvalidOperationException("no primary");
            var info = (await server.InfoAsync()).SelectMany(g => g).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
            string Get(string k) => info.TryGetValue(k, out var v) ? v : "0";
            return new
            {
                up = true,
                primary = server.EndPoint.ToString(),
                replicas = int.Parse(Get("connected_slaves")),
                opsPerSec = double.Parse(Get("instantaneous_ops_per_sec")),
                connectedClients = int.Parse(Get("connected_clients")),
                memory = Get("used_memory_human"),
                version = Get("redis_version"),
            };
        }
        catch { return new { up = false }; }
    }
}
