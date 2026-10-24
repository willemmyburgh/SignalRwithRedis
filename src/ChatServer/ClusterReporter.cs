using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using StackExchange.Redis;

namespace ChatServer;

public record ServerInfo(string Name, int Connections, int VirtualConnections, double CallsPerSec, long Ts);

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

                var mine = new ServerInfo(me, ServerStats.Connections, ServerStats.VirtualConnections, Math.Round(rate, 1), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                await db.HashSetAsync(DemoKeys.Servers, me, JsonSerializer.Serialize(mine));

                var live = new List<ServerInfo>();
                var dead = new List<string>();
                var cutoff = DateTimeOffset.UtcNow.Subtract(Stale).ToUnixTimeMilliseconds();
                foreach (var e in await db.HashGetAllAsync(DemoKeys.Servers))
                {
                    var info = JsonSerializer.Deserialize<ServerInfo>((string)e.Value!, JsonSerializerOptions.Web);
                    if (info is null || info.Ts < cutoff) dead.Add(e.Name!);
                    else live.Add(info);
                }
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
                }, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                log.LogWarning(ex, "cluster report failed");
            }
        }
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
        var info = (await redis.GetServers()[0].InfoAsync()).SelectMany(g => g).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
        string Get(string k) => info.TryGetValue(k, out var v) ? v : "0";
        return new
        {
            opsPerSec = double.Parse(Get("instantaneous_ops_per_sec")),
            connectedClients = int.Parse(Get("connected_clients")),
            memory = Get("used_memory_human"),
            version = Get("redis_version"),
        };
    }
}
