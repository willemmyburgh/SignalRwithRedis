using Microsoft.AspNetCore.SignalR;
using StackExchange.Redis;

namespace ChatServer;

public static class RedisConfig
{
    /// <summary>
    /// With Redis__Sentinels set, the app connects through Sentinel (it asks the Sentinels who the primary is and
    /// follows failovers). Otherwise it uses ConnectionStrings:Redis, a single node.
    /// </summary>
    public static ConfigurationOptions Create(IConfiguration config)
    {
        var sentinels = config["Redis:Sentinels"];
        var opts = string.IsNullOrWhiteSpace(sentinels)
            ? ConfigurationOptions.Parse(config.GetConnectionString("Redis") ?? "localhost:6379")
            : SentinelOptions(config);
        if (!string.IsNullOrWhiteSpace(sentinels)) opts.ServiceName = config["Redis:ServiceName"] ?? "mymaster";
        opts.AbortOnConnectFail = false;   // start even if Redis isn't up yet; keep retrying
        opts.AllowAdmin = true;            // INFO for the stats panel
        opts.ConnectRetry = 5;
        opts.ReconnectRetryPolicy = new ExponentialRetry(500, 3000);
        return opts;
    }

    public static ConfigurationOptions SentinelOptions(IConfiguration config)
    {
        var opts = new ConfigurationOptions();
        foreach (var ep in config["Redis:Sentinels"]!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            opts.EndPoints.Add(ep);
        return opts;
    }
}

/// <summary>Lets the demo trigger a controlled primary failover through Sentinel.</summary>
public class RedisHa(IConfiguration config)
{
    public async Task<string> FailoverAsync()
    {
        if (string.IsNullOrWhiteSpace(config["Redis:Sentinels"])) return "No Sentinel configured";
        var opts = RedisConfig.SentinelOptions(config);
        opts.CommandMap = CommandMap.Sentinel;
        opts.TieBreaker = "";
        opts.AbortOnConnectFail = false;
        await using var sentinel = await ConnectionMultiplexer.SentinelConnectAsync(opts);
        var service = config["Redis:ServiceName"] ?? "mymaster";
        foreach (var ep in sentinel.GetEndPoints())
        {
            try { await sentinel.GetServer(ep).SentinelFailoverAsync(service); return $"Failover requested via {ep}"; }
            catch { /* try next sentinel */ }
        }
        return "No Sentinel reachable";
    }
}

/// <summary>
/// Graceful shutdown (SIGTERM from `docker stop`, a rolling restart or the "graceful shutdown" button):
/// 1. flip /healthz to 503 so the load balancer stops sending new connections,
/// 2. wait for it to notice, 3. ask clients to reconnect in small waves (they land on the other servers),
/// 4. only then let the host stop.
/// </summary>
public class GracefulDrain(Microsoft.AspNetCore.SignalR.IHubContext<ChatHub> hub, IConnectionMultiplexer redis, IHostApplicationLifetime lifetime, ILogger<GracefulDrain> log) : IHostedService
{
    // Not done in StopAsync: in minimal hosting Kestrel is registered last, so it stops FIRST and would cut every
    // WebSocket before we get a chance to move the clients. ApplicationStopping fires before any service is stopped;
    // blocking in the callback holds the whole shutdown (the host's shutdown timeout only starts afterwards).
    public Task StartAsync(CancellationToken ct)
    {
        lifetime.ApplicationStopping.Register(() => DrainAsync().GetAwaiter().GetResult());
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;

    private async Task DrainAsync()
    {
        ServerStats.Draining = true;
        log.LogWarning("draining: {Connections} connections", ServerStats.Connections);
        await hub.Clients.All.SendAsync("Notice", $"🔧 {ChatHub.ServerName[..Math.Min(6, ChatHub.ServerName.Length)]} is shutting down gracefully: moving its clients away");

        await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None); // let the load balancer's health check notice

        var ids = ConnectionTracker.Sample(int.MaxValue);
        const int waves = 10;
        var size = Math.Max(1, (int)Math.Ceiling(ids.Length / (double)waves));
        foreach (var batch in ids.Chunk(size))
        {
            await hub.Clients.Clients(batch).SendAsync("Reconnect", "drain");
            await Task.Delay(400, CancellationToken.None);
        }

        // Stragglers (slow clients, clients that connected during the drain): keep asking until gone or timeout.
        var deadline = DateTime.UtcNow.AddSeconds(14);
        while (ServerStats.Connections > 0 && DateTime.UtcNow < deadline)
        {
            await hub.Clients.Clients(ConnectionTracker.Sample(int.MaxValue)).SendAsync("Reconnect", "drain");
            await Task.Delay(500, CancellationToken.None);
        }
        log.LogWarning("drain finished with {Left} connections left", ServerStats.Connections);

        try { await redis.GetDatabase().HashDeleteAsync(DemoKeys.Servers, ChatHub.ServerName); } catch { /* Redis may be down */ }
        ServerStats.Stopped = true;
    }
}

/// <summary>
/// Sentinel failover + pub/sub: Redis replicates PUBLISH to replicas, so a backplane subscription that ends up
/// on a replica (e.g. re-created while the primary was changing) receives every message AND the one from the
/// real primary subscription, i.e. duplicates. Demanding the primary for every subscribe/publish keeps exactly
/// one subscription, and StackExchange.Redis moves it with the primary.
/// </summary>
public static class PrimaryOnlyBackplane
{
    public static async Task<IConnectionMultiplexer> ConnectAsync(ConfigurationOptions options, TextWriter log)
    {
        var mux = await ConnectionMultiplexer.ConnectAsync(options, log);
        var proxy = System.Reflection.DispatchProxy.Create<IConnectionMultiplexer, MultiplexerProxy>();
        ((MultiplexerProxy)(object)proxy).Target = mux;
        return proxy;
    }

    private static object? Call(System.Reflection.MethodInfo method, object target, object?[]? args)
    {
        try { return method.Invoke(target, args); }
        catch (System.Reflection.TargetInvocationException e) when (e.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw();
            throw;
        }
    }

    public class MultiplexerProxy : System.Reflection.DispatchProxy
    {
        public IConnectionMultiplexer Target = null!;

        protected override object? Invoke(System.Reflection.MethodInfo? method, object?[]? args)
        {
            var result = Call(method!, Target, args);
            if (method!.Name == nameof(IConnectionMultiplexer.GetSubscriber) && result is ISubscriber sub)
            {
                var proxy = System.Reflection.DispatchProxy.Create<ISubscriber, SubscriberProxy>();
                ((SubscriberProxy)(object)proxy).Target = sub;
                return proxy;
            }
            return result;
        }
    }

    public class SubscriberProxy : System.Reflection.DispatchProxy
    {
        public ISubscriber Target = null!;

        protected override object? Invoke(System.Reflection.MethodInfo? method, object?[]? args)
        {
            var ps = method!.GetParameters();
            for (var i = 0; i < ps.Length; i++)
                if (ps[i].ParameterType == typeof(CommandFlags)) args![i] = (CommandFlags)args[i]! | CommandFlags.DemandMaster;
            return Call(method, Target, args);
        }
    }
}
