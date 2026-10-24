using System.Net;
using Microsoft.Extensions.Primitives;
using Yarp.ReverseProxy.Configuration;

namespace Gateway;

/// <summary>
/// Docker's embedded DNS answers the service name with one address per replica. Every second this provider resolves it
/// and, when the set of addresses changed, publishes a new YARP configuration. Destination ids are the addresses, so
/// health state and request counts survive unrelated changes. This is what makes `--scale server=N` and server restarts
/// work without touching the gateway.
/// </summary>
public sealed class DnsConfigProvider(IConfiguration config, ILogger<DnsConfigProvider> log) : BackgroundService, IProxyConfigProvider
{
    private readonly string _host = config["UPSTREAM_HOST"] ?? "server";
    private readonly int _port = int.Parse(config["UPSTREAM_PORT"] ?? "8080");
    private volatile Snapshot _current = new([], [], new CancellationTokenSource());
    private string _lastKey = "";

    public IProxyConfig GetConfig() => _current;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        do
        {
            try
            {
                var addresses = (await Dns.GetHostAddressesAsync(_host, ct)).Select(a => a.ToString()).Order().ToArray();
                var key = string.Join(',', addresses);
                if (key == _lastKey) continue;
                _lastKey = key;
                Publish(addresses);
                log.LogInformation("upstream {Host}: {Count} destinations ({Addresses})", _host, addresses.Length, key);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                log.LogWarning("DNS lookup for {Host} failed: {Message}", _host, ex.Message);
            }
        } while (await timer.WaitForNextTickAsync(ct));
    }

    private void Publish(string[] addresses)
    {
        var cluster = new ClusterConfig
        {
            ClusterId = "servers",
            // Every proxied WebSocket counts as one in-flight request for its whole life, so "least requests"
            // behaves like least-connections: new clients go to the emptiest server.
            LoadBalancingPolicy = "LeastRequests",
            HealthCheck = new HealthCheckConfig
            {
                // A draining server answers 503 here and leaves rotation; its existing sockets keep running.
                Active = new ActiveHealthCheckConfig
                {
                    Enabled = true, Interval = TimeSpan.FromSeconds(1), Timeout = TimeSpan.FromSeconds(2),
                    Policy = "ConsecutiveFailures", Path = "/healthz",
                },
                Passive = new PassiveHealthCheckConfig { Enabled = true, Policy = "TransportFailureRate", ReactivationPeriod = TimeSpan.FromSeconds(5) },
            },
            Metadata = new Dictionary<string, string> { ["ConsecutiveFailuresHealthPolicy.Threshold"] = "2" },
            HttpRequest = new Yarp.ReverseProxy.Forwarder.ForwarderRequestConfig { ActivityTimeout = TimeSpan.FromHours(1) },
            Destinations = addresses.ToDictionary(a => a, a => new DestinationConfig { Address = $"http://{a}:{_port}" }),
        };
        var route = new RouteConfig { RouteId = "all", ClusterId = "servers", Match = new RouteMatch { Path = "{**catch-all}" } };

        var old = _current;
        _current = new Snapshot([route], [cluster], new CancellationTokenSource());
        old.Signal(); // tells YARP to reload
    }

    private sealed class Snapshot(IReadOnlyList<RouteConfig> routes, IReadOnlyList<ClusterConfig> clusters, CancellationTokenSource cts) : IProxyConfig
    {
        public IReadOnlyList<RouteConfig> Routes { get; } = routes;
        public IReadOnlyList<ClusterConfig> Clusters { get; } = clusters;
        public IChangeToken ChangeToken { get; } = new CancellationChangeToken(cts.Token);
        public void Signal() => cts.Cancel();
    }
}
