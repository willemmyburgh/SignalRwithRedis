using Yarp.ReverseProxy;
using Yarp.ReverseProxy.Model;

namespace Gateway;

public static class StatusPage
{
    public static object Snapshot(IProxyStateLookup lookup)
    {
        if (!lookup.TryGetCluster("servers", out var cluster)) return new { destinations = Array.Empty<object>() };
        return new
        {
            policy = cluster.Model.Config.LoadBalancingPolicy,
            destinations = cluster.DestinationsState.AllDestinations
                .OrderBy(d => d.DestinationId, StringComparer.Ordinal)
                .Select(d => new
                {
                    address = d.Model.Config.Address,
                    active = d.Health.Active.ToString(),
                    passive = d.Health.Passive.ToString(),
                    inFlight = d.ConcurrentRequestCount,
                }),
        };
    }

    public const string Html = """
        <!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Gateway</title>
        <style>body{font:15px system-ui;margin:2rem auto;max-width:720px;padding:0 1rem}table{border-collapse:collapse;width:100%}td,th{padding:.4rem .6rem;border-bottom:1px solid #8884;text-align:left}.Healthy{color:#16a34a}.Unhealthy{color:#dc2626}</style>
        <h2>YARP gateway</h2><p id="p"></p>
        <table><thead><tr><th>Destination</th><th>Active check</th><th>Passive</th><th>Open sockets</th></tr></thead><tbody id="b"></tbody></table>
        <script>
        async function tick(){const r=await (await fetch('/api')).json();p.textContent='load balancing: '+r.policy;
        b.innerHTML=r.destinations.map(d=>`<tr><td>${d.address}</td><td class="${d.active}">${d.active}</td><td>${d.passive}</td><td>${d.inFlight}</td></tr>`).join('')}
        tick();setInterval(tick,1000)
        </script>
        """;
}
