using System.Net;
using Gateway;
using Yarp.ReverseProxy;
using Yarp.ReverseProxy.Model;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(80);     // proxied traffic
    k.ListenAnyIP(8404);   // gateway status page
    k.Limits.MaxConcurrentConnections = null;          // long-lived WebSockets: no connection cap
    k.Limits.MaxConcurrentUpgradedConnections = null;
});

builder.Services.AddReverseProxy();
// Replaces the static config provider: destinations come from DNS (one A record per scaled replica).
builder.Services.AddSingleton<DnsConfigProvider>();
builder.Services.AddSingleton<Yarp.ReverseProxy.Configuration.IProxyConfigProvider>(sp => sp.GetRequiredService<DnsConfigProvider>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<DnsConfigProvider>());

var app = builder.Build();

// Port 8404 serves the status page; everything else on port 80 is proxied.
app.Use(async (ctx, next) =>
{
    if (ctx.Connection.LocalPort != 8404) { await next(); return; }
    var lookup = ctx.RequestServices.GetRequiredService<IProxyStateLookup>();
    if (ctx.Request.Path == "/api")
    {
        await ctx.Response.WriteAsJsonAsync(StatusPage.Snapshot(lookup));
        return;
    }
    ctx.Response.ContentType = "text/html; charset=utf-8";
    await ctx.Response.WriteAsync(StatusPage.Html);
});

app.MapReverseProxy();
app.Run();
