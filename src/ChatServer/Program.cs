using Microsoft.AspNetCore.SignalR;
using ChatServer;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var redis = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";

builder.Services.AddSignalR(o => o.AddFilter<CountingFilter>())
    .AddStackExchangeRedis(redis, options =>
    {
        // Isolates this app's channels if the Redis instance is shared.
        options.Configuration.ChannelPrefix = RedisChannel.Literal("SignalRScaleDemo");
    });

// Separate multiplexer for the demo's own shared state (poll, roster, server heartbeats).
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var cfg = ConfigurationOptions.Parse(redis);
    cfg.AbortOnConnectFail = false;
    cfg.AllowAdmin = true; // needed for INFO (Redis stats panel)
    return ConnectionMultiplexer.Connect(cfg);
});
builder.Services.AddHostedService<ClusterReporter>();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<ChatHub>("/chat");
app.MapHealthChecks("/healthz");
app.MapGet("/stats", () => new
{
    server = ChatHub.ServerName,
    connections = ServerStats.Connections,
    virtualConnections = ServerStats.VirtualConnections,
    hubCalls = ServerStats.Invocations,
});

app.Run();
