using Microsoft.AspNetCore.SignalR;
using ChatServer;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR(o =>
    {
        o.AddFilter<CountingFilter>();
        // Detect dead peers faster than the 15s/30s defaults.
        o.KeepAliveInterval = TimeSpan.FromSeconds(5);
        o.ClientTimeoutInterval = TimeSpan.FromSeconds(15);
    })
    .AddStackExchangeRedis(options =>
    {
        options.Configuration = RedisConfig.Create(builder.Configuration);
        var cfg = options.Configuration;
        options.ConnectionFactory = writer => PrimaryOnlyBackplane.ConnectAsync(cfg, writer);
        // Isolates this app's channels if the Redis instance is shared.
        options.Configuration.ChannelPrefix = RedisChannel.Literal("SignalRScaleDemo");
    });

// Separate multiplexer for the demo's own shared state (poll, roster, server heartbeats).
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(RedisConfig.Create(builder.Configuration)));
builder.Services.AddSingleton<RedisHa>();
// Durable path: replicated log. LocalConnections + the wrapping lifetime manager let the log write to local sockets.
builder.Services.AddSingleton<LocalConnections>();
builder.Services.AddSingleton<ChatLog>();
builder.Services.AddSingleton<LogDelivery>();
builder.Services.AddSingleton(typeof(HubLifetimeManager<>), typeof(TrackingLifetimeManager<>));
builder.Services.AddHostedService<ChatLogTail>();
builder.Services.AddHostedService<ClusterReporter>();
builder.Services.AddHostedService<GracefulDrain>();   // after ClusterReporter => stops first
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(28));

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles(new StaticFileOptions { OnPrepareResponse = c => c.Context.Response.Headers.CacheControl = "no-cache" }); // always revalidate: edits show up on reload

app.MapHub<ChatHub>("/chat");
// Readiness: 503 while draining, so the load balancer takes this instance out of rotation.
app.MapGet("/healthz", () => ServerStats.Draining ? Results.StatusCode(503) : Results.Ok("ok"));
app.MapGet("/stats", (HttpContext http) => new
{
    clientIp = http.Request.Headers["X-Forwarded-For"].ToString(), // address as seen via the load balancer
    server = ChatHub.ServerName,
    connections = ServerStats.Connections,
    virtualConnections = ServerStats.VirtualConnections,
    hubCalls = ServerStats.Invocations,
    log = new { appended = ServerStats.LogAppendedCount, delivered = ServerStats.LogDeliveredCount, replayed = ServerStats.LogReplayedCount, tailRestarts = ServerStats.LogTailRestarts },
});

app.Run();
