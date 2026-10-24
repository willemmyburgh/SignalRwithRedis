using ChatServer;

var builder = WebApplication.CreateBuilder(args);

var redis = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";

builder.Services.AddSignalR()
    .AddStackExchangeRedis(redis, options =>
    {
        // Isolates this app's channels if the Redis instance is shared.
        options.Configuration.ChannelPrefix = StackExchange.Redis.RedisChannel.Literal("SignalRScaleDemo");
    });

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
    messagesSent = ServerStats.Messages,
});

app.Run();
