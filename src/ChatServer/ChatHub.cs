using Microsoft.AspNetCore.SignalR;

namespace ChatServer;

public class ChatHub : Hub
{
    public static string ServerName { get; } = Environment.MachineName;

    public override async Task OnConnectedAsync()
    {
        ServerStats.ConnectionOpened();
        // Tell the caller which instance accepted its connection.
        await Clients.Caller.SendAsync("Connected", ServerName);
        await base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        ServerStats.ConnectionClosed();
        return base.OnDisconnectedAsync(exception);
    }

    // Broadcast to every client on every server (via the Redis backplane).
    // sentTicks is echoed back untouched so clients can measure end-to-end latency.
    public Task SendMessage(string user, string message, long sentTicks = 0)
    {
        ServerStats.MessageSent();
        return Clients.All.SendAsync("ReceiveMessage", user, message, ServerName, sentTicks);
    }
}

public static class ServerStats
{
    private static int _connections;
    private static long _messages;

    public static int Connections => _connections;
    public static long Messages => Interlocked.Read(ref _messages);

    public static void ConnectionOpened() => Interlocked.Increment(ref _connections);
    public static void ConnectionClosed() => Interlocked.Decrement(ref _connections);
    public static void MessageSent() => Interlocked.Increment(ref _messages);
}
