# SignalR scale-out with a Redis backplane

Horizontally scaled ASP.NET Core SignalR (.NET 10) behind an nginx load balancer, with Redis as the backplane. Everything runs in Docker Compose.

```
 browser / load test ──► nginx :9000 ──┬─► server-1 ─┐
                                       ├─► server-2 ─┼──► Redis (pub/sub backplane)
                                       └─► server-N ─┘
```

- `src/ChatServer` – SignalR hub (`/chat`), `/stats`, `/healthz`, static demo page. Uses `Microsoft.AspNetCore.SignalR.StackExchangeRedis`.
- `src/LoadTest` – console load generator (N connections, a few publishers, reports delivery %, throughput and p50/p95/p99 latency).
- `nginx/nginx.conf` – WebSocket-aware round-robin load balancer; re-resolves Docker DNS so scaling needs no restart.

## Run

```bash
docker compose up -d --build        # 3 server replicas by default
open http://localhost:9000          # open in 2+ tabs; each shows which instance it hit
```

Messages sent in one tab appear in all tabs, even when the tabs are on different servers (the `via` tag shows the instance that processed the send).

## Scale horizontally

```bash
docker compose up -d --scale server=6
curl localhost:9000/stats           # repeat – responses come from different instances
```

## Load test

```bash
docker compose run --rm loadtest
# tune: CONNECTIONS=2000 SENDERS=10 MSGS_PER_SEC=5 DURATION_SEC=60
docker compose run --rm -e CONNECTIONS=2000 -e SENDERS=10 loadtest
```

Each published message is delivered to *every* connection, so deliveries/s = senders × rate × connections. Sample run (3 servers, 500 connections, 25 msg/s published, Docker Desktop on a laptop): 100% delivered, ~12.5k deliveries/s, p50 7.6 ms, p99 18 ms.

Watch the backplane: `docker compose exec redis redis-cli monitor`.

## Design notes

- **Sticky sessions not needed**: clients use `skipNegotiation` + WebSockets only. With the negotiate step or long-polling/SSE fallbacks you would need session affinity at the load balancer (the docs call this out).
- **Backplane limits**: every message is published to Redis and fanned out to every server, so Redis traffic grows with messages × servers. For high fan-out, prefer groups/targeted sends over `Clients.All`.
- **Redis is a single node here**; for production use a managed/clustered Redis with `ChannelPrefix` per app.
- Stop everything: `docker compose down`.
