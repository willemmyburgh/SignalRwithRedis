# SignalR scale-out with a Redis backplane

Horizontally scaled ASP.NET Core SignalR (.NET 10) behind an HAProxy load balancer, with Redis as the backplane. Everything runs in Docker Compose.

```
 browser / load test ──► HAProxy :9000 ──┬─► server-1 ─┐
                                       ├─► server-2 ─┼──► Redis (pub/sub backplane)
                                       └─► server-N ─┘
```

- `src/ChatServer` – SignalR hub (`/chat`), `/stats`, `/healthz`, static demo page. Uses `Microsoft.AspNetCore.SignalR.StackExchangeRedis`.
- `src/LoadTest` – console load generator (N connections, a few publishers, reports delivery %, throughput and p50/p95/p99 latency).
- `HAProxy/HAProxy.conf` – WebSocket-aware round-robin load balancer; re-resolves Docker DNS so scaling needs no restart.

## Run

```bash
docker compose up -d --build        # 3 server replicas by default
open http://localhost:9000          # open in 2+ tabs; each shows which instance it hit
```

Messages sent in one tab appear in all tabs, even when the tabs are on different servers (the `via` tag shows the instance that processed the send).

## Use it from other devices on your LAN

Port 9000 is published on all interfaces. On phones/laptops open `http://willem-macs:9000` (or `http://willem-macs.local:9000`, or the Mac's IP; Android often needs the IP). The **Invite devices** button shows a QR code. If devices can't connect, check the macOS firewall.

The page is a lab; every device sees the same live cluster panel (per-server connections, hub calls/s, Redis ops/s, clients online) and can trigger:

| Tab | Demonstrates |
|---|---|
| Chat | `Clients.All` and groups (rooms A/B/C); tag shows which server handled the sender |
| Canvas | real-time fan-out of pointer events |
| Poll & React | shared state in Redis + backplane notifications; emoji floating on every screen |
| Experiments | latency probe, server-side **burst** (loss/order/throughput reported by every device), multi-device **flood** (end-to-end latency via Redis), **virtual users** (each device opens up to 1000 extra connections), **chaos** (crash the server you're on and watch clients fail over) |

Device-side experiments can run on *all* devices at once from any device (devices can opt out with "accept remote commands").

## Connection rebalancing

WebSockets are long-lived, so after a crash, restart or `--scale` the new/empty server stays empty. Every server watches the cluster's heartbeats (Redis hash); a server above the cluster average, with a gap to the emptiest server beyond a small dead band, asks a few of its own clients to reconnect (`Reconnect` hub message, small batches, 3 s cooldown). Clients drop and reconnect with jitter, and HAProxy's `leastconn` sends them to the emptiest server. Toggle it from the cluster panel to compare. Example (5 servers, 500 connections, one server stopped and restarted): 101 each, then 127 each on four servers, then back to 103/94 within ~10 s.

## Device names

Browsers can't read a device's real name. Order used: a name you type (saved in the browser) → `src/ChatServer/devices.json` (`{"match": "<regex on User-Agent>", "name": "..."}`, edited live, no restart) → reverse DNS of the client address (only works when the real client IP reaches the server, which Docker Desktop hides) → browser-reported device + browser, e.g. `iPhone · Safari`. Duplicates are numbered (`Mac · Chrome`, `Mac · Chrome #2`; a different browser gets its own name). Edit `devices.json` with a tool that keeps the file inode (e.g. `cat >`), or restart the servers, because it is bind-mounted.

## Scale horizontally

```bash
docker compose up -d --scale server=6   # use --no-deps with `compose run`, or it resets to 3
curl localhost:9000/stats           # repeat – responses come from different instances\n# the cluster panel on the page picks up new instances within a few seconds
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
- **Failure semantics**: crashed servers are restarted by Docker (`restart: unless-stopped`); clients reconnect with an unbounded retry policy and re-join groups. Messages during the gap are lost (backplane is at-most-once).
- Stop everything: `docker compose down`.
