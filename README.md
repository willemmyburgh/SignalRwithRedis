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

## Architecture: what happens by default

```
 browsers / load test ──WebSocket──► HAProxy :9000 (leastconn, /healthz) ──► server-1..N (SignalR hub /chat)
                                                                                   │  pub/sub backplane + shared state
                                                    Sentinel x3 ◄── "who is primary?" ┤
                                                         │ monitor / promote          ▼
                                                         └──────────────► Redis primary ──replicates──► replica 1, replica 2
```

**Connect.** A client opens one WebSocket (WebSockets only, no negotiate, so no sticky sessions). HAProxy picks the server with the fewest connections. The server resolves a device name, registers the client in the Redis roster, and answers with its identity and build stamp.

**Send.** A hub call such as `SendMessage` makes the receiving server *publish* to a Redis pub/sub channel (`…:all`, `…:group:room:A`, `…:connection:<id>`). Redis forwards it to every subscribed server and each server writes to its own sockets. Redis does not store the message.

**Background, every second or so.** Each server writes a heartbeat to `demo:servers`; the lowest-named live server (leader) aggregates heartbeats, roster and Redis INFO and pushes `ClusterStats` to all clients; every server checks whether it holds more than its share of connections and, if so, asks a batch of its clients to reconnect (HAProxy sends them to the emptiest server). Clients ping for RTT every 3 s; SignalR keep-alive is 5 s with a 15 s timeout; HAProxy health-checks every server every second; Sentinels watch Redis continuously.

**Guarantee today: at most once.** Messages can be lost during a Redis failover (seconds), when a server is killed, while a client's socket is being swapped (drain, rebalance), and for any client that is offline. Measured: 98-100 % delivered across five failovers under load, ~1.3 % loss during a graceful drain, 100 % in steady state.

The full picture with diagrams is in the architecture page that accompanies this repo (published as an artifact).

## Proposal: a durable log (Redpanda / Kafka) for no message loss

*Not built yet.* The idea is to stop treating Redis pub/sub as the source of truth.

| Piece | Design |
|---|---|
| Log | 3-node Redpanda (Raft; Kafka API; one binary per node). Topic `chat.messages`, 12 partitions keyed by room/conversation, replication factor 3, `min.insync.replicas=2`, retention 24 h, named volumes. |
| Send | Hub method produces with `acks=all` and an idempotent producer, and returns `{msgId, partition, offset}` only after the write is acknowledged. The client shows pending then persisted, and retries with the same `msgId`. |
| Receive | Every server tails all partitions (one consumer group per server) and fans out to its local sockets. Pushes carry `(partition, offset)`. The client keeps the last offset per partition and drops duplicates. |
| Resume | On any reconnect the client sends `resume(lastOffsets)`; the server replays from the log, merges with live messages by offset, then goes live. This covers server crash, drain, rebalance bounce and sleeping phones. |
| Redis | Stays for ephemeral data: presence, cluster stats, poll, drawing strokes. It is no longer on the message path, so a Redis outage or failover no longer affects chat. |
| Proof | Extend the load test: each sender numbers its messages; every receiver asserts contiguous numbers per sender. Chaos under load: kill/drain a server, Redis failover, stop one broker (continues), stop two brokers (writes refused, nothing silently lost). Pass = zero gaps, zero visible duplicates. |

What "no loss" covers: any message the sender saw as persisted, delivered exactly once on screen to every client that returns within retention, as long as 2 of 3 brokers survive. It does not cover a message never acknowledged whose sender gave up, a client away longer than retention or with cleared offsets, simultaneous loss of two brokers' disks, or ordering between different rooms. `docker compose down` keeps the data on volumes; `down -v` deletes it.

Alternatives considered: Kafka in KRaft mode (same design, heavier), Redis Streams (smallest change, but asynchronous replication can lose acknowledged writes on failover), NATS JetStream (lighter, less .NET tooling). Expected cost: about 5-10 ms extra per send for the replicated write.

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

## High availability and graceful shutdown

- **Redis**: 1 primary + 2 replicas + 3 Sentinels (quorum 2); the app connects through the Sentinels and follows failovers. Try `docker compose stop redis-1` (or the *Redis failover* button in Lab → Resilience; or `docker exec <sentinel> redis-cli -p 26379 sentinel failover mymaster`). Nodes have fixed IPs and are referenced by IP everywhere, and the backplane connection demands the primary for pub/sub: otherwise StackExchange.Redis can hold subscriptions on two nodes and every message is delivered twice after a failover (seen and fixed, see `RedisHa.cs`). Five consecutive failovers under load: 98-100% delivered, no duplicates. Messages published during the few seconds of election are lost (pub/sub is at-most-once); the demo's poll/roster state survives via replication.
- **Graceful shutdown** (`docker stop`, `docker restart`, rolling upgrades, or the Lab button): on SIGTERM the instance fails `/healthz` (HAProxy stops sending it new connections within ~2 s), waits 3 s, asks its clients to reconnect in 10 waves, waits until they are gone (max ~14 s), then exits. `stop_grace_period: 30s`. The drain runs in `ApplicationStopping`, because Kestrel stops before hosted services in minimal hosting and would otherwise cut the sockets first. Clients that are moved this way lose the messages published while their socket is being swapped (typically a fraction of a second), so it is *mostly* seamless, not lossless. A make-before-break client (second socket first) would close that gap.
- **Abrupt crash** (`docker kill`, power loss): clients detect it within ~15 s at worst (usually instantly via TCP reset), reconnect with back-off, and re-join rooms.
- Rolling restart: `for s in $(docker compose ps -q server); do docker restart $s; sleep 20; done`.
- Open pages reload themselves when they reconnect to a newer deployment.

## Scale horizontally

```bash
docker compose up -d --scale server=6   # use --no-deps with `compose run`, or it resets to 3
curl localhost:9000/stats           # repeat – responses come from different instances\n# the cluster panel on the page picks up new instances within a few seconds
```

## Load test

```bash
docker compose run --rm loadtest
# tune: CONNECTIONS=2000 SENDERS=10 MSGS_PER_SEC=5 DURATION_SEC=60 AUDIENCE=everyone|rooms|mixed ROOMS=3
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
