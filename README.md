# SignalR scale-out lab: Redis backplane, Redis HA, and a replicated message log

Horizontally scaled ASP.NET Core SignalR (.NET 10) behind a YARP gateway. Redis (primary, replicas, Sentinels) connects the servers and carries live state. A 3-broker Redpanda cluster adds a **durable, replayable message path** so that a message confirmed to its sender cannot be lost. Everything runs in Docker Compose.

```
 browsers / load test ─WebSocket─► YARP gateway :9000 ─► server-1..N (SignalR hub /chat)
   (least requests, /healthz)                              │            │
                                                           │            └─► Redpanda x3 (Raft log, RF 3)   durable messages
                                                           ▼
                        Sentinel x3 ◄─ "who is primary?" ─ Redis primary ─► replica 1, replica 2   live state + at-most-once path
```

## What this proof of concept accomplishes

It answers four practical questions about running real-time .NET services at scale, and lets you check each answer yourself:

1. **Can SignalR run on many servers at once?** Yes. Three (or `--scale server=N`) identical servers share one set of users through a Redis backplane, behind a YARP gateway that spreads the WebSockets.
2. **What happens when a part dies or is upgraded?** The lab lets you crash a server, drain one gracefully, fail over Redis and stop Redpanda brokers while real clients are connected, and shows what each does to users.
3. **Can a message be made not to get lost?** The default path (Redis pub/sub) can lose messages, and the lab measures how many. The durable path (Redpanda replicated log plus resume offsets) loses none in every injected failure (audited, 0 missing).
4. **How do you keep all this runnable by one command?** Everything is Docker Compose: 3 servers, gateway, 3 Redis, 3 Sentinels, 3 Redpanda brokers, console. Data volumes survive `docker compose down -v`.

It is a learning and measurement lab, not a production template: no authentication, one Docker host, Redis not persisted. Documentation map: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (how it fits together), [docs/USER-MANUAL.md](docs/USER-MANUAL.md) (the web page, tab by tab), [docs/SHARDING.md](docs/SHARDING.md) (considerations for spreading the Everyone traffic over partitions), and the sections below (measurements and lessons).

## Requirements

Docker Desktop on macOS or Windows (or Docker Engine on Linux) with Compose v2, about 6 GB of RAM for Docker, and ports 9000, 8404 and 8082 free. You do not need the .NET SDK: everything builds inside Docker. On Windows use PowerShell for the commands below (`.\scripts\up.ps1`) or Git Bash/WSL for the `.sh` scripts.

| Path | Used for | Guarantee |
|---|---|---|
| Redis pub/sub backplane | the original chat, rooms, canvas, reactions, poll notifications, cluster panel | at most once (lost during failover, crash, socket swap, offline) |
| Redpanda log | "Durable" chat messages (checkbox in the Chat tab, audited flood in Lab) | at least once to the log, exactly once on screen, replayed after any reconnect |

## Layout

- `src/ChatServer`: SignalR hub (`/chat`), `/stats`, `/healthz`, the lab web page, the durable log code (`DurableLog.cs`), Redis HA helpers (`RedisHa.cs`).
- `src/Gateway`: YARP reverse proxy and load balancer. Status page: http://localhost:8404
- `src/LoadTest`: .NET load generator, including a durable mode with a sequence audit.
- `scripts/volumes.sh`, `scripts/up.sh`: create the persistent volumes, start the stack.

## Run

```bash
./scripts/up.sh                     # macOS/Linux/Git Bash. Windows PowerShell: .\scripts\up.ps1
# then open these in a browser (use two or more tabs on the first one; each shows which server it hit):
#   http://localhost:9000   the lab page
#   http://localhost:8082   Redpanda Console: brokers, partitions, messages
#   http://localhost:8404   gateway: destinations, health, open sockets per server
```

The first start takes about a minute (Redpanda forms its cluster, then `redpanda-init` creates the topic, then the servers start). Plain `docker compose up -d` also works once the volumes exist.

### Volumes survive everything compose does

The three Redpanda data volumes `signalr-redpanda-1..3` are declared `external: true` and created by `scripts/volumes.sh`, so `docker compose down`, `down -v` and removing the project never delete them. Verified: after `down -v` and a restart the log still held 18,533 records. Delete them only on purpose: `docker volume rm signalr-redpanda-1 signalr-redpanda-2 signalr-redpanda-3`. (Redis is deliberately not persisted: it only holds live state.)

## What happens by default

**Connect.** A client opens one WebSocket (WebSockets only, no negotiate, so no sticky sessions). The gateway sends it to the server with the fewest open sockets. The server resolves a device name, registers the client in the Redis roster and answers with its identity and build stamp. If the client has stored offsets it immediately sends `Resume` (see below).

**Send, original path.** A hub call such as `SendMessage` publishes to a Redis pub/sub channel (`…:all`, `…:group:room:A`, `…:connection:<id>`). Redis forwards it to every subscribed server and each server writes to its own sockets. Redis does not store the message.

**Send, durable path.** `SendDurable(msgId, room, user, text)` appends to the Redpanda topic `chat.messages` with an idempotent producer and `acks=all` (topic `min.insync.replicas=2`) and returns `{partition, offset}` only after a majority of brokers hold it. If that fails the call throws and the client retries with the **same** `msgId`. Every server tails every partition and writes each record to its own sockets as `LogMessage(partition, offset, …)`; Redis is not involved. The client keeps the highest offset per partition and ignores records it already has (by offset, and by `msgId` for a retry that produced a second record).

**Resume.** On every (re)connect the client sends its last offsets. The server replays everything after them for that client's audience (everyone, plus the rooms it joined), holding live records for that connection meanwhile and merging by offset, then goes live. This is what covers a killed server, a drain, a rebalance bounce and a sleeping phone. Offsets older than the 24 h retention are reported as truncated.

**Background, every second or so.** Each server writes a heartbeat to the Redis hash `demo:servers`; the lowest-named live server (leader) aggregates heartbeats, roster, Redis INFO and Redpanda broker count and pushes `ClusterStats` to all clients; every server checks whether it holds more than its share of connections and, if so, asks a batch of its clients to reconnect (the gateway sends them to the emptiest server). Clients ping for RTT every 3 s; SignalR keep-alive is 5 s with a 15 s timeout; the gateway health-checks every server every second; Sentinels watch Redis continuously.

## Is a message ever lost? (measured)

Durable mode, 300 connections, 3 senders × 5 msg/s for 60 s, event injected at 20 s. Each sender numbers its messages and retries unconfirmed ones; **every receiver must end up with every confirmed number**. `MISSING` is the sum over all 300 receivers (`MODE=durable`, see Load tests):

| Event during the run | Confirmed | Missing | Notes |
|---|---|---|---|
| Server killed (`docker kill`) | 899 | **0** | 1,953 records replayed to reconnecting clients |
| Server restarted gracefully (SIGTERM drain) | 900 | **0** | 81 replayed |
| Redis primary failover | 901 | **0** | latency tail up to about 5 s (not investigated) |
| One Redpanda broker stopped for 20 s | 900 | **0** | p99 16 ms, no visible effect |
| Two Redpanda brokers stopped for 20 s (no quorum) | 899 | **0** | writes refused for about 20 s, senders retried; messages arrive late (p95 about 25 s); 3 records were written twice and hidden by the id check |
| Controller broker killed; server killed + broker stopped together | 897 / 897 | **0** | |

Same test on the original Redis path: 98-100 % delivered over five failovers, about 1.3 % lost in a graceful drain, plus whatever a killed server or a sleeping phone misses.

Steady-state cost of the durable path: p50 about 6 to 7 ms from send to delivery (the replicated write is included), against about 4 to 5 ms on the Redis path.

**What "no loss" covers:** any message the sender saw confirmed, shown exactly once on every client that returns within 24 h, while at least 2 of 3 brokers survive. **Not covered:** a message never confirmed whose sender gave up (tab closed), a client away longer than retention or with cleared storage, losing the disks of two brokers at once, ordering between different rooms, and a total outage (that is the deliberate `docker compose down`, and even then the data stays on the volumes).

## Things this lab taught (fixed in the code)

- Redis Sentinel + StackExchange.Redis delivered every message **twice** after some failovers: pub/sub was subscribed on two nodes (Redis replicates PUBLISH to replicas). Fix: Redis nodes have fixed IPs, and the backplane connection demands the primary (`RedisHa.cs`).
- Kestrel stops before hosted services in minimal hosting, so a graceful drain started in `StopAsync` found 0 connections. The drain now runs in `ApplicationStopping`.
- Docker gives a restarted container a new IP. A surviving Redpanda broker kept the old addresses of two restarted peers and reported them down, which looked like a lost quorum. The brokers now have fixed IPs too.
- The first durable tail consumer stopped delivering after the two-broker outage on two of three servers (all later runs delivered nothing), which the audit caught immediately. The cause was not isolated; the consumer now starts from explicit offsets, remembers the next offset per partition, and a watchdog restarts it from there if it stalls with unread records. Each connection also has its own writer queue, so one slow client cannot stall delivery to the rest. The watchdog has not been triggered in any later run, so that path is untested.
- The load test needed keep-alive settings matching the server's 15 s client timeout, or it dropped 8 % of its own connections.

## Use it from other devices on your LAN

Port 9000 is published on all interfaces. On phones and other computers on the same network open `http://<this-computer's-name-or-IP>:9000`. The IP address always works (macOS: `ipconfig getifaddr en0`; Windows: `ipconfig`); computer names usually work on Apple devices (`name.local`) and often not on Android. The **Invite devices** button in the page shows a QR code with the right address. If devices can't connect, allow Docker Desktop through the firewall (macOS: System Settings > Network > Firewall; Windows: allow Docker Desktop on private networks) and make sure both devices are on the same Wi-Fi without client isolation.

| Tab | Demonstrates |
|---|---|
| Chat | `Clients.All` and groups (rooms A/B/C); the **Durable** checkbox sends through the replicated log (pending, then persisted p3@41; replayed after reconnects) |
| Canvas | real-time fan-out of pointer events |
| Poll & React | shared state in Redis + backplane notifications; emoji floating on every screen |
| Lab | latency probe, server-side **burst**, multi-device **flood**, **virtual users**, an **audited durable flood** (per-sender gap detection on every device, GAPS must stay 0), and **resilience** buttons (crash, graceful shutdown, Redis failover) |

Device-side experiments can run on *all* devices at once from any device (devices can opt out with "accept remote commands").

## Load tests

```bash
docker compose run --no-deps --rm loadtest                              # original path, 500 connections
docker compose run --no-deps --rm -e MODE=durable -e CONNECTIONS=300 -e SENDERS=3 -e MSGS_PER_SEC=5 -e DURATION_SEC=60 loadtest
# tune: CONNECTIONS SENDERS MSGS_PER_SEC DURATION_SEC AUDIENCE=everyone|rooms|mixed (original path) ROOMS=3
```

`--no-deps` matters: without it `compose run` re-applies the default replica count. While a durable run is going, break things: `docker kill signalrwithredis-server-2`, `docker restart signalrwithredis-server-3`, `docker stop signalrwithredis-redpanda-2-1`, `docker compose exec sentinel-1 redis-cli -p 26379 sentinel failover mymaster`. The run ends with `RESULT: PASS` only if every confirmed message reached every receiver.

## Redis HA, graceful shutdown, rebalancing

- **Redis**: 1 primary + 2 replicas + 3 Sentinels (quorum 2); the app connects through the Sentinels and follows failovers. Nodes and Sentinels reference each other by fixed IP.
- **Graceful shutdown** (`docker stop`, `docker restart`, rolling upgrades, or the Lab button): on SIGTERM the server fails `/healthz` (the gateway stops sending it new sockets within about 2 s), waits 3 s, asks its clients to reconnect in 10 waves, waits until they are gone (max about 14 s), then exits. `stop_grace_period: 30s`.
- **Abrupt crash**: clients notice within about 15 s at worst (usually at once via TCP reset), reconnect with back-off, rejoin rooms and, in durable mode, resume from their offsets.
- **Connection rebalancing**: WebSockets are long-lived, so a restarted or newly scaled server stays empty. Every server compares its connection count with the cluster average (from the Redis heartbeats) and, when clearly above it, asks a few of its clients to reconnect. The gateway's least-requests policy sends them to the emptiest server. Toggle it in the cluster panel.
- Rolling restart: `for s in $(docker compose ps -q server); do docker restart $s; sleep 20; done`.
- Open pages reload themselves when they reconnect to a server from a newer build.

## Gateway (YARP)

`src/Gateway` replaces the earlier HAProxy. It resolves the Docker DNS name `server` every second (one address per replica), so `--scale server=N`, restarts and crashes need no gateway restart. Policy `LeastRequests`: every proxied WebSocket stays "in flight" for its whole life, so it behaves like least-connections (300 sockets landed 100/99/97 on three servers). Active health checks call `/healthz` every second; two failures or a 503 (draining) remove a destination without touching its open sockets. Plain HTTP requests are not spread evenly (ties go to the first destination); only the sockets matter here.

## Device names

Browsers can't read a device's real name. Order used: a name you type (saved in the browser) → `src/ChatServer/devices.json` (`{"match": "<regex on User-Agent>", "name": "..."}`, bind-mounted, edited live) → reverse DNS of the client address (needs the real client IP, which Docker Desktop hides) → browser-reported device + browser, e.g. `iPhone · Safari`. Duplicates are numbered (`Mac · Chrome`, `Mac · Chrome #2`).

## Scale horizontally

```bash
docker compose up -d --scale server=6   # then use --no-deps with `compose run`
curl localhost:8404/api                 # gateway view: destinations, health, open sockets
```

## Design notes

- **Why a log and not a bigger backplane**: Redis pub/sub forgets a message the moment it is forwarded. Durability and replay need an append-only, replicated record that receivers can re-read by offset.
- **Ordering**: a room maps to one partition (key `room:<name>`), "everyone" maps to one partition (key `all`), so each is ordered. Everything on "everyone" shares one partition: fine for a lab, a hot spot at scale (shard the key and order per sender instead).
- **Fan-out**: every server reads every partition, like the Redis backplane, so read traffic grows with messages × servers.
- **Why not `--mode dev-container`** for Redpanda: it skips fsync, which defeats the point.
- **Kept on Redis on purpose**: presence, cluster stats, poll, drawing strokes, reactions: losing one costs nothing.
- Stop everything: `docker compose down` (data volumes stay).
