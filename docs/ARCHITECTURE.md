# Architecture

## Components

| Component | Runs as | Job |
|---|---|---|
| Browser / load test | outside Docker (or the `loadtest` container) | Holds one WebSocket to the gateway |
| YARP gateway (`src/Gateway`) | 1 container, ports 9000 (app) and 8404 (status) | Spreads WebSockets over the servers (least requests), health-checks `/healthz` every second, re-resolves the Docker DNS name `server` every second |
| App server (`src/ChatServer`) | 3 containers (`--scale server=N`) | ASP.NET Core 10 SignalR hub at `/chat`, the lab web page, `/healthz`, `/stats` |
| Redis (primary + 2 replicas) | 3 containers | SignalR backplane (pub/sub between servers) and small shared records (heartbeats, roster, poll) |
| Redis Sentinels | 3 containers | Vote (quorum 2) and promote a replica when the primary dies; servers ask them who the primary is |
| Redpanda (3 brokers) | 3 containers on external volumes | Raft-replicated, append-only log for durable messages (topic `chat.messages`: 12 partitions, replication 3, min ISR 2, 24 h retention) |
| Redpanda Console | 1 container, port 8082 | Browse brokers, partitions and messages |

```
 browsers ─WebSocket─► YARP gateway ─► server-1..N ──pub/sub + state──► Redis primary ─► replicas
                                           │                                  ▲
                                           │                      Sentinels (who is primary?)
                                           └──append / tail / replay──► Redpanda x3 (Raft)
```

## The two message paths

**Original path (at most once).** `SendMessage` publishes to a Redis pub/sub channel. Redis forwards it to every subscribed server and forgets it. Messages are lost during a Redis failover, a server crash, a socket swap (drain, rebalance) and while a client is offline.

**Durable path (no confirmed message lost).**
1. The client sends `SendDurable(msgId, room, user, text)`.
2. The server appends to Redpanda (idempotent producer, `acks=all`, `min.insync.replicas=2`) and returns `{partition, offset}` only after a majority of brokers hold the record. On failure the call throws and the client retries with the same `msgId`.
3. Every server tails every partition and writes each record to its own local sockets as `LogMessage(partition, offset, ...)`, through one writer queue per connection. Redis is not on this path.
4. The client stores the highest offset per partition (in `sessionStorage`) and drops anything already shown, by `(partition, offset)` and by `msgId`.
5. On every reconnect the client calls `Resume(offsets)`. The server replays what was missed from the log, buffers live records for that connection meanwhile and merges by offset, then goes live. Offsets older than retention are reported as truncated.

## Background activity

| Who | What | How often |
|---|---|---|
| Each server | Heartbeat (connections, calls/s, colour slot, draining flag) to Redis hash `demo:servers` | 1 s |
| Lowest-named live server (leader) | Aggregates heartbeats, roster, Redis INFO and broker count; pushes `ClusterStats` to every client | 1 s |
| Every server | If above the cluster average by a margin, asks a batch of its clients to reconnect (gateway sends them to the emptiest server); 3 s cooldown | 1 s check |
| Each client | RTT ping; SignalR keep-alive every 5 s, silent peer dropped after 15 s | 3 s / 5 s |
| Gateway | `/healthz` per server; 503 or two failures removes a destination without touching its open sockets | 1 s |
| Log tail (per server) | Remembers next offset per partition; watchdog restarts a stalled consumer from there | 5 s |
| Sentinels | Watch Redis; after 3 s without an answer and a quorum of 2, promote a replica | continuous |

## Failure behaviour

| Event | Original path | Durable path |
|---|---|---|
| Server killed | Messages for its clients lost until they reconnect | Unconfirmed sends retried, receivers resume from their offset |
| Graceful stop (SIGTERM) | Server fails `/healthz`, waits 3 s, asks clients to reconnect in 10 waves, exits when empty (max about 14 s); about 1.3 % lost | Replay covers the swap |
| Redis primary failover | Messages during the 3-10 s election lost | Not on the path; only presence and stats pause |
| Redis completely down | Chat stops | Chat continues; cluster panel and poll degrade |
| One Redpanda broker down | n/a | Raft elects new leaders; nothing lost |
| Two brokers down | n/a | Writes refused (min ISR 2), senders see "not persisted" and retry; arrives late, nothing dropped |
| Phone asleep | Everything missed | Replayed on resume within 24 h |
| `docker compose down` | Everything gone | Broker data stays on external volumes |

Audited results (300 connections, 3 senders at 5/s, 60 s, failure at 20 s): server kill, graceful restart, Redis failover, one broker, two brokers, controller kill and server kill plus broker stop all ended with 0 missing. See the README for the table and the lessons.

## Design decisions worth knowing

- **WebSockets only, no negotiate**: no sticky sessions needed, any server can take any client.
- **Fixed IPs on Redis and Redpanda nodes** (`172.30.77.0/24`): a restarted container otherwise gets a new IP and peers keep the old one. For Redis it also prevented every message arriving twice after a failover (the backplane demands the primary, `PrimaryOnlyBackplane` in `RedisHa.cs`).
- **Graceful drain runs in `ApplicationStopping`**, because in minimal hosting Kestrel stops before hosted services (`GracefulDrain` in `RedisHa.cs`).
- **External volumes** for Redpanda data so no compose command deletes them.
- **No `--mode dev-container`** for Redpanda: it skips fsync.
- **Ordering**: a room maps to one partition (key `room:<name>`), "everyone" to one partition (key `all`). Fine for a lab, a hot spot at scale.
- **Not covered**: authentication, TLS, multi-host deployment, Redis persistence, schema evolution.

## Where to read the code

| Topic | File |
|---|---|
| Hub methods (chat, rooms, canvas, poll, lab commands, `SendDurable`, `Resume`) | `src/ChatServer/ChatHub.cs` |
| Service wiring, `/healthz`, `/stats` | `src/ChatServer/Program.cs` |
| Heartbeats, leader, rebalancing, cluster stats | `src/ChatServer/ClusterReporter.cs` |
| Redis Sentinel config, failover, graceful drain, primary-only backplane | `src/ChatServer/RedisHa.cs` |
| Producer, log tail, resume, per-connection writer queues | `src/ChatServer/DurableLog.cs` |
| Browser client | `src/ChatServer/wwwroot/app.js` |
| Gateway: DNS discovery, status page | `src/Gateway/` |
| Load generator and audit | `src/LoadTest/Program.cs` |
| Whole environment | `docker-compose.yml` |
