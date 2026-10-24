# User manual: the lab web page

Open `http://localhost:9000` (or `http://<computer-name-or-IP>:9000` from a phone or another computer on the same network; the **Invite devices** button shows a QR code). Open it on two or more devices or tabs to see anything interesting.

## Screen layout

- **Header (always visible):** your device name (editable, remembered), which server you are on (coloured chip), round-trip time (RTT), messages received per second, the **accept remote cmds** checkbox, **Invite devices**.
- **Cluster panel (always visible, scrolls inside itself):** one row per server with colour, connection count, virtual connections and calls per second. Redis line (primary, replicas), log line (brokers, partitions), **Auto-rebalance** toggle, list of online clients.
- **Tabs (bottom bar):** Chat, Canvas, Poll, Lab. Only the tab content scrolls.

Each server has its own colour and a short id chip, shared by every client, so "who handled this" is always the same colour.

## Chat

- Pick the audience next to the message box: Everyone or a room (A, B, C). Each message shows an audience tag.
- Join or leave rooms with the room chips. Messages to a room reach only devices that joined it.
- **Durable** checkbox: the message goes through the Redpanda log. It shows pending, then "persisted pX@Y" (partition and offset) once a majority of brokers hold it. After a reconnect the page replays what it missed. Try it: send durable messages while you run `docker compose stop redpanda-2`, or kill a server.
- Without Durable the message goes through Redis pub/sub and can be lost in failures.

## Canvas

Press **Draw** to enter draw mode (so touch scrolling still works otherwise), pick a colour, and draw. Strokes appear on every device. **Clear for all** wipes everyone's canvas. Late joiners see only new strokes (the backplane relays, it does not store).

## Poll and reactions

Vote on the poll (counts live in Redis; the backplane only tells everyone to refresh). Tap emoji to float them on every screen; the page shows reactions per second across the cluster.

## Lab

**Target** card: when "run on all devices" is ticked, experiments start on every device that has **accept remote cmds** ticked. **Stop everything on all devices** halts every running experiment.

| Experiment | What it does | What to look at |
|---|---|---|
| 1 Latency probe | 20 round trips to your server | RTT min/avg/max; compare phone and laptop |
| 2 Server burst | Your server publishes N messages to everyone or to rooms through Redis | Per-device table: got, rate, out of order. Loss means the at-most-once path dropped something |
| 3 Flood | Every device sends messages at a rate for N seconds, to everyone, random rooms or mixed | Deliveries per second, latency per device |
| 4 Virtual users | This browser opens +50/+200/+500 extra WebSocket connections; each is a real client | Connection counts per server in the cluster panel. **Close mine** closes them; they stay until you do |
| 5 Durable audit | Every device sends numbered messages through the log; each receiver checks per sender for gaps | **GAPS must stay 0**, even while you crash a server, drain one, fail Redis over or stop a broker |
| 6 Resilience | **Crash my server** (abrupt exit), **Gracefully shut down my server** (drain), **Redis failover** | Cluster panel and the event log: how long clients take to move, what was lost |

Event log at the bottom: connects, reconnects, moves ("moved ... in Ns"), replays, notices.

## Suggested first session (10 minutes)

1. Open the page on two devices. Send an Everyone message and a room message and see who gets them.
2. Lab 4: add 200 virtual users. Watch the three servers balance, then turn Auto-rebalance off and on.
3. Lab 2: run a burst of 5000. Note any "out of order" or missing.
4. Lab 5: start the audited flood. While it runs press **Crash my server**, then **Redis failover**, then in a terminal `docker compose stop redpanda-2`. GAPS stays 0.
5. Repeat 3 without Durable (Lab 3 flood) while crashing a server and compare losses.

## Other pages

- `http://localhost:8404` gateway status: destinations, health, open sockets per server (`/api` for JSON).
- `http://localhost:8082` Redpanda Console: brokers, topic `chat.messages`, partitions, live messages.
- `http://localhost:9000/stats` one server's counters (through the gateway, a random one).

## Troubleshooting

| Problem | Cause and fix |
|---|---|
| Phone cannot open the page | Different network, name not resolved, or firewall. Use the computer's IP address or the QR code; allow Docker Desktop in the firewall |
| Page says reconnecting for a long time | The server it was on is gone; it retries with back-off. Reload if it persists |
| ~50 virtual connections you did not start | Another device or old tab opened them; press **Close mine** there or **Stop everything on all devices** |
| "not persisted" on a durable send | Fewer than 2 brokers are up; the page keeps retrying and delivers when they return |
| Everything looks wrong after `compose up` | Redpanda takes about a minute to form its cluster; servers start after the topic exists |
