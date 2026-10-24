# Sharding the "Everyone" messages: considerations

Status: analysis only, nothing here is implemented.

## The problem

`ChatLog.AppendAsync` (`src/ChatServer/DurableLog.cs`) keys every durable message with `all` when it is addressed to Everyone, and `room:<name>` when it is addressed to a room. Redpanda hashes the key to pick the partition, so **all Everyone traffic lands in one of the topic's 12 partitions**:

- Throughput for Everyone is capped at what one partition leader (one Raft group, one broker core) can write. The other 11 partitions and most of the cluster sit idle for that traffic.
- That broker is also the hot spot for failures: when its leader moves, all Everyone senders see the same latency spike at once.
- It does not matter in this lab (a few hundred messages per second). It matters when Everyone is the dominant audience.

Rooms already shard naturally: each room name hashes to its own partition. A busy room is the same problem on a smaller scale.

## What the single partition buys today

Sharding trades away real properties, so list them first:

1. **One total order for Everyone.** Every client sees Everyone messages in the same order, because a partition is an ordered list.
2. **Trivial per-sender order.** A sender's messages are in one partition, hence in order.
3. **Simple resume.** A client's bookmark for Everyone is one offset on one partition.

After sharding you keep (2) only if the shard is chosen per sender, and you lose (1).

## Options

| Option | Key | Keeps | Loses | Notes |
|---|---|---|---|---|
| Per-sender shard | `all:<hash(sender) mod N>` | Order of each sender's messages | One global order | Best default for chat. A very chatty sender is still one hot key |
| Per-message spread | `all:<hash(msgId) mod N>` or no key | Nothing | All ordering | Even load. Only fine if the client orders by timestamp or sequence |
| Time bucketing | `all:<minute mod N>` | Rough order within a bucket | Strict order, hot partition per bucket | Rarely a good idea |
| Dedicated topic per audience class | topic `chat.everyone` | Separate retention and partition count | Two topics to tail and resume | Useful if Everyone needs different retention or many more partitions than rooms |
| Keep one partition, move the hot spot | pin partition leader to a quiet broker | Order | Throughput ceiling | Only a mitigation |

`N` (the number of Everyone shards) should be at most the topic's partition count (12), and in practice a divisor of it. Two shard keys can still hash to the same partition, so the spread is statistical, not guaranteed. Choose `N` after measuring: run the audited flood with `SENDERS` well above 3 and compare per-partition rates in Redpanda Console.

## What would change in the code

| Area | Change | Size |
|---|---|---|
| Producer (`ChatLog.AppendAsync`) | Build the key from the chosen strategy instead of the constant `all` | Small, a few lines |
| Tail (`ChatLogTail`) | None. Each server already reads every partition from explicit offsets and delivers by envelope room, not by partition | None |
| Resume (`LogDelivery.Collect`) | None for correctness. It already filters records by the envelope's room (empty = Everyone) across all partitions and merges the batch by timestamp. Check the merge key: with several Everyone partitions the broker timestamp is the only cross-partition order it has | Review only |
| Client (`app.js`) | None for correctness: offsets are kept per partition and duplicates are dropped by `(partition, offset)` and by message id. For a consistent on-screen order across devices, insert messages by timestamp or by `(sender, sequence)` instead of arrival order | Small to medium |
| Load test and audit | The audit already checks gaps per sender, which matches per-sender sharding. Add a check that more than one partition was used, and raise `SENDERS` | Small |
| Docs and UI | State that Everyone order is per sender | Small |

## Consistency questions to decide first

1. **Do users expect everyone to see the same order?** In chat, usually yes for a conversation, and not for a firehose such as reactions or a live feed. If yes, sharding Everyone is the wrong tool for the conversation case.
2. **What defines "order"?** Per sender is cheap. Causal order (a reply appears after the message it answers) needs the reply to carry the id of its parent, and the client to hold a reply back until the parent is shown.
3. **Who sorts?** Server-side sorting would need coordination between servers and defeats the point. Client-side sorting by timestamp is easy but clocks differ. A broker timestamp (set at append) is consistent per partition, not across partitions, and two partitions can disagree by milliseconds. A hybrid logical clock or a per-sender sequence number gives a deterministic order.
4. **Late joiners and replay.** A client that resumes replays several partitions and must interleave them. Define the tie-break (timestamp, then partition, then offset) and use it everywhere so replay and live delivery agree.

## Side effects to expect

- **Read load does not fall.** Every server still reads every partition, so reads scale with messages times servers. Sharding relieves the write hot spot only. Cutting reads would mean assigning partitions to servers and routing each client to the servers that hold its audience, which gives up "any client on any server".
- **Rebalancing and shard count.** Changing `N` later changes which partition a sender maps to. A sender's order across the change is no longer guaranteed. Pick `N` generously at the start or accept one-time reordering.
- **Hot keys remain.** One sender who produces most of the traffic still fills one partition. Per-sender sharding does not fix that case.
- **Ordering across rooms** was never promised and is unchanged.
- **Retention and compaction** are per partition and unaffected.
- **Exactly-once on screen** does not depend on the shard: the id check and the offset check still apply.

## Suggested path if this is ever built

1. Measure first. Run the audited flood with many senders and read per-partition write rates in Redpanda Console to see whether the single partition is the actual limit.
2. Implement per-sender keys with a configurable `N` (environment variable, default 1 = today's behaviour).
3. Add a deterministic client-side order (timestamp, partition, offset) and an on-screen note that order is per sender.
4. Extend the audit with a check that Everyone traffic hit more than one partition, and rerun the chaos scenarios from the README.
5. Compare p50 and p99 and the broker CPU against the single-partition baseline before keeping it.
