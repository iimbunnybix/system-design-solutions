# 047 · The team tripled the consumers during a 3.6-million-message backlog, and the backlog drained no faster

## Problem
An order-events pipeline reads a Kafka-style topic with **6 partitions**
through a consumer group of 6 consumers. Each consumer handles about
**700 messages/s**; producers write about **3,000 messages/s** at peak.

After a downstream outage the topic held a backlog of **3.6 million
messages** (600,000 per partition). Fulfilment dashboards were 50 minutes
behind and the business wanted the lag gone within 15 minutes. The on-call
engineer scaled the consumer deployment from 6 to **18 pods**. CPU on the new
pods stayed at zero, consumer lag kept falling at exactly the same rate as
before, and the backlog took the full **50 minutes** (3,000 s) to drain.

In the simulation (`naive_scale_consumers`), drain time is **3,000 s with 6
consumers and 3,000 s with 18**; both the range and the round-robin assignor
leave **12 of the 18 consumers with no partition**.

## Root cause
**Within one consumer group, each partition is consumed by at most one
member.** That is how the broker keeps per-partition order. The partition
count is therefore the ceiling on a group's parallelism: with 6 partitions,
members 7 to 18 joined the group, received an empty assignment and sat idle.
Each busy consumer only had `700 - 500 = 200` msg/s of spare capacity after
keeping up with its partition's new messages, so 600,000 messages took
3,000 s no matter how many idle pods were added.

(Problem 039 is about ordering broken by round-robin partitioning; here the
ordering is fine and the problem is parallelism capped by partition count.)

## Solution
A capacity planner (`plan_capacity`) sizes the **partition count**, not the
pod count, ahead of time:

1. **Required throughput** = peak inflow + backlog / target drain time
   = 3,000 + 3,600,000 / 900 = **7,000 msg/s**.
2. **Partitions** = ceil(required × (1 + headroom) / per-consumer throughput)
   = ceil(7,000 × 1.25 / 700) = **13**. One partition feeds at most one
   consumer, so this is also the useful maximum number of consumers.
3. **Warnings** for the engineer:
   - more consumers than partitions, e.g. 18 consumers on 13 partitions:
     "5 consumers will sit idle";
   - growing an existing topic (6 to 13): keys are remapped to new
     partitions, so per-key ordering is not guaranteed across the change, and
     the existing backlog stays on the old partitions.

On a topic created with the planned 13 partitions, the same 3.6M backlog
drains in **590 s** (under the 900 s target) with 13 consumers; 18 consumers
give the same 590 s, because 5 are idle.

Simulation results:

| Setup | Consumers | Idle | Drain time |
|---|---|---|---|
| 6 partitions (incident) | 6 | 0 | 3,000 s |
| 6 partitions (incident) | 18 | 12 | 3,000 s |
| 6 partitions grown live to 13 | 13 | 0 | 1,279 s |
| 13 partitions planned up front | 13 | 0 | **590 s** |

Growing the topic **during** the incident helps only a little: the 600,000
messages already on each old partition do not move, and only new messages
spread out, so the old partitions still need 1,279 s. Partitions must be
sized before the backlog, which is why the planner runs at design and review
time.

**Ordering caveat:** the producer maps `blake2b(key) mod partitions`. Going
from 6 to 18 partitions moves **66.6%** of 10,000 sample keys to a different
partition (from 6 to 13: 92.5%). For a while, a key's old events sit on its
old partition and its new events on a new one, read by different consumers,
so they can be applied out of order unless producers pause until the old
partitions are drained or consumers check per-key versions.

## Trade-offs
- **More partitions are not free.** Each one costs broker memory, file
  handles, replication traffic and longer leader elections, and makes
  rebalances slower. Plan for realistic peak plus headroom, not for "as many
  as possible".
- **Pick counts that divide evenly.** 13 partitions on 13 consumers is even,
  but if you later run 6 consumers the range assignor gives one consumer three
  partitions and the other five two. Many teams round up to a number with many
  divisors (12, 24, 36) so common group sizes balance.
- **Per-consumer throughput must be measured**, not guessed, and is often
  bounded by the downstream system (database, API) rather than the consumer.
  If the downstream is the bottleneck, more partitions will not help either.
- **Alternatives when you cannot repartition:** process messages from one
  partition concurrently inside a consumer (per-key worker queues, as in
  Confluent's parallel consumer), or copy the backlog to a temporary topic
  with more partitions; both keep per-key order only if work is split by key.
- The model assumes evenly spread backlog and steady inflow; hot partitions
  (one key dominating) cap throughput regardless of partition count.

## Run
```bash
python -m unittest -v test_solution.py
```
