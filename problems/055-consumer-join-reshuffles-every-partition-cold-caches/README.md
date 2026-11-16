# 055 · Every scale-out of the order consumers caused a lag spike, because one new pod reshuffled a third of all partitions

## Problem
An order-enrichment service consumes a 120-partition topic with 12 consumer
pods. Each pod keeps a warm in-memory cache of customer and merchant records
for the partitions it owns, built up from a database while it processes.
Adding one pod during a traffic peak was meant to *reduce* lag. Instead,
consumer lag jumped on most partitions at once, the database saw a burst of
cache-fill reads, and the backlog took longer to clear than before the
scale-out. Removing a pod (or the leave-and-rejoin of a rolling restart)
caused the same spike.

## Root cause
Partitions were split with a **range assignor**: sort the consumers by name
and give each a contiguous block of partitions. A membership change shifts
every block boundary, so partitions move even between consumers that were
not involved. Measured with `naive_range_assign` on 120 partitions:

| Change | Partitions that change owner |
|---|---|
| 12 → 13 consumers, new name sorts last | 45 |
| 12 → 13 consumers, new name sorts first | 75 |
| 12 → 11 consumers (`worker-05` leaves) | 31 |

Kubernetes pod names end in random suffixes, so where a new pod sorts is
luck. Every moved partition is paused while its offsets are handed over and
then runs on a cold cache, which is the lag spike. Only 9 or 10 partitions
actually *needed* to move in each case.

## Solution
`sticky_assign` takes the previous assignment as input and:

1. Computes fair quotas: every consumer gets `partitions // consumers`, and
   the remainder goes to the consumers that already hold the most, so they
   can keep one more instead of handing it over.
2. Lets each surviving consumer **keep** its old partitions up to its quota.
3. Hands only the leftovers (partitions of departed consumers and over-quota
   ones) to consumers that still have room.

Results on the same 120 partitions: adding a 13th consumer moves 9
partitions (all to the new pod), removing `worker-05` moves exactly its 10,
and an unchanged group moves none. A full rolling restart of all 12 pods
moves 120 partitions in total, 10 per restart, with the group balanced
(shares differ by at most one) after every step.

This is the idea behind Kafka's `StickyAssignor` and
`CooperativeStickyAssignor`. The cooperative variant also lets consumers keep
processing the partitions they retain during the rebalance, instead of every
consumer revoking everything first.

## Trade-offs
- The assignor needs the previous assignment; a coordinator that loses it
  falls back to a fresh (still balanced, but unsticky) assignment.
- It balances partition *counts*, not load. Partitions with very different
  traffic need weighted quotas or lag-aware assignment.
- Stickiness does not remove the cost of the moves that are needed. If a
  cache is expensive to warm, pair this with pre-warming on the new owner or
  a shared cache tier.
- Alternatives: static membership (a restarted pod reclaims its old identity
  and partitions within a timeout, so restarts move nothing), or consistent
  hashing of partitions to consumers, which is sticky but less evenly
  balanced.

## Run
```bash
python -m unittest -v test_solution.py
```
