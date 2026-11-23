# 067 · Like counts on popular posts drifted 15% above the real number, and the "fix" made them drop by 60%

## Problem
A social app serves users from three regions (EU, US, AP). To keep likes
fast, each region accepts likes locally and keeps its own copy of every
post's like count; a background job syncs the regions every few seconds
over a message bus with at-least-once delivery.

Creators noticed that counts on viral posts were higher than the number of
people in the "liked by" list, and that the three regions showed different
numbers. The like count is used for trending and creator payouts, so the
finance team flagged it. A quick fix shipped the next day made counts
*drop*: posts lost most of their likes in front of their creators.

## Root cause
**The merge operation was not idempotent.** Each region sent "I received N
new likes since my last sync" and receivers did `total += N`
(`DeltaSyncReplica`). When an acknowledgement was lost, the bus redelivered
the message and the same N was added twice. Every retry permanently
inflated the count. In the simulation (3,000 likes spread over three
regions, a sync every 50 likes, 20% of messages delivered twice), the
regions end at **3,439, 3,441 and 3,480**: about 15% too high, and not even
equal to each other.

The quick fix sent whole totals and kept `max(mine, theirs)`
(`MaxTotalReplica`). Max is idempotent, but it is not a sum: if EU and US
each gained likes since the last sync, keeping the bigger total throws the
other region's likes away. The same run ends at **1,201** in every region,
60% of the likes lost.

## Solution
Use a **PN-counter**, a conflict-free replicated data type
(`PNCounterReplica`):

1. Each replica keeps two maps, `likes` and `unlikes`, from **origin region
   to the number of operations that region accepted**. A region only ever
   increments its own slot.
2. Syncing sends the full maps (`state`). Merging takes the **per-region
   maximum** (`merge_counts`). A region's own count only grows, so the
   higher number is always the newer one, and max never loses or repeats
   anything.
3. The value is `sum(likes) - sum(unlikes)`.

The merge is idempotent (a duplicate is harmless), commutative and
associative (order and grouping do not matter), so with retries and
shuffled delivery all three regions end at exactly **3,000**.

## Trade-offs
- State grows with the number of regions (or replicas) that ever wrote,
  per counter. Three regions is trivial; per-device counters would not be.
  Keep the origin to a small, stable set of writers.
- Sending full state is larger than sending deltas. Delta-state CRDTs send
  only changed entries, but must keep the "max per origin" merge to stay
  idempotent.
- A PN-counter counts operations, not people: one user liking twice from
  two regions counts twice. "Has this user liked this post" needs a set
  (an observed-remove set) keyed by user, and the count derived from it.
- Counts are only eventually consistent: for a few seconds each region
  shows a different number. For payouts, compute from the durable like
  events, not from the live counter.
- Alternatively, make delta messages idempotent with a per-sender sequence
  number that receivers deduplicate. That works, but needs ordered
  delivery or gap handling per sender, which the CRDT avoids.

## Run
```bash
python -m unittest -v test_solution.py
```
