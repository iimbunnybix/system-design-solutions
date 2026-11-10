# 015 · Two replicas quietly disagreed for weeks, and the nightly repair job that should have fixed it ran for hours and saturated the network

## Problem
A user-profile store keeps every key on several replicas. One replica was
briefly partitioned and missed a few dozen writes; hinted handoff expired
before it came back, so nothing re-sent them. Reads that hit that replica
returned old profiles, and nobody noticed for weeks because the difference was
only a handful of keys out of millions.

The team had a "repair" job: stream every key and version from one replica to
the other and compare them one by one. With millions of keys it took hours,
saturated the inter-rack link while it ran (slowing live traffic), and was
eventually disabled — which is why the divergence was never repaired.

## Root cause
The repair cost was proportional to the **total number of keys**, not to the
**number of keys that differ**. A full comparison ships everything even when
99.98% of it is identical, so it was too expensive to run often and got
switched off.

## Solution
**Merkle-tree anti-entropy** (as used by Dynamo, Cassandra and Riak):

1. Hash every key to one of 1,024 buckets with a stable hash (SHA-256, not
   Python's per-process randomised `hash()`).
2. Hash each bucket's sorted `(key, version, value)` entries into a leaf.
   Every field is length-prefixed, so a value that happens to contain
   separator bytes can never make two different buckets hash the same.
3. Build a binary tree: each parent is the hash of its two children.
4. The replicas compare roots. Equal roots → done, one hash exchanged.
   Otherwise they descend only into children whose hashes differ.
5. For each differing leaf bucket, only that bucket's keys are exchanged and
   reconciled: the higher version wins (ties are broken on the value so both
   sides still converge).

Results from the tests (100,000 keys, 1,024 buckets, 22 differing keys):

| Repair method | Hashes compared | Keys exchanged | Keys repaired |
|---|---|---|---|
| Naive full comparison | 0 | 100,001 | 22 |
| Merkle tree | 235 | 1,982 | 22 |
| Merkle tree, replicas already equal | 1 | 0 | 0 |

That is about 50× less key traffic for the same repairs, and a check of two
healthy replicas costs a single hash.

## Trade-offs
- The tree must be rebuilt (or updated incrementally) as data changes; here
  it is rebuilt from scratch on every repair, which is CPU work on each side.
  Real systems keep per-range trees and update leaves on write.
- Bucket count is a tuning knob: more buckets means smaller key exchanges but
  a bigger tree; fewer buckets means each differing bucket drags more
  identical keys along.
- Highly scattered divergence (one bad key in every bucket) degrades towards
  a full comparison — then a full resync is simpler.
- "Higher version wins" assumes versions are trustworthy; concurrent writes
  need vector clocks or CRDTs (see problem 006).
- Deletes need tombstones, otherwise repair resurrects deleted keys.

## Run
```bash
python -m unittest -v test_solution.py
```
