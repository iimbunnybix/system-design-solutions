# 064 · The moment reads moved to the new cache cluster, product pages failed and the database read load jumped 21x

## Problem
A retail site serves product pages through a cache cluster in front of the
product database. The team built a new cache cluster (new region, larger
instances) to replace the old one. On migration day they changed the
client config so that **100% of reads went to the new cluster at once**.

The new cluster was empty. Reads that the old cluster answered from memory
all became misses and went to the database. Within a second the database
connection pool was exhausted, page loads timed out, and the on-call
engineer saw database read QPS jump by more than 20x on the dashboard.

In the simulation (50,000 products with Zipf-like popularity, 1,000 reads/s,
10,000 keys per cache cluster, seeded traffic):

- Before the cutover the old cluster hit **95.3%** of reads: the database
  saw **47.3 reads/s**.
- In the second of the cutover the database received **1,000 reads**, 21x
  normal. It can serve 300 reads/s, so **705 reads failed** and showed users
  an error page; database load stayed above its 150 reads/s budget for
  4 seconds.

## Root cause
A cache's hit rate is a property of its *contents*, not of the cluster. The
new cluster had none of the hot keys, so its hit rate started at 0%, and
every read became a database read until it was refilled. Flipping all
traffic at once meant the database had to absorb the whole fill load in one
moment, and many concurrent misses for the same hot key all reached it
before the first fill landed. Nothing measured whether the new cluster was
ready before it took traffic, and nothing pulled traffic back when the
database overloaded.

This is different from a scan flushing an LRU (040), where the cache is
emptied by the wrong traffic, and from adding a node without consistent
hashing (001), where keys are remapped. Here the cache is new and simply
cold.

## Solution
`warmed_cutover` moves traffic in two phases:

1. **Shadow fill (dual-read).** Reads are still answered by the old
   cluster. For each read the client also looks the key up in the new
   cluster (to measure its would-be hit rate) and writes the value it
   already has into it. Database load does not change: the first 30 seconds
   are identical to the naive run.
2. **Guarded ramp.** `RampController` moves a stable, hash-chosen share of
   keys to the new cluster in steps of 5%, 25%, 50%, 100%. Every
   10 seconds it advances one step only if the new cluster's hit rate is
   within 2 points of the old one's. If database load in any second exceeds
   the budget, it steps back one step at once.

The naive one-step flip is kept as `naive_cutover`.

| | Peak DB reads/s | Seconds over budget | Failed reads |
|---|---|---|---|
| Flip 100% at once | 1,000 | 4 | 705 |
| Ramp only, no shadow fill | 483 | 4 | 218 |
| Shadow fill + guarded ramp | 67 | 0 | 0 |

With shadow fill the new cluster was warm after 70 seconds and all traffic
was on it at second 100. Over seeds 0 to 4 the fixed cutover never failed a
read.

## Trade-offs
- The ramp alone is not enough: without shadow fill, each step sends cold
  keys to the database, and the step-back reacts only *after* the budget is
  broken (it stepped back four times and still failed 218 reads). Shadow
  fill is what keeps the database flat; the step-back is the safety net.
- Shadow reads double cache read traffic during the migration and add
  cross-region latency if done synchronously; do them asynchronously or on
  a sample of reads, at the cost of a slower warm-up.
- Copying values from the old cluster can copy stale entries. Use the same
  TTLs and invalidations on both clusters while both are live (dual-write
  deletes).
- Alternatives: bulk-copy the hot key set (from the old cluster's key dump
  or access logs) before the cutover, or put request coalescing in front of
  the database so concurrent misses for one key cost one read.

## Run
```bash
python -m unittest -v test_solution.py
```
