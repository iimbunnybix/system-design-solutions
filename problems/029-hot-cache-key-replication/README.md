# 029 · One product went viral and a quarter of all page loads timed out on a single cache node, while the other nine sat idle

## Problem
An online store caches rendered product pages in a 10-node cache cluster.
Each key lives on exactly one node, chosen by hashing the key, and each node
can serve about 20,000 reads per second. Normal traffic is about
100,000 reads/s spread over thousands of products, so every node runs at
roughly 50%.

Then a celebrity posted a link to one pair of sneakers. Within minutes 40% of
all reads were for that one product page. The node that owned the key went to
100% CPU, its reads started timing out, and every page that needed any key
on that node (not just the sneakers) failed or fell back to the database.
The dashboard showed one red node and nine nodes at 30%. The on-call engineer
added two more cache nodes, which changed nothing: the hot key still lived
on one node. The store lost a quarter of its page loads at the moment it had
the most visitors.

## Root cause
**Placement by key hash spreads keys, not load.** Hashing guarantees that
*keys* are spread evenly, but a single key's reads all go to its one node, so
the most a key can get is one node's capacity. When one key's demand
(40,000 reads/s) is twice a node's capacity, no number of extra nodes helps.

The model (`NaiveCacheClient`, one second of spike traffic, seed 29) gives:

| Node | Reads in 1 s | Over capacity |
|---|---|---|
| Viral key's node | 45,468 | 25,468 |
| Busiest other node | 6,518 | 0 |

25,468 of 100,000 reads (25%) are above capacity and time out, while the other
nodes use at most a third of theirs. This is different from a cache stampede
(problem 003: many misses for one key at expiry) and from a hot database row
(problem 007: write lock contention). Here every read is a cache *hit*; the
one node simply cannot serve them all.

## Solution
**Detect hot keys and serve them from several copies on different nodes.**

- `HotKeyDetector` keeps the last 1,000 reads in a sliding window with a
  count per key. A key with at least 20 of them (2% of traffic) is hot. When
  its share falls, it leaves the window and is no longer hot.
- For a hot key, `ReplicatingCacheClient.get` picks one of 5 copies at random
  (`key`, `key#1` … `key#4`, seeded RNG). Copy *i* lives on the *i*-th node
  after the key's home node, so the copies are always on 5 different nodes.
  (Hashing `key#i` independently would sometimes put two copies on one node.)
- A missing copy is filled from the home copy, not from the database, so
  replication adds no database reads (the test checks this: 9,969 DB reads
  in both versions, all of them first reads of catalog keys).
- `set` writes the home copy and deletes every other copy; `invalidate`
  deletes all copies. Both always fan out to all copies, even when the key is
  no longer hot, because copies from an earlier hot period would otherwise
  serve old prices the next time the key gets hot.

Same traffic, same seed:

| Design | Busiest node | Reads over capacity |
|---|---|---|
| One copy per key | 45,468 | 25,468 (25%) |
| Hot keys on 5 copies | 14,156 | 0 |

## Trade-offs
- **Short staleness window across copies.** Between a write and the delete of
  the last copy, a read can still get the old value from a replica. Here
  that is microseconds in one process; across a network it is a few
  milliseconds, fine for a product page but not for a stock count that must
  be exact.
- **Memory:** a hot key is stored 5 times. Only hot keys are copied, so this
  is a handful of entries.
- **Per-client detection:** each app server has its own detector and its own
  view of what is hot, which is fine because the copies are just cache
  entries. A central detector (or the cache server reporting hot keys, as
  some managed caches do) reacts faster for keys that are hot only in total.
- **Copy count is fixed** at 5 here. A key that needs more than 5 nodes'
  worth of reads needs more copies; sizing copies from the measured share
  (`share × total / capacity`) handles bigger spikes.
- **Alternative:** a small in-process cache (a few seconds of TTL) in each app
  server for hot keys removes the network hop entirely, at the cost of
  staleness up to the TTL and harder invalidation across servers. Many
  systems use both.

## Run
```bash
python -m unittest -v test_solution.py
```
