# 001 · Adding one cache server during a sale emptied the whole cache and took down the DB

## Problem
A product catalogue is cached on 4 Redis nodes; keys are placed with
`node = hash(key) % N`. During a festive sale traffic doubled, so ops added
a 5th node. Immediately ~80% of cache lookups missed, every miss went to the
database, and the DB fell over in under a minute.

## Root cause
With modulo placement, changing `N` from 4 to 5 changes the node for almost
every key: only keys where `hash % 4 == hash % 5` stay put, about 1 in 5.
So scaling the cache *up* behaves like flushing it.

## Solution
**Consistent hashing with virtual nodes:**

- Servers and keys are hashed onto the same ring (0 … 2³²).
- A key belongs to the first server clockwise from its position.
- Adding a server only takes over the arcs just before its points, so only
  about **1/(N+1)** of keys move; removing one moves only its own keys.
- Each physical server gets many **virtual nodes** (e.g. 150 points) so load is
  even; weights let bigger machines take more points, and weight 0 drains a node (it stays a member but owns no keys); negative weights are rejected.
- Lookup is a binary search over sorted ring positions: O(log V).

The tests measure the actual fraction of 100,000 keys that move and the
load spread across servers.

## Results (100,000 keys)
| Change | Keys moved: `hash % N` | Keys moved: consistent hashing |
|---|---|---|
| 4 → 5 nodes | ~80% | ~20% (the theoretical minimum) |
| Remove 1 of 5 nodes | ~80% | ~20% (only that node's keys) |

## Trade-offs
- More virtual nodes = smoother load but a bigger ring in memory.
- Hot keys still overload one node; fix with replication or local caching.
- Moved keys still miss once; warm the new node or ramp traffic gradually.

## Run
```bash
python -m unittest -v test_solution.py
```
