# 060 · One storage node crashed and 8% of profile reads failed, although every key had two healthy copies

## Problem
A user-profile service reads from a 12-node key-value store that keeps 3
replicas of every key. To keep each node's in-memory cache hot, the client
library always sends a key to the same replica, chosen with rendezvous
hashing, and on a timeout it retries the request up to 3 times in total.

One node crashed during a kernel upgrade. Monitoring expected no user impact
because every key had two other copies, yet about 1 in 12 profile reads
started failing with "storage unavailable", login pages errored for those
users, and the dead node's address kept appearing in client timeout logs
until it came back 40 minutes later.

## Root cause
The client chose the replica **once**, before the retry loop, so every retry
went to the same node. For a key whose first choice was the dead node, all
three attempts timed out on the same machine while the two healthy replicas
were never asked. Retries only help if they can land somewhere different.
With 10,000 keys and one node down (`naive_get`), 8.42% of reads failed
(close to the expected 1 in 12), and the dead node received 2,526 requests:
3 for every key it was first choice for.

## Solution
`failover_get` keeps the locality of the original design but changes what a
retry means:

- The key's **preference list** (its 3 replicas, best rendezvous score first)
  is computed once, and attempt *n* goes to replica *n*. A healthy first
  replica still serves every request, so caches stay hot.
- A replica that just failed is **never retried** while an untried one is
  left. If there are more attempts than replicas, the list wraps around.

Results on the same 10,000 keys:

| Nodes down | `naive_get` failures | `failover_get` failures |
|---|---|---|
| 1 | 8.42% | 0% |
| 2 | 17.14% | 0% |
| 3 (all replicas of some keys) | 24.61% | 0.48% |

With one node down, the dead node now gets one attempt per affected key
(842 requests instead of 2,526) and total load is 10,842 requests for
10,000 reads. With three nodes down only keys whose three replicas are all
dead fail, about 1 in C(12, 3) = 220 replica sets.

## Trade-offs
- **Each affected read still pays one timeout.** Add outlier detection or a
  per-node circuit breaker (problem 020's failure detector, or
  backend-api-solutions problem 009) so the client skips a node known to be
  down instead of discovering it on every key.
- **Only for reads, or writes that are safe to repeat.** A write retried on
  another replica must be idempotent or coordinated by the store.
- **Other replicas may be stale.** With asynchronous replication a failover
  read can return older data; use quorum reads (problem 014) where that matters.
- **Load shifts to the next replicas.** The dead node's traffic lands on its
  keys' second choices; rendezvous hashing spreads it over all other nodes,
  but each must have headroom for it.

## Run
```bash
python -m unittest -v test_solution.py
```
