# 034 · One tenant's month-end dashboard refresh timed out every query on one worker while nine others sat half idle

## Problem
A B2B analytics product runs customer queries on 10 query workers. Each
query is routed by consistent hashing on the tenant id, so a tenant's data
stays warm in one worker's local cache and repeat queries are fast. Each
worker runs up to 150 queries at once; beyond that they queue and hit the
30-second timeout. On a normal peak about 1,000 queries are in flight, 100
per worker.

At month end the largest tenant refreshed every dashboard for its finance
team at once and sent 40% of all concurrent queries. All of them went to
that tenant's home worker. The worker was at three times its capacity,
hundreds of queries timed out, and so did the queries of every small tenant
that happened to share the worker. The dashboard showed one worker pinned
and the other nine at about 30-55% of their slots. Adding workers did nothing: the tenant still
had exactly one home.

## Root cause
**Consistent hashing balances keys, not load.** It gives every tenant one
home and spreads tenants evenly, which is exactly what keeps caches warm,
but it has no idea how much work a key brings. One tenant's share of the
traffic goes to one worker however many workers there are.

`simulate(NaiveRouter)` (1,000 concurrent queries, 200 tenants, the big
tenant sending 40%, seed 34):

| Router | Busiest worker | Least busy | Queries over the 150-slot capacity | Workers the big tenant uses |
|---|---|---|---|---|
| Consistent hashing | 464 | 47 | 314 | 1 |
| **Bounded loads, c = 1.25** | **125** | **62** | **0** | **8** |

Different from problem 029, where one cache key gets so many *reads* that it
needs copies on several nodes and has to be detected as hot first. Here the
work units are separate queries that can run anywhere; they only prefer the
home worker, and no detection is needed.

## Solution
**Consistent hashing with bounded loads** (`BoundedLoadRouter`; Mirrokni,
Thorup and Zadimoghaddam, 2016; used in Google Cloud Pub/Sub and HAProxy's
`hash-balance-factor`):

1. **A bound per worker:** `ceil(c × (in_flight + 1) / workers)` with
   c = 1.25, so no worker holds more than 25% above the average. The `+ 1`
   counts the query being placed, so the workers together always have room
   for it (tested with one key and c = 1.01).
2. **Home first, then clockwise:** a query goes to its tenant's home worker
   if that worker is under the bound, otherwise to the next worker on the
   ring that is. Placement stays deterministic and a tenant's overflow lands
   on the same few neighbours, which keeps some cache locality.
3. **Release on completion** frees the slot, and new queries go back to the
   home worker as soon as it is under the bound again.

In the simulation the busiest worker has 125 queries and none is over
capacity. The big tenant's queries spread over 8 workers, and about 80% of
the other tenants' queries still run on their home worker.

## Trade-offs
- **Locality is traded for balance.** 20% of the small tenants' queries
  moved, because the big tenant's overflow fills the workers clockwise from
  its home and pushes their own tenants further along (a known cascade
  effect). A larger c keeps more at home (c = 1.5: 85%, c = 2.0: 92%) but
  allows a higher peak. Pick c so that `c × average` stays under a worker's
  real capacity: with c = 2.0 the bound is 200 and 100 queries are again
  over the 150 slots.
- **The router needs the current load of every worker.** One router (or a
  load balancer like HAProxy) has it locally; many independent routers need
  shared counters or accept some overshoot from stale load reports.
- **The bound counts queries, not cost.** A few huge queries can still
  overload a worker; weight the load by estimated cost if queries vary a lot.
- **Alternatives:** power-of-two-choices (hash to two candidate workers and
  pick the less loaded) balances well with less state but gives each key two
  homes; splitting a known big tenant over several hash keys works but needs
  someone to know which tenants are big.

## Run
```bash
python -m unittest -v test_solution.py
```
