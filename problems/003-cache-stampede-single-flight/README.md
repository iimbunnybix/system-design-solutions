# 003 · Database CPU spikes to 100% every few minutes, exactly when the homepage cache entry expires

## Problem
A shopping site caches the rendered homepage feed (an expensive query that
joins promotions, inventory and personalisation defaults) with a 60-second
TTL. At peak there are thousands of requests per second for that key. Every
time the entry expires, database CPU spikes, p99 latency jumps for every
page, and occasionally the DB connection pool is exhausted and the site
returns errors for several seconds. Adding database replicas only made the
spikes slightly smaller.

## Root cause
A **cache stampede** (a.k.a. dog-piling). The cache code is "get → miss →
query DB → set". When a hot key expires, *every* in-flight request misses
in the same instant and *each one* runs the expensive query. The DB load
is proportional to concurrent requests, not to distinct keys. The test
`test_naive_cache_stampedes` reproduces it: 50 concurrent requests on an
expired key make 50 DB calls.

## Solution
`StampedeProtectedCache` combines three standard techniques:

1. **Single-flight (request coalescing)**: per key, the first miss becomes
   the leader and runs the loader; everyone else waits on its result. The
   same 50 concurrent requests make **1** DB call. Failures are passed to all
   waiters and **not cached**, so the next request retries. A double-check
   under the lock prevents a second load by a caller that missed just as
   the leader finished.
2. **Stale-while-revalidate**: after the soft TTL (`ttl_s`) the old value is
   still served for `stale_s` seconds while **one** background refresh runs.
   User requests never wait on the DB for a hot key; only a cold or
   hard-expired key loads synchronously.
3. **Probabilistic early refresh (XFetch)**: each read refreshes early if
   `now − Δ·β·ln(U) ≥ expiry`, where Δ is how long the last load took and
   `U ~ Uniform(0,1]`. The probability of refreshing with `r` seconds left is
   `e^(−r/(Δβ))`. Measured with Δ = 1 s, β = 1: **0%** at 30 s left, ~**13.5%**
   at 2 s left, ~**61%** at 0.5 s left. Keys refresh shortly before expiry,
   by one request, rather than all at once after it.

## Trade-offs
- Single-flight here is **per process**. With 200 app servers you can still
  get up to 200 concurrent loads; use a distributed lock (e.g. Redis
  `SET key NX PX`) or a request-coalescing proxy if that is too many.
- Serving stale data is a product decision: fine for a homepage feed, not
  for an account balance. Set `stale_s=0` for data that must be fresh.
- XFetch needs the load time per entry and adds a little extra DB load
  from early refreshes; `β` trades freshness against wasted refreshes.
- Adding random jitter to TTLs prevents *many different* keys expiring at
  once (e.g. after a cache flush), a related but different problem that
  single-flight alone doesn't solve.

## Run
```bash
python -m unittest -v test_solution.py
```
