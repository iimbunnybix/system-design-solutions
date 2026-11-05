# 009 · Basic-plan API keys got 2,000 requests a minute instead of 100, and the first fix throttled paying customers

## Problem
A public API sells plans by request quota: the basic plan allows 100 requests
per minute per API key. Traffic goes through a fleet of API gateway nodes
behind a round-robin load balancer, 10 nodes normally and 20 at peak. A
capacity review found basic-plan keys sustaining 1,000–2,000 requests per
minute, enough to overload a reporting backend. Revenue was affected too:
customers had no reason to upgrade. The quick fix, "set each node's limit to
100 / 10 = 10", produced the opposite complaint the next week: a large
customer whose SDK kept one long-lived connection (always the same node) was
throttled at 10 requests per minute while well under its quota.

## Root cause
**The quota is global, but the counters were local.** Each node kept an
in-memory counter per key and enforced 100/min on its own, so the effective
limit was `100 × nodes`, and it grew every time the fleet autoscaled.
Dividing the limit by the node count assumes perfectly even load balancing,
which sticky connections, consistent hashing and HTTP/2 connection reuse all break.

In the simulation (a client sending 50 requests/second for 5 minutes):

| Design | Admitted per minute |
|---|---|
| Local limit 100, 10 nodes | 1,000 |
| Local limit 100, 20 nodes | 2,000 |
| Local limit 10 (= 100/10), sticky client sending 80/min | 10 |
| **Shared sliding log, 10 or 20 nodes** | **≤ 100 in any 60 s window** |

## Solution
`SharedStore.sliding_log_acquire` + `GatewayNode` in `solution.py`:

1. **Shared state** keyed by `rl:<api key>` in a central store (Redis in
   production), so every node sees the same count.
2. **Sliding log:** the store keeps the timestamps of admitted requests from
   the last 60 seconds (a sorted set in Redis) and admits a request only if
   fewer than 100 are left after dropping older ones. It does not reset at the
   minute mark: after 50 requests at t = 0 s and 50 at t = 30 s, a key gets
   exactly 50 more at t = 60 s. Only admitted requests are stored, so memory
   per key is bounded by the limit (100 timestamps).
3. **Atomic check-and-record** (one Lua script in Redis:
   `ZREMRANGEBYSCORE`, `ZCARD`, `ZADD`). Ten threads
   making 500 concurrent attempts against a limit of 100 get exactly 100 through.
   A separate GET then INCR from each node would let several nodes admit the
   "last" request.
4. **Degraded mode:** if the store is unreachable, each node falls back to a
   local limit: its slice of the quota (`fallback_share`). Every node gets
   `limit // fleet_size` and the first `limit % fleet_size` nodes one extra,
   so the slices add up to exactly the limit (10 each for 100 on 10 nodes; 100
   admitted in the outage test). An earlier version floored each slice at 1,
   so a 5/min plan on 20 nodes admitted 20/min during an outage; now nodes
   0-4 admit 1 each, the other 15 fail closed, and the fleet admits 5. This
   keeps the rate limiter from being a hard dependency of the whole API.

The result is independent of fleet size: 500 requests admitted over 5 minutes
with either 10 or 20 nodes, and a sticky client at 80/min gets all 400 of its requests.

## Trade-offs
- **One network round trip per request** to the store (sub-millisecond on a
  local Redis, but it adds up). For very high rates, nodes can batch: take a
  local allowance of N tokens from the store at a time, accepting slight
  overshoot.
- **The store becomes critical infrastructure.** It needs replication and
  monitoring; the fallback policy (fail open, fail closed or per-node share,
  as here) is a business decision.
- An earlier version used a **sliding window counter** (`prev × (1 − elapsed
  fraction) + curr`, two integers per key). It assumes the previous window's
  requests were spread evenly, so a client that sends 100 requests in the last
  second of a minute and keeps sending got **198** requests through in one
  rolling 60 s window. The sliding log is exact (tested with that same burst)
  at the cost of up to `limit` timestamps per key, which matters for plans
  with very large limits. GCRA/token bucket in the store is exact with one
  value per key and is the usual choice there (see backend problem 002 for
  the single-node token bucket).
- Multi-region deployments either accept per-region quotas or pay
  cross-region latency for a global counter.

## Run
```bash
python -m unittest -v test_solution.py
```
