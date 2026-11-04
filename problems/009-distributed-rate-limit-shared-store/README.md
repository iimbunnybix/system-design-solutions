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
| **Shared sliding window, 10 or 20 nodes** | **≤ 100 in any 60 s window** |

## Solution
`SharedStore.sliding_window_acquire` + `GatewayNode` in `solution.py`:

1. **Shared counters** keyed by `rl:<api key>:<window index>` in a central
   store (Redis in production), so every node sees the same count.
2. **Sliding window counter:** `estimate = prev × (1 − elapsed fraction) +
   curr`. It needs only two integers per key, avoids the double burst a fixed
   window allows at its boundary, and does not reset at the minute mark: after
   a 100-request burst, a key gets 0 more at t = 60 s and 50 more at t = 90 s.
3. **Atomic check-and-increment** (one Lua script in Redis). Ten threads
   making 500 concurrent attempts against a limit of 100 get exactly 100 through.
   A separate GET then INCR from each node would let several nodes admit the
   "last" request.
4. **Degraded mode:** if the store is unreachable, each node falls back to a
   local limit of `limit / fleet_size` (10). With round-robin that keeps the
   fleet near the quota (100 admitted in the outage test) without making the
   rate limiter a hard dependency of the whole API.

The result is independent of fleet size: 496 requests admitted over 5 minutes
with either 10 or 20 nodes, and a sticky client at 80/min gets all 400 of its requests.

## Trade-offs
- **One network round trip per request** to the store (sub-millisecond on a
  local Redis, but it adds up). For very high rates, nodes can batch: take a
  local allowance of N tokens from the store at a time, accepting slight
  overshoot.
- **The store becomes critical infrastructure.** It needs replication and
  monitoring; the fallback policy (fail open, fail closed or per-node share,
  as here) is a business decision.
- The sliding window counter is an approximation that assumes requests in
  the previous window were spread evenly. A sliding *log* is exact but stores
  every timestamp; GCRA/token bucket in the store is another common choice
  (see backend problem 002 for the single-node token bucket).
- Multi-region deployments either accept per-region quotas or pay
  cross-region latency for a global counter.

## Run
```bash
python -m unittest -v test_solution.py
```
