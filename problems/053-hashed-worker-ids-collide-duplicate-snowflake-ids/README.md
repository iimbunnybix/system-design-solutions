# 053 · Checkout failed with "duplicate key" on new orders after the order service scaled out

## Problem
An order service generates Snowflake-style 64-bit order IDs locally on each
pod: `| timestamp ms | 10-bit worker ID | 12-bit sequence |`. To avoid running
a coordinator, each pod derived its worker ID as `crc32(pod_name) % 1024`.
It worked for months at 20-40 pods. During a sale the Deployment scaled to
71 pods, and `orders-api-70` got worker ID 226, the same as `orders-api-4`.
Whenever both pods created an order in the same millisecond with the same
sequence number they produced the *same* order ID: one checkout failed on the
primary key, and in a downstream system without that constraint two customers'
orders were merged under one ID.

## Root cause
Hashing names into a small space is the **birthday problem**, not a
uniqueness guarantee. With 1,024 worker IDs the chance that at least two pods
collide is 53.8% at 40 pods and 99.3% at 100 pods (computed by
`chance_of_any_collision`). The Snowflake format is only unique if every live
generator holds a distinct worker ID, and nothing enforced that.

## Solution
Worker IDs are **leased** from a shared registry (in production: etcd,
ZooKeeper or a database row with a compare-and-set):

- At start-up a pod takes the lowest worker ID that is free or whose lease
  expired, for `LEASE_TTL_MS = 30 s`, and renews it while alive.
- The 1,025th concurrent pod is refused (`NoWorkerIdAvailable`) instead of
  silently sharing an ID; all 1,024 pods get distinct IDs in the tests.
- A crashed pod's ID becomes reusable after its lease expires.
- The generator checks its lease before every ID and stops
  `RENEW_SAFETY_MARGIN_MS = 5 s` before the lease ends. A pod that was paused
  (GC, VM freeze) and lost its lease cannot keep issuing IDs under a worker ID
  that another pod now owns, and its renewal fails.

## Trade-offs
- Start-up now depends on the registry. Pods already running keep issuing
  IDs until their lease runs out, so a short registry outage is tolerated;
  a long one stops ID generation.
- Safety depends on the pod's clock not running slow relative to the
  registry by more than the margin. Larger margins are safer but need more
  frequent renewals.
- Alternatives: a Kubernetes StatefulSet ordinal (unique while the set
  has fewer than 1,024 replicas, but two clusters or a manual pod break it),
  database block allocation of ID ranges, or 128-bit random IDs (UUIDv4/v7),
  which make collisions negligible at the cost of bigger, less index-friendly keys.

## Run
```bash
python -m unittest -v test_solution.py
```
