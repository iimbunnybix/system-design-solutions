# 046 · Minutes after a new API node joined, it fell over under every client's traffic while the old nodes sat idle

## Problem
A fleet of 100 API clients (gateway instances) balanced requests across 10
backends. To be "smart", each client sent every request to the backend with
the lowest load according to a shared load report published by the metrics
service and refreshed every 10 seconds.

During a traffic increase the team added an 11th backend. It reported zero
load, so all 100 clients sent all of their requests to it. Within seconds it
held every in-flight request in the system, its latency exploded and its
health checks failed, while the ten old backends drained to nothing. At the
next report refresh the whole herd jumped to the next "emptiest" node, and
then the one after that.

In the simulation (100 clients, 1 request per client per tick, each request
lasting 5 ticks, report refreshed every 10 ticks, 60 ticks), the new node
peaks at **500 in-flight requests** (all of them) against a fair share of 45
and a safe limit of 80. The herd then alternates between `backend-0` and
`backend-1` at each refresh; backends spend **80** backend-ticks above the
safe limit.

## Root cause
Every client made the **same deterministic choice from the same stale data**.
"Least loaded" is only a good rule when the load information is fresh; with a
report that is 10 ticks old, the choice does not see the requests that every
other client has just sent to the same node. The new node's zero in the
report acted as a magnet until the next refresh. This is the classic herd
effect described in Mitzenmacher's analysis of stale load information.

## Solution
`TwoChoicesClient` uses the "power of two random choices":

1. For each request, sample **two different backends at random**.
2. Send it to the one with fewer requests **in flight from this client**,
   counted locally, so the information is never stale.

Random sampling means different clients pick different pairs, so they do not
move as one herd; comparing two choices instead of picking one at random
avoids most of the imbalance of pure random placement.

Results (same traffic, seeds 0-4):

| Balancer | Peak in-flight on any backend | Peak on new node | Overloaded backend-ticks |
|---|---|---|---|
| Least loaded from shared report | 500 | 500 | 80 |
| Two random choices, local counts | 65-70 | 53-57 | **0** |

The peaks above the fair share of 45 come mostly from the warm backends'
starting load of 50 plus random imbalance, all well under the limit of 80.

## Trade-offs
- **Local counts ignore other clients' load.** If one backend is slow for
  everyone, a client only learns it as its own requests pile up there. With
  many clients that is usually enough; otherwise combine with latency-aware
  scoring (e.g. peak EWMA as in Finagle/Linkerd) or server-reported load.
- **New nodes do not ramp up gently.** They get their share immediately; if
  caches or JIT need warming, add slow start (devops-cloud-solutions problem 033).
- **Not perfectly even.** Two choices keep the maximum close to the average
  but still above it; more choices help little and cost more sampling.
- Keeping the shared report but **adding randomness** (pick randomly among the
  few least loaded) also breaks the herd, but the stale data still biases
  every client in the same direction.

## Run
```bash
python -m unittest -v test_solution.py
```
