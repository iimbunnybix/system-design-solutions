# 024 · Search pages took half a second at p99 even though every shard answered in 10 ms on average

## Problem
A product search page fans each query out to 20 index shards and renders
when the last one answers. Every shard has several replicas, and the client
picks one at random. Shard dashboards looked healthy: median and p95 shard
latency around 10-12 ms. But page-level p99 was close to 500 ms, and the
slow pages were the ones users complained about. Adding more shard replicas
did not help, because the slowness came from short pauses (garbage
collection, background compaction) that hit any replica, about 2% of calls.

## Root cause
A page is as slow as its **slowest** shard call. If each call is slow with
probability 2%, a page with 20 calls avoids every slow call with probability
0.98^20 ≈ 0.67, so about **one page in three** waits for a pause. In the
seeded simulation (`NaiveCaller`, 5,000 pages) 33% of pages included a slow
call, page p50 was 11.9 ms and page p99 was **494 ms**. The rare per-call tail
became the common page tail.

## Solution
`HedgedCaller` sends a **hedged (backup) request** to another replica when the
first call has not answered within the hedge delay, and uses whichever reply
arrives first (the approach described in Dean and Barroso, "The Tail at
Scale"):

- **Hedge delay = recent p95** of first-call latency (`HedgeDelay`, last
  1,000 calls, recomputed every 50 calls and cached). Only the slowest ~5% of
  calls get a backup, so the extra load is small. Before 100 samples exist, a
  fixed 50 ms delay is used.
- **Hedge budget** (`HedgeBudget`): a token bucket that earns 0.10 tokens per
  first call (burst of 10). This is twice the ~5% that steady state needs, but
  it stops hedging from doubling traffic when the whole fleet slows down.

Results from the same seeded simulation (5,000 pages × 20 shards):

| Client | Page p50 | Page p99 | Calls per shard request |
|---|---|---|---|
| Naive | 11.9 ms | 494 ms | 1.00 |
| Hedged, budget 10% | 11.9 ms | 23.9 ms | 1.05 |
| Hedged, budget 5% | 11.9 ms | 374 ms | 1.05 |

A budget equal to the steady-state hedge rate (5%) runs dry and most of the
benefit is lost, so the budget needs headroom. In a sudden fleet-wide
slowdown (every call slow, after warming up on healthy traffic), the first
500 shard calls sent 170 hedges without a budget and 58 with it.

## Trade-offs
- **Only for idempotent reads.** A hedged write can be applied twice unless
  the server deduplicates it (see backend-api-solutions problem 001, idempotency keys).
- **Wasted work.** In the simulation 5.1% of shard calls got a hedge, but
  3.1% of calls were hedged although they were only slightly above p95 and
  never truly slow, so most hedges were wasted. A higher percentile
  (p98-p99) wastes less but helps fewer requests. *Tied requests* (send both
  at once and let the server cancel the copy that has not started) remove
  most of this waste but need server support.
- **Cancellation.** The losing call keeps running unless it is cancelled;
  real clients should cancel it (HTTP/2 RST_STREAM, gRPC cancel).
- **Hedges do not fix overload.** If the backend is slow because it is
  saturated, hedging adds load; the budget limits that, and the adaptive
  delay rises as soon as the window fills with slow calls.
- **Alternatives:** remove the pauses themselves (GC tuning, compaction
  throttling), route away from slow replicas (latency-aware load balancing),
  or reduce fan-out.

## Run
```bash
python -m unittest -v test_solution.py
```
