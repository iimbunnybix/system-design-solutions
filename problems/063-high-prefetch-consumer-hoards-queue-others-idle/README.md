# 063 · The image queue autoscaled from 1 to 10 workers, but the backlog took 9½ minutes to drain and nine workers sat idle

## Problem
A photo-sharing app resizes uploads through a RabbitMQ-style work queue.
Most jobs are thumbnails that take about 1 second; about one in five is a
large RAW or panorama file that takes 20 seconds. Each consumer processes one
job at a time and was configured with a prefetch of **100** unacknowledged
messages, a value copied from a tuning guide for small, uniform messages.

One evening a bulk import put a burst of **300 jobs** (1,554 seconds of
work, 66 of them slow) on the queue while one consumer was running. The
autoscaler saw the queue depth and added **nine consumers after 30 seconds**.
With perfect sharing the backlog should have been gone after about 182
seconds. Instead it took **578 seconds**. The queue's "ready" count dropped to zero
the moment the new consumers connected, yet users kept waiting minutes for
their photos to appear. The nine new
consumers were idle for a combined **3,956 consumer-seconds** while jobs
waited, and the average job finished **169.4 seconds** after upload.

## Root cause
Prefetch hands messages to a consumer before it can work on them:

- When the burst arrived, the broker immediately pushed 100 messages into
  the only connected consumer's local buffer. From then on those jobs were
  invisible to the shared queue, and no other consumer could take them.
- Each job it acknowledged freed one credit, so it kept refilling to 100.
  When the nine new consumers connected at 30 s, the 197 jobs left in the
  queue were split about 22 each; they finished and went idle. The first
  consumer still held 100 jobs, 23 of them 20-second files, and worked
  through them alone: it was busy for all 578 seconds.
- Uneven job durations make it worse: even between consumers that joined at
  the same moment, an equal *number* of messages is not an equal amount of
  *work*, so whoever got the slow files finishes last.
- The queue-depth metric looked healthy because hoarded messages count as
  "unacked", not "ready", which hides the stranded work from dashboards
  and autoscalers.

## Solution
`dispatch_with_credit` gives each consumer credit for at most `prefetch`
unacked messages and hands out messages round-robin only to consumers with
credit left. `bounded_run` sets prefetch to **1**, the number of jobs a
consumer can work on at once, so every job stays in the shared queue until
some consumer is actually free. Whoever finishes first takes the next job,
which balances work rather than message counts. The original setting is
kept as `naive_dispatch` / `naive_run`.

Same 300 jobs, same scale-out at 30 s (deterministic one-second fake clock,
seed 7):

| Prefetch | Backlog drained | Avg. job done after | Idle consumer-seconds while jobs waited |
|---|---|---|---|
| 100 (original) | 578 s | 169.4 s | 3,956 |
| 10 | 284 s | 108.7 s | 845 |
| 1 (fix) | 192 s | 108.6 s | 0 |

The ideal for this burst is 182.4 s, so the fix is within 6%. A prefetch of
10 helps but still strands work behind slow jobs.

## Trade-offs
- Prefetch 1 means one broker round trip between jobs. That is nothing for
  1–20 second image jobs, but for millisecond-sized messages it caps
  throughput; there, use a small prefetch (roughly consumer concurrency ×
  2) rather than unlimited, or batch messages.
- If a consumer processes N jobs in parallel, set prefetch to N, not 1.
- Monitor "unacked" as well as "ready" messages and autoscale on both, so
  hoarded work is not hidden.
- This is different from the credit-based flow control problem (030), where
  credit protects a single slow consumer from being flooded and running out
  of memory (backpressure to the producer). Here memory is fine; the issue
  is *fairness between competing consumers* of one queue. It is also not
  the partition-count problem (047): with a shared queue any consumer can
  take any message, so scaling out works as soon as messages are not locked
  into one consumer's buffer.

## Run
```bash
python -m unittest -v test_solution.py
```
