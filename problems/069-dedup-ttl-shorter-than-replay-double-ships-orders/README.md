# 069 · After a consumer restart, 721 orders from the morning were shipped a second time even though the consumer "deduplicated" messages

## Problem
An online shop's fulfilment service reads "order paid" messages from a
partitioned log (Kafka-style, at-least-once delivery) and tells the warehouse
to ship each order. Because a message can be delivered more than once, the
consumer kept every processed message id in a key-value store with a
**one-hour TTL** and skipped ids it had seen.

One morning the consumer's offset commits started failing (an expired
credential for the commit call, logged at DEBUG). Processing carried on
normally for three hours, about one order every 10 seconds, 1,080 orders in
total. Then the pod was rescheduled. On restart the consumer resumed from the
last committed offset, the start of the three hours, and the log replayed
every message. The dedup store still knew only the last hour of ids.

Result in the model (`test_solution.py`): **721 of 1,080 orders (67%) were
shipped twice**. The warehouse sent duplicate parcels, and support spent a
week arranging returns.

## Root cause
**The dedup window was shorter than the redelivery window.** A TTL-based id
store only catches duplicates that arrive within the TTL. The log, however,
can redeliver anything after the last committed offset, and how old that is
has no upper bound: a stuck commit, a consumer-group reset, or an operator
replaying a topic all redeliver messages hours or days old. Every message
processed more than one hour before the replay (offsets 0 to 720, processed
at or before 2 h into the 3 h run) had been forgotten.

## Solution
`OffsetMarkConsumer` uses the log's own ordering instead of a time window.
Within a partition, messages are replayed in offset order, so "already
processed" is exactly "offset <= the highest offset processed so far". The
consumer stores that **mark per partition** together with the shipment (in a
real system, in the same database transaction as the shipment row), and skips
any message at or below it, however old.

- The state is one number per partition, not one entry per message, so there
  is nothing to expire and no TTL to tune.
- Because the mark and the effect are written together, a crash between
  "ship" and "remember" cannot happen.

With the same replay, the fixed consumer ships each of the 1,080 orders
exactly once.

## Trade-offs
- **Needs per-partition ordering.** The mark works because offsets in a
  partition only grow. A queue without ordering (SQS standard queues, a
  thread pool processing out of order) needs per-message dedup instead; then
  the dedup retention must be longer than the longest possible redelivery,
  for example the topic's retention period, and messages older than the
  window should be rejected and alerted on rather than processed.
- **One processing order per partition.** Processing a partition in parallel
  breaks the "offset <= mark" rule; parallelise across partitions instead.
- **The mark must live with the effect.** If shipments are made by an external
  API instead of a local write, the mark cannot share its transaction; then
  pass an idempotency key (the order id) to that API as well.
- **Silent commit failures are their own bug.** Alert on consumer-group lag
  and on commit errors, so a replay of hours is noticed before a restart.

## Run
```bash
python -m unittest -v test_solution.py
```
