# 039 · Customers saw "payment received" on orders that had already shipped, and warehouse dashboards showed paid orders stuck for days

## Problem
An e-commerce order service publishes a state-change event for every order:
`created`, then `paid`, then `shipped`. The events go to a queue topic with
4 partitions, and a consumer group runs one consumer per partition in
parallel; each consumer writes the new status into the order database read
by the customer app and the warehouse dashboard. To spread load evenly, the
producer picked the partition **round-robin**.

Support started getting tickets from customers whose order page said
"payment received" although the parcel was already at their door. The
warehouse "paid, not yet shipped" dashboard filled with orders that had
shipped, and staff opened duplicate pick tickets for some of them.

In the simulation (2,000 orders over 60 s, each order's events 1-30 ms
apart, 4 partitions, consumers taking 1-12 ms per event, seed 7),
round-robin applied **193 events after a later event of the same order**
and left **102 orders** (5.1%) in the wrong final state (`paid` or
`created` instead of `shipped`).

## Root cause
**Ordering is only guaranteed within a partition.** Round-robin put the
three events of one order on three different partitions. Each partition is
FIFO, but the partitions are consumed independently: when the "paid"
partition had a small backlog or a slow event in front, the "shipped" event
on another partition was applied first, and then the late "paid" overwrote
it. The consumers did nothing wrong individually; the producer threw away
the per-order ordering when it chose the partition.

## Solution
**Partition by entity key** (`stable_partition` and `keyed_run` in
`solution.py`):

1. The producer computes the partition from the order id:
   `blake2b(order_id) mod partitions`. Every event of one order lands on the
   same partition, which one consumer reads in order, so per-key order is
   preserved end to end.
2. The hash must be **stable across processes**. Python's `hash()` of a
   string is randomised per process (`PYTHONHASHSEED`); two producer
   instances would route the same order to different partitions and the bug
   would come back. The tests show `hash("order-42")` changing between
   two interpreters while `stable_partition` does not.
3. Different orders still hash to different partitions, so all 4 consumers
   keep working in parallel (queue sizes 1,482 / 1,494 / 1,497 / 1,527
   events for 6,000 events).

Result on the same workload: **0** out-of-order events and **0** orders in
the wrong state; for 5 different seeds no event was ever applied before an
earlier event of the same order. The worst publish-to-apply lag went from
20 ms to 47 ms because the load is slightly less even.

## Trade-offs
- **Hot keys.** All events of one key go to one consumer, so a key with a
  lot of traffic overloads its partition while the others idle. In the
  hot-key simulation (6,000 events in 20 s, 40% for one key) one partition
  gets **3,372** events against 861-897 on the others, and the worst lag
  grows to **2,077 ms**; round-robin spreads the same traffic as 1,500 per
  partition with 12 ms worst lag. Options: pick a finer key when only
  per-entity order matters (order id, not seller id), split a known hot key
  into sub-keys when its events are independent, or give hot entities their
  own topic.
- Ordering is per key, not global: events of different orders can still be
  applied in any order. That is usually what the business needs.
- Changing the partition count remaps keys (`mod N` changes). In-flight
  events of one order can then be split across old and new partitions;
  drain the topic or pause producers when resizing, or create partitions
  generously up front.
- One slow or poison event blocks every later event of the keys on that
  partition (head-of-line blocking). Bounded retries plus a dead-letter
  queue keep the partition moving, at the cost of breaking order for that
  key.
- Keyed partitions keep order in the queue, but retries in the consumer or
  producer can still reorder. Producers need idempotent, in-order sends
  (Kafka `enable.idempotence`); as a second defence consumers can reject a
  status change that goes backwards using a per-order version number.
- Kafka's default partitioner does this when a message has a key
  (murmur2 of the key); SQS FIFO uses a message group id for the same
  purpose.

## Run
```bash
python -m unittest -v test_solution.py
```
