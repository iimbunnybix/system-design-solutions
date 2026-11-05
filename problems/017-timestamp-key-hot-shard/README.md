# 017 · One database shard pegged at 100% CPU while the other seven idled, and event ingestion lagged by hours

## Problem
An activity-event store (clicks, page views, order events) runs on a
range-partitioned database in the Bigtable/HBase style: rows are sorted by row
key and each of 8 shards owns one contiguous key range. The row key was
`<epoch-ms timestamp>|<event id>` so that "events between 10:00 and 11:00"
was a single fast range scan. The table was pre-split evenly over the last 30
days of timestamps.

On launch day every new event went to the same shard. That node hit 100% CPU
and its write queue grew, ingestion lag climbed into hours, and dashboards
showed stale numbers — while the other seven shards received **zero** writes.
Adding more shards did not help: the new shard just became the new hot one.

## Root cause
The key is **monotonically increasing**. Every new event's timestamp is larger
than every existing key, so all live writes sort after the last split point
and land on the last shard. In the simulation, 4,000 events arriving over 1,000
seconds today all go to shard 7 (`[0, 0, 0, 0, 0, 0, 0, 4000]`). The store
splits hot ranges eventually, but the split just moves the hot spot to the new
"end of the table".

## Solution
Put a **stable hash bucket of the event id in front of the timestamp**:
`<bucket>|<timestamp>|<event id>`.

- `bucket_for` uses SHA-256 of the event id modulo 16 buckets. `hashlib` is used
  instead of `hash()` because Python randomises string hashes per process, and
  every writer and reader must agree on the bucket.
- Shard boundaries are placed on bucket prefixes (`bucket_split_points`), two
  buckets per shard. New events now spread evenly: the same 4,000 events land
  `[498, 496, 511, 517, 503, 469, 489, 517]`, and the busiest shard takes 12.9%
  of writes against an ideal 12.5%.
- Inside each bucket rows are still time-ordered, so a time-range read
  (`salted_time_range`) is a **scatter-gather**: one range scan per bucket,
  then a k-way `heapq.merge` back into time order. Tests check it returns
  exactly the same events, in the same order, as the original single scan.

## Trade-offs
- **Read amplification:** every time-range query becomes 16 scans. Run them in
  parallel and keep the bucket count as small as gives an even spread (a
  common rule is a small multiple of the shard count).
- **Bucket count is baked into the keys.** Changing it means rewriting data or
  reading both layouts during a migration; choose it with growth headroom.
- Point lookups need the event id *and* timestamp to build the key; a lookup
  by id alone needs a secondary index.
- It spreads load from *many* writers. A single very hot entity (one id) still
  maps to one bucket; that needs per-entity sharding such as split counters.
- Alternatives: pure hash partitioning (even writes, but no range scans at
  all), a reversed/bit-shuffled id, or time-bucketed tables with a hashed
  partition key (the DynamoDB/Cassandra pattern of `(day, bucket)` partitions).

## Run
```bash
python -m unittest -v test_solution.py
```
