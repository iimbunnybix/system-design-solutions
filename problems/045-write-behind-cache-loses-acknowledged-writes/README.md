# 045 · After the cart cache restarted, shoppers' recently added items vanished even though every "add to cart" had returned success

## Problem
A shopping-cart service put a write-behind cache in front of its database to
absorb peak traffic. Each cart write was stored in the cache's memory,
acknowledged to the app immediately, and flushed to the database in one batch
every 5 seconds. Writes were fast and the database load dropped sharply.

Then the cache process was OOM-killed during a sale. Every cart change made
since the previous flush was gone: shoppers saw items disappear, some checked
out with stale carts, and support could not restore anything because the
database never received those writes.

In the simulation (one write every 10 ms, a flush every 5 s, crash after 730
writes = 7.3 s), the naive cache acknowledges writes to 657 carts and loses
**207** of them: every cart touched after the 5.0 s flush.

## Root cause
The acknowledgement promised durability that did not exist. A write was only
in process memory until the next flush, so the "durability window" was up to
the whole flush interval. A crash, kill or deploy inside that window drops
acknowledged data, and nothing on restart can know it existed.

## Solution
`JournaledWriteBehindCache` keeps the batching but adds a write-ahead journal:

1. **Journal before ack.** `put` appends the write to an append-only journal
   file and calls `fsync` before acknowledging. `FakeDisk` models the page
   cache: an append without `fsync` is lost on a crash, so skipping `fsync`
   would keep the bug.
2. **Flush, then truncate.** Every 5 s dirty keys go to the database in one
   batch, and only after that succeeds is the journal truncated. Truncating
   first would reopen the window.
3. **Replay on restart.** `recover` applies the journal in order (last write
   to a key wins) and truncates it.

Results from the tests:

| Cache | Carts lost after crash | Journal records replayed | fsyncs |
|---|---|---|---|
| Naive write-behind | 207 of 657 | – | 0 |
| Journaled write-behind | **0** | 230 | 730 (one per write) |

## Trade-offs
- **One fsync per write** puts a synchronous disk flush on the write path,
  which is slow on network disks. Group commit (batch several writes per
  fsync, problem 036) keeps most of the throughput while acknowledging only
  after the shared fsync.
- **The journal lives on the cache host.** If the disk itself is lost, so are
  the journalled writes; replicate the journal (or use a replicated log such as
  Kafka) when the host is not trusted to come back.
- **Replay must be idempotent.** A crash after the database write but before
  the truncate replays the batch again; plain "set key to value" is safe,
  increments would not be.
- **Write-through** (write to the database before acking) avoids the journal
  but gives up the batching that motivated the cache.
- Readers that bypass the cache still see the database up to 5 s behind.

## Run
```bash
python -m unittest -v test_solution.py
```
