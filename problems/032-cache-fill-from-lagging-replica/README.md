# 032 · Merchants changed a price, and shoppers kept seeing the old one for exactly 10 minutes even though the cache had been cleared

## Problem
A marketplace's product service caches product rows in Redis with a 10-minute
TTL (cache-aside). On a cache miss it reads from a PostgreSQL read replica,
because the primary is reserved for writes and checkout. When a merchant
changes a price, the service updates the primary and then deletes the cache
key, exactly as the cache-aside pattern says.

After a pricing tool let merchants cut prices in bulk for a promotion,
support got a flood of tickets: the product page showed the old price, the
cart (which reads the primary) showed the new one, and shoppers abandoned
carts or asked for refunds of the difference. Engineers checked the cache
invalidation logs and every key had been deleted on time. The stale price
always lasted almost exactly 10 minutes and only affected popular products.

## Root cause
**The cache was refilled from a replica that had not applied the write yet.**
Deleting the key is correct, but it creates a miss, and on a product read
~100 times per second the next request arrives within milliseconds. That
request reads the replica, which is still a few hundred milliseconds behind
(asynchronous replication), gets the old price, and writes it back into the
cache with a fresh 10-minute TTL. The replica catches up a moment later, but
nothing invalidates the cache again, so the old price is served for the
whole TTL.

The simulation (`NaiveProductService`, one product read every 10 ms, 250 ms
replication lag, price changed at t = 1 s) shows **60,000 reads (10 minutes)**
with the old price after the update; the last one is at 600.99 s, when the
TTL finally expires. Even a 5 ms lag gives the same 60,000: any miss inside
the lag window is enough. Unpopular products were unaffected because their
next read usually came after the replica caught up.

Related but different problems: problem 005 is about a user reading their
own write from a lagging replica (that read is stale once; here the stale
value is stored and served to everyone for the TTL), and backend-api problem
011 is a race where a slow request writes an old value after the delete
(here nothing is slow; the fill source is behind).

## Solution
**Versioned fills** (`VersionedProductService` in `solution.py`):

1. **Every write returns its commit LSN** (in PostgreSQL,
   `pg_current_wal_lsn()` after commit).
2. **Invalidate with a marker, not a delete.** The cache entry is replaced by
   "invalidated at LSN n" with a 30-second TTL, longer than the worst
   replication lag seen.
3. **A fill must be at least that new.** On a miss that finds a marker, the
   service reads the replica and checks its applied LSN
   (`pg_last_wal_replay_lsn()`). If the replica has applied LSN n, it fills
   from the replica; otherwise it reads the primary. Misses with no marker
   (ordinary TTL expiry) always go to the replica.
4. **Version-checked SET.** Each cached value carries the LSN it is current
   as of, and the cache refuses to overwrite a higher version (a small Lua
   script in Redis). This also stops the slow-request race from backend-api
   problem 011: a late fill with an old version loses to the marker (tested).

Same simulation:

| Design | Reads with the old price after the update | Primary reads |
|---|---|---|
| Delete + fill from replica | 60,000 (10 minutes) | 0 |
| Marker + versioned fill | 0 | 1 |

Only the first miss inside the lag window goes to the primary; that fill is
cached with the new version, and every later read is a cache hit.

## Trade-offs
- **The marker must outlive the worst replication lag.** If the replica is
  more than 30 s behind and nobody reads the key while the marker lives, the
  next miss fills from the replica again (a test pins this). Alert on replica
  lag and either lengthen the marker TTL or take a lagging replica out of the
  read pool.
- **Primary load during bursts of updates.** A bulk price change sends one
  primary read per changed hot product. Many concurrent misses on the same
  key should be collapsed with single-flight (problem 003) so it stays one.
- **Simpler alternatives:** always fill from the primary after a write
  (costs primary reads on every miss), or delete the key a second time after
  the expected lag ("delayed double delete", which shrinks the window but
  depends on guessing the lag). Write-through (setting the new value in the
  cache from the writer) avoids the miss entirely, but needs the same version
  check so two writers cannot leave the older value behind.
- Redis and the database are still not updated atomically. If the service
  crashes between the commit and setting the marker, the old value lives
  until its TTL. Publishing invalidations from the database's change stream
  (CDC) closes that gap.

## Run
```bash
python -m unittest -v test_solution.py
```
