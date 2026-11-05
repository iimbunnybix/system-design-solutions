# 007 · One viral post made "like" requests time out, and the whole site slowed down with it

## Problem
A social app stores like counts in the `posts` table and runs
`UPDATE posts SET likes = likes + 1 WHERE id = ?` for every like. A creator's
post went viral and drew a sustained burst of a few thousand likes per second.
Like requests started timing out, the app's database connection pool filled
with requests waiting on that one row, and unrelated pages (feeds, profiles)
became slow because they could not get a connection. The database host itself
had plenty of CPU and I/O left.

## Root cause
**All writes contended for one row lock.** Each `UPDATE` holds the row's lock
until its transaction commits, so a single row can absorb at most
`1 / lock_hold_time` updates per second, however large the database is. With
a 1 ms hold, that ceiling is 1,000 writes/s. At 2,000 writes/s the row is at
200% utilization and the queue grows for as long as the burst lasts.

In the discrete-event model (Poisson arrivals at 2,000/s for 5 s, 1 ms lock
hold, 10,210 writes) a single row gives a **median wait of 2.6 s** and a
**p99 of 5.2 s**. Waiting writes hold connections, which is how one hot key
starved the rest of the site.

## Solution
**Sharded counter.** Replace the single value with N rows
`(post_id, shard, count)`:

- **Write:** increment one *randomly chosen* shard, which spreads the lock
  load over N independent rows.
- **Read:** `SUM(count)` over the N shards (N is small, so this is cheap, and
  the result can be cached for a second or two).
- **Sizing:** `recommended_shards(peak, hold, max_utilization=0.5)` picks the
  smallest N that keeps each row at or below 50% lock utilization, which is 4
  for this burst. Cold keys keep 1 shard; hot keys can be grown on demand,
  and existing counts are preserved. Shrinking is refused because reads would
  ignore the higher rows.

Same model, same load (`seed=1`; `test_readme_table_matches_the_model` checks
every row):

| Shards | Utilization per row | p50 wait | p99 wait |
|---|---|---|---|
| 1 | 2.0 | 2.6 s | 5.2 s |
| 2 | 1.0 | 87 ms | 167 ms |
| 4 | 0.5 | 0.05 ms | 3.2 ms |
| 16 | 0.125 | 0 | 1.0 ms |

A threaded test with 8 writers × 2,000 increments over 8 shards ends at
exactly 16,000, with every shard used.

## Trade-offs
- Reads cost N row reads instead of one, and a strongly consistent total needs
  all N in one snapshot. For a like count, a slightly stale cached sum is fine.
- Choosing N per key needs either a hot-key detector or a generous default;
  too many shards wastes rows for the millions of cold posts.
- Alternatives: buffer increments in memory or Redis (`INCR`) and flush to the
  database every few seconds (fewer DB writes, but risks losing un-flushed
  counts on a crash); or write likes to an append-only log/stream and
  aggregate asynchronously (scales furthest, counts lag).
- This only fixes *counter* hot spots. A hot row that needs a real
  read-modify-write invariant (e.g. inventory that must not go below zero)
  needs reservation or partitioned-quota designs instead.
- The model ignores commit latency variance and lock-manager overhead; real
  numbers will be worse at high utilization, which is why the sizing targets
  50%, not 100%.

## Run
```bash
python -m unittest -v test_solution.py
```
