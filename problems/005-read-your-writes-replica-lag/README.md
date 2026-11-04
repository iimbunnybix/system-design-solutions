# 005 · Users saved their profile, the page reloaded with the old value, and some edits seemed to vanish

## Problem
A social app moved all reads to three PostgreSQL read replicas behind a
round-robin pool to take load off the primary. It worked on dashboards: the
primary's CPU dropped by more than half. Within a day support got a wave of
tickets. Users changed their display name, the page reloaded with the old
name, so they saved again (doubling write traffic on those endpoints). Others
saw a new comment appear, refreshed, and it was gone, then back again.

## Root cause
Replication to the replicas is **asynchronous**. Usually the lag is a few
milliseconds, but during write bursts or long queries on a replica it reaches
seconds. A round-robin read can therefore hit:

- a replica that has not yet applied **the user's own write** (no
  read-your-writes), or
- a replica **older than the one used for the previous read**, so data
  appears and then disappears (no monotonic reads).

Both are reproduced in the tests with `NaiveRouter`.

## Solution
**Session-consistent routing using log sequence numbers** (`SessionConsistentRouter`):

- Every write returns the primary's commit LSN (in PostgreSQL:
  `pg_current_wal_lsn()` after commit). The client session stores
  `min_lsn = max(min_lsn, lsn)`, for example in a signed cookie, so it works
  across app servers (tested with two routers sharing one session).
- A read goes to a replica whose applied LSN is `>= min_lsn` (in PostgreSQL:
  `pg_last_wal_replay_lsn()`, polled and cached by the router). The starting
  replica rotates so fresh replicas share the load.
- After reading, the session's `min_lsn` is raised to that replica's LSN,
  which gives **monotonic reads**.
- If no replica is fresh enough, the read goes to the **primary**.

Sessions that did not write are not affected and keep reading from replicas.
In a seeded randomized run (20 sessions, 5,000 operations, 30% writes, 4
replicas lagging randomly) no read violated either guarantee, and **93%** of
reads were still served by replicas.

## Trade-offs
- The primary absorbs reads right after writes. A write-heavy session hits the
  primary often, and a badly lagging fleet can push all recent writers onto it
  at once. Alert on replica lag and on the fallback rate. A variant waits up
  to a few milliseconds for a replica to catch up before falling back.
- This gives **per-session** consistency, not global. Another user can still
  see the old name for a moment. Cases that need linearizable reads (balances,
  uniqueness checks) must read from the primary.
- The client must carry the token. Losing it (new device, cleared cookies)
  silently downgrades to eventual consistency for that client.
- Simpler alternatives: send a user's reads to the primary for N seconds
  after they write (needs N ≥ worst-case lag, which is not bounded), or use
  synchronous replication (higher write latency, and a slow replica stalls
  commits).

## Run
```bash
python -m unittest -v test_solution.py
```
