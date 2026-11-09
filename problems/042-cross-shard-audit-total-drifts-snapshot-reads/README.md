# 042 · The hourly ledger audit kept reporting thousands of dollars created or lost, but every account balanced when checked by hand

## Problem
A wallet service stores user balances in a sharded database: 4 shards,
accounts assigned to shards by id, millions of transfers a day, most of them
between accounts on different shards. Cross-shard transfers use two-phase
commit, so both sides of a transfer commit together and the total amount of
money in the system never changes.

An hourly audit job sums every balance and compares it with the expected
total. After the service grew, the audit started failing almost every hour,
sometimes "$1,377 missing", sometimes "$6,000 too much". Each alarm paged
the finance on-call, who froze payouts while engineers checked accounts one
by one and found nothing wrong. After a few weeks the team started ignoring
the alarm, which defeats the reason the audit exists.

## Root cause
**The audit read each shard at a different moment.** It summed shard 0's
latest balances, then shard 1's, and so on, while transfers kept
committing. A transfer from an account on shard 1 to an account on shard 0
that committed after shard 0 was read but before shard 1 was read appears
only as a debit: the money looks lost. The opposite order makes money
appear. Two-phase commit made each transfer atomic, but the reader did not
read all shards at one point in time, so atomic writes were still seen half
done.

In the simulation (4 shards × 250 accounts × 1,000 = 1,000,000 total, 50
transfers committing while each shard is read, 200 audits, seed 0):
**200 of 200 audits were wrong**, with a median error of 1,377 and a worst
error of 8,687. Seeds 1–4 also gave 200/200 wrong audits.

## Solution
`solution.py` makes the audit a **snapshot read**:

1. Each shard keeps versions of every balance, tagged with the commit
   timestamp (multi-version concurrency control). Both sides of a transfer
   get the same commit timestamp.
2. The audit picks one snapshot timestamp when it starts and reads every
   shard "as of" that timestamp: the newest version with
   `commit_ts <= snapshot_ts`.
3. A transfer committed at or before the snapshot is fully counted; one
   committed after it is fully ignored, however long the scan takes.

Result for the same workload: **0 wrong audits** out of 200 for each of seeds
0–4. The live system keeps taking writes; nothing is locked.

## Trade-offs
- **Old versions must be kept** for as long as the longest snapshot read
  runs, then garbage-collected (this code never discards them). Long audits
  cost storage; PostgreSQL's VACUUM and CockroachDB's GC TTL are the real
  versions of this.
- **Commit timestamps must be comparable across shards.** Here one logical
  clock gives them out. Real systems use a timestamp oracle (TiDB, Percolator),
  hybrid logical clocks (CockroachDB, see 022) or bounded-uncertainty clocks
  (Spanner's TrueTime). A snapshot read must also wait for transactions that
  are prepared but not yet committed with a timestamp at or below the
  snapshot, or it may miss them; the simulation commits instantly and skips
  this.
- Alternatives: stop writes during the audit (simple, but an outage), read
  a database-wide consistent backup or replica snapshot, or audit with
  per-transfer double-entry checks (every debit has a matching credit) that
  do not need a global sum.

## Run
```bash
python -m unittest -v test_solution.py
```
