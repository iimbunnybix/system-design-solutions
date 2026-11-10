# 044 · After the rebalancer moved a busy shard to a new server, users found profile edits from the last few minutes undone and some new accounts gone

## Problem
A user-profile store is split into shards, each owned by one server. When a
server runs hot, a rebalancer moves one of its shards to a fresh server: copy
every record across, then flip the routing table so reads and writes go to
the new owner. Moves ran during the day, because shards only get hot under
load.

After each move, support tickets followed: "I changed my address and it
changed back", "I signed up this morning and now my login doesn't exist".
The writes had all returned success. They were not on the new server, and the
old server, which did have them, was wiped by the cleanup job a day later.

In the simulation (10,000 keys copied in batches of 500, 20 client writes
per batch), the copy-then-switch move loses **218 of the 394** keys written
during the copy, including **38** brand-new keys.

## Root cause
The copy is not instantaneous, and the shard keeps accepting writes while it
runs. A write to a key that was already copied, or to a key created after the
copy took its key list, lands only on the old server. Switching routing at
the end makes the new server authoritative while it is missing exactly those
writes. Nothing tracked what changed **during** the copy.

## Solution
`move_with_catch_up` uses the snapshot-plus-change-log pattern (the same idea
as MySQL/Postgres replica seeding, Vitess `MoveTables` and online schema
change tools):

1. **Record the log position first.** Every write on the source is appended
   to a change log. The move remembers the log position *before* copying, so
   it knows that everything after it may have been missed.
2. **Bulk copy** in batches while writes continue.
3. **Catch up.** Replay logged changes after that position to the target, in
   log order, `REPLAY_BATCH_ENTRIES` (100) per step while clients keep writing
   20 per step, until at most `CUTOVER_MAX_LAG` (50) changes remain. Replaying
   in order makes the final value correct even for keys the copy had already
   picked up at a newer value.
4. **Short freeze and switch.** Refuse writes (`ShardFrozen`, client retries),
   replay the last few changes, switch routing, unfreeze.
5. **Give up safely.** If writes keep outrunning the replay
   (`MAX_CATCH_UP_STEPS`), raise `CatchUpTooSlow` without freezing or
   switching, and the old server stays the owner.

Results from the tests (seed 3):

| Move | Acknowledged writes lost | Writes refused |
|---|---|---|
| Copy, then switch | 218 of 394 keys | 0 |
| Snapshot + change-log catch-up | **0** | 20 (one step during the freeze) |

## Trade-offs
- **A brief write outage is still needed** for the final cutover. Clients must
  retry `ShardFrozen`; keep `CUTOVER_MAX_LAG` small so the freeze is short.
  Fully online cutover needs the router and both servers to agree on a
  per-key or per-version handover, which is much harder.
- **The change log must be retained** from the start of the copy until the
  cutover; if it is truncated (log retention, binlog purge) the move must
  restart.
- **Deletes need log entries too** (tombstones), otherwise a key deleted
  during the copy reappears on the target; this model has no deletes.
- **Dual writes instead of a log** (write to both servers during the move)
  look simpler but fail on partial failures and reorderings between the two
  writes; a single ordered log avoids that.
- **Readers** must also be cut over at the switch; reading from the target
  before cutover sees incomplete data.
- After the switch, clients with an old routing table still write to the old
  server; reject those with a "moved" error (problem 026).

## Run
```bash
python -m unittest -v test_solution.py
```
