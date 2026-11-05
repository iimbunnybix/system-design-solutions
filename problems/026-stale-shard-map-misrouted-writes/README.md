# 026 · After a shard split, some users' profile changes "saved" successfully and then reverted

## Problem
A social app stores user profiles in a range-sharded database: each shard
owns a range of user ids, and every API server caches the shard map and
routes requests itself. Shard `s1` was running hot, so the team split it:
the upper half of its id range was copied to a new shard `s3`, and the shard
map in the config service was updated. Over the next hour, support received
reports that edits to bios and privacy settings showed "Saved" and then
reverted on refresh. All affected users had ids in the moved range. Some
privacy changes ("make my account private") were among the lost writes.

## Root cause
API servers refresh their cached map on a timer, so for a while some of them
still had the **old map** and sent writes for moved users to `s1`. `s1` had
no idea which keys it was supposed to own: it accepted every write. Those
writes went into `s1`'s leftover copy of the data, while reads from servers
with the new map went to `s3`, which still had the old value. The test
reproduces it: a stale client writes `kate = v2`, the write "succeeds", and
a fresh client still reads `v1`; the `v2` sits orphaned on `s1`.

The client's cached map was treated as the truth, but only the shard knows
for certain what it currently owns.

## Solution
- **Shards check ownership.** Each shard holds the shard map version that
  assigned its ranges, and every `get`/`put` for a key outside them raises
  `WrongShard` with the current map version, instead of serving it.
- **Clients refresh on rejection.** `Client._with_routing` routes using its
  cached map; on `WrongShard` it reloads the map from the config service
  and retries, at most 3 attempts. A stale client fixes itself on the first
  misrouted request (one refresh, tested) and keys that did not move need no
  refresh at all.
- **Publish order matters.** During the split, the shards learn the new map
  before the config service publishes it, so no shard ever accepts a key it
  has already given away.

This is the same idea as Redis Cluster's `MOVED` redirect, MongoDB's
shard versioning (stale config errors), and Bigtable/HBase clients
re-looking up a region when a server says it does not host it.

## Trade-offs
- The copy and the map switch are shown as one atomic step. In a real
  migration, writes that arrive at `s1` during the copy must be blocked
  briefly or forwarded to `s3`, otherwise they are lost in the gap.
- Every request pays for an ownership check; it is a local lookup in a
  sorted list (`bisect`), so it is cheap compared with a network hop.
- If the config service is unavailable, clients with stale maps keep getting
  rejected; they need a fallback (ask the rejecting shard for the new owner)
  or retries with backoff.
- Consistent hashing reduces how many keys move during a resize but does not
  remove the need for this check; any cached routing table can be stale.

## Run
```bash
python -m unittest -v test_solution.py
```
