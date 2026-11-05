# 033 · Messages users had deleted reappeared in their chats after a database node came back from a three-day repair

## Problem
A chat app stores messages in an eventually consistent key-value store
(Cassandra-style, replication factor 3). Writes and deletes go to every
replica that is up; a nightly anti-entropy job and read-repair copy whatever
a replica missed. To cut the cost of scanning deleted rows, an engineer had
lowered the tombstone grace period from the default 10 days to one hour.

Then one node in a replica set had a failed disk and was out for three days.
When it came back, users started reporting that messages they had deleted,
some of them deleted for privacy reasons, were back in their chats, on every
device. Support could not delete them for good: a few were deleted again and
stayed deleted, but the backlog was thousands of conversations. Legal flagged
it because account-erasure requests had also been undone.

## Root cause
**A delete is only a tombstone, and the tombstone was purged before every
replica had seen it.** While node c was down, deletes reached the other two
replicas as tombstones. One hour later compaction purged those tombstones.
When c came back it still had the old messages, and repair found a value on
c and nothing at all on a and b, so it "repaired" a and b by copying the
value back. The delete was gone from the system; the data was not.

The original repair rule made it worse: it treated a tombstone like "this
replica has no data", so a live value won even while the tombstone still
existed.

`simulate()` (1,000 messages, 200 deleted one hour after c goes offline,
compaction every hour, then c rejoins and repair runs):

| Store | c offline 3 days | c offline 12 days |
|---|---|---|
| Original (1 h grace, live value wins) | 200 resurrected | 200 |
| 1 h grace, newest version wins | 200 | 200 |
| 10-day grace, live value wins | 200 | 200 |
| 10-day grace, newest version wins, no rejoin check | 0 | 200 |
| **Fix: all three, plus full resync after a long absence** | **0** | **0** |

Each part alone is not enough; that is why all three are in the fix.

Related problems: 015 (Merkle-tree repair; its trade-offs note that deletes
need tombstones) and 006 (CRDT cart, where tombstones are part of the merge).
Here the tombstones exist but are thrown away too early.

## Solution
`safe_store()` in `solution.py`:

1. **Keep tombstones for `gc_grace` = 10 days**, longer than the longest
   downtime you allow and longer than the interval between full repairs.
   A tombstone written while a replica is away then outlives that absence.
2. **Refuse a rejoin after more than `gc_grace` of downtime.**
   `rejoin()` raises `RejoinRefused`; the operator (or automation) wipes the
   node and runs `full_resync()`, which copies the merged data of the healthy
   peers, tombstones included. Downtime of exactly `gc_grace` is still safe
   because purging needs a tombstone *older* than `gc_grace` (tested).
3. **Merge by version** (`newest_wins`): the higher version wins whether it
   is a value or a tombstone, and a tie goes to the tombstone. A deliberate
   write after the delete has a higher version, so re-creating a key still
   works (tested).

After repair the returning replica holds the same tombstones, with their
original write time, so all three purge them together once `gc_grace` has
passed (tested).

## Trade-offs
- **Tombstones cost space and read time for `gc_grace`.** Workloads that
  delete a lot (queues, TTL-heavy tables) scan many tombstones; that cost is
  why the grace period was lowered in the first place. Better: model such
  data so whole partitions expire together, or use time-window compaction.
- **`gc_grace` is a promise you must keep operationally:** a full repair has
  to finish on every node within each `gc_grace` window, and a node down for
  longer must be rebuilt, not rejoined. A rebuild streams the whole data set
  and takes time and bandwidth.
- **Versions here come from one counter.** Real stores use client or
  coordinator timestamps; with clock skew a delete can carry an older
  timestamp than the write it meant to remove (see problem 022 for hybrid
  logical clocks).
- **Alternatives:** stores that track which replicas have acknowledged a
  tombstone can purge it as soon as all have (no fixed grace), at the cost of
  more coordination; a CRDT with causal metadata (problem 006) avoids
  timestamps but has its own metadata growth.
- The model has no hinted handoff. Hints shrink the window but are dropped
  after a few hours, so they do not replace the grace period.

## Run
```bash
python -m unittest -v test_solution.py
```
