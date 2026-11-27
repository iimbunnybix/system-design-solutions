# 076 · One storage node died and some keys were gone for good, although every key was "stored three times"

## Problem
A key-value store holds user session and profile data on six storage
machines in three racks (two per rack), about 40 million keys in total.
Keys are placed on a consistent-hash ring with 16 virtual nodes per
machine, and every key is written to 3 replicas. The design doc promised
that any single machine, or even a whole rack, could fail without losing
data.

Then one machine's disk controller failed. Thousands of keys immediately
started returning errors on quorum reads and writes, and some keys could not
be read at all: every one of their copies had been on that machine. Users
were logged out and some lost saved preferences. A later rack power cut in
a test environment was worse.

## Root cause
**"The next 3 vnodes clockwise" is not "3 machines".** Each machine's 16
vnodes land at random positions, so a machine's vnodes are often adjacent
on the ring. A key whose next 3 vnodes are `node-4, node-4, node-2` has
only two real copies; `node-4, node-4, node-4` has one. Racks were never
considered, so even three distinct machines could share a rack.

This is not a key-movement problem (problem 001) or a load-balance problem
(problem 034): placement is stable and even. The replica set itself is
wrong.

Measured on 20,000 keys with the six-machine ring (hashing with `hashlib`,
so the ring is the same on every run and every server):

| Original placement | Result |
|---|---|
| Keys with fewer than 3 distinct machines | **37.7%** (7,537 keys) |
| Keys with fewer than 3 distinct racks | **76.8%** (15,351 keys) |
| Worst single machine failure: keys with no copy left | **96** |
| Worst single machine failure: keys below quorum (2 of 3) | **2,629** |
| Rack `rack-a` failure: keys with no copy left | **828** |

## Solution
**Walk the ring past duplicates until there are 3 distinct machines,
preferring distinct racks** (`distinct_replicas`):

1. Start at the first vnode clockwise from the key, as before, so the
   first replica (the key's owner) does not change.
2. Skip any vnode whose machine is already chosen.
3. Rack-aware: skip machines whose rack is already used, but remember them
   in ring order.
4. If the ring runs out of new racks (fewer racks than replicas), top up
   from the remembered machines, so there are still 3 distinct machines.
5. If there are fewer machines than replicas, return every machine once
   instead of looping or repeating one.

With the fix, on the same keys: **0%** of keys have fewer than 3 distinct
machines or racks, no single machine failure loses a key or quorum
(**0** and **0**), and no rack failure does either. On a four-machine,
two-rack cluster every key gets 3 machines covering both racks.

## Trade-offs
- **Load shifts between machines.** A machine right after a run of
  another machine's vnodes picks up the replicas the skipped vnodes would
  have taken. In the model the replicas per machine ranged from 7,776 to
  11,572 before and 8,260 to 11,740 after: about as uneven, but differently.
  The spread comes from only 16 vnodes per machine; more vnodes even it out.
- **Racks must be balanced.** If one rack has far fewer machines, rack-aware
  placement forces a copy of most keys onto those machines and overloads
  them. Keep racks equal in size, or weight vnodes by rack capacity.
- **Changing placement is a migration.** Switching an existing cluster to
  this rule changes the replica set of every key that lacked 3 distinct
  racks (76.8% in the model), so their data must be copied before the old
  replicas are dropped.
- **Alternatives:** Cassandra's `NetworkTopologyStrategy` (the same walk,
  per data center and rack); CRUSH-style placement (Ceph), which picks
  replicas through a failure-domain tree directly; or a placement table
  managed by a coordinator that checks failure domains explicitly.

## Run
```bash
python -m unittest -v test_solution.py
```
