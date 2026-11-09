# 014 · Users' saved settings flipped back to old values at random, and got much worse after a storage node came back from maintenance

## Problem
A user-settings service (notification preferences, language, privacy
toggles) stores data in a leaderless, Dynamo-style key-value store with three
replicas per key. To keep latency low it was configured to acknowledge a
write after **one** replica stored it (`W=1`) and to read from **one**
replica (`R=1`).

Users reported that settings they had just changed showed the old value on
the next page load, then the new one, then the old one again. Privacy
complaints followed when "hide my profile" appeared to switch itself off.
It got much worse after one storage node was taken out for maintenance for a
couple of hours and brought back: for a while, a third of reads went to a node
that had missed every write made during the maintenance.

## Root cause
**Read and write sets that do not overlap.** With `N=3, W=1, R=1`, a write
is acknowledged when one replica has it, and a read may go to a different
replica that has not received it yet (replication lag) or never will
(it was down when the write was sent). Nothing forces the read to see the
latest acknowledged write, and with `R=1` there is no second answer to
compare against, so stale replicas are never repaired by reads.

In the simulation (3 replicas, 20,000 operations on 20 hot keys, 30% writes,
1–20 ticks replication delay, one replica down for 6,000 ticks and another
for 500), **5.51% of reads returned stale data**. Even with no outages at
all, replication lag alone made 6.11% of reads stale. `W=1, R=2` and `W=2,
R=1` reduce the rate but do not remove it.

## Solution
`solution.py`:

1. **Quorums with `R + W > N`.** With `N=3, W=2, R=2`, every read set of 2
   replicas shares at least one replica with every acknowledged write set of
   2, so the newest acknowledged version is always among the answers. Each
   value carries a version and the read returns the highest one. Across 5
   seeds: **0 stale reads**, and with one replica down neither reads nor
   writes fail. The same holds for `N=5, W=3, R=3` with two replicas down at
   overlapping times.
2. **Blocking read repair.** When the answers disagree, the coordinator
   writes the newest version back to the stale replicas it contacted before
   answering, so a rejoined replica heals as its keys are read. This is also
   what stops the flip-flop: `R + W > N` only guarantees the latest
   *acknowledged* write. A read can return a write that is still in flight
   (on one replica, not yet acknowledged), and the next read can miss it and
   show the old value again. Copying the returned value to the `R` replicas
   read means any later read set of `R` overlaps it. The `went_back_reads`
   metric counts reads older than a value an earlier read of the same key
   returned: in the outage scenario it is **97** for `W=3, R=1` and **15**
   for `W=2, R=2` without read repair (both with 0 stale reads), and **0**
   for `W=2, R=2` with read repair across 5 seeds. Monotonic reads therefore
   need `R + W > N` *and* blocking read repair with `R >= 2` (Cassandra at
   `QUORUM` does this).
3. **Anti-entropy.** `anti_entropy` syncs all up replicas in the background
   for keys nobody reads (real systems compare Merkle trees to avoid scanning
   every key).
4. **Replicas keep the highest version**, so late or duplicated replication
   messages cannot roll a value back.

## Trade-offs
- **Latency**: each request waits for the second-fastest of three replicas
  instead of the fastest. That was the reason for `W=1, R=1`, and it is the
  price of consistent reads.
- **Availability**: with two of three replicas down, `W=2, R=2` refuses
  operations (348 reads unavailable and 158 writes failed in the test) where
  `W=1, R=1` would have served stale data. `W=3, R=1` or `W=1, R=3` are also
  strict quorums but fail as soon as *one* replica is down (1,972 failed
  writes / 4,529 unavailable reads in the outage scenario). `W=3, R=1` also
  does not fix the reported symptom: with one replica per read there is
  nothing to repair, so values still go back in time (97 went-back reads).
- `R + W > N` gives "read your latest acknowledged write", not
  linearizability: concurrent writes are resolved last-writer-wins by
  version, and a failed write can still be partly applied and seen later.
  Use conditional writes or a consensus-based store (Raft/Paxos) when you need
  compare-and-set semantics.
- Versions here come from one counter. Real systems use per-coordinator
  timestamps (clock skew can lose writes) or vector clocks/CRDTs to detect
  concurrent updates (see 006 for a CRDT cart).
- Sloppy quorums with hinted handoff keep writes available during outages
  but weaken the overlap guarantee until hints are delivered.

## Run
```bash
python -m unittest -v test_solution.py
```
