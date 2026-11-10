# 043 · After a rack switch failed, customers on one rack kept seeing their old balance for 20 minutes even though every write was committed

## Problem
A five-node metadata store (nodes `a`–`e`, one leader) holds wallet balances.
Writes already go through a majority (problem 023), so a leader cut off from
the cluster can no longer acknowledge writes. Reads were treated as "safe"
and answered by the leader straight from its local copy, because that keeps
the read path at one hop.

A top-of-rack switch failed and split the cluster into `a, b` (with the old
leader `a`) and `c, d, e`. Within a second the majority side elected `c`, and
customers routed to `c` topped up their wallets. Customers whose sessions
were still pinned to `a` kept reading the old balance for the 20 minutes the
partition lasted: payments that had just succeeded looked like they had not,
and support received a wave of "my top-up vanished" tickets and duplicate
top-ups.

## Root cause
A leader **cannot know locally that it is still the leader**. `a` was never
told it had been replaced; nothing in the read path checked. A committed
write on the new leader is only guaranteed to be visible to a read that is
served by someone who has proved, recently, that a majority still follows
them. `naive_read` reproduces it: after failover `c` returns `200` and `a`
returns `100`, and `a` still returns `100` twenty minutes later.

## Solution
`linearizable_read` combines the two standard techniques (Raft's lease reads
and read index, also used by etcd and TiKV):

1. **Leader lease.** Each heartbeat that a majority acknowledges gives the
   leader a lease of `LEASE_DURATION_MS` (900 ms), counted from when the round
   *started*. Inside the lease the leader answers from local memory.
2. **Why the lease is safe.** A follower votes for a new leader only after
   `ELECTION_TIMEOUT_MS` (1,000 ms) without a heartbeat. The lease is shorter
   than that by `MAX_CLOCK_DRIFT_MS` (100 ms), so it always expires before any
   majority could elect someone else. The test
   `test_no_new_leader_can_be_elected_while_the_old_lease_is_valid` checks that
   an election during the lease fails.
3. **Read index after the lease.** When the lease has expired, the leader
   sends a heartbeat round first. If a majority acknowledges its term, the
   lease is renewed and the read is served; otherwise it raises `NotLeader`,
   and the client retries on another node. A leader that meets a node with a
   newer term gives up its lease immediately.

In the tests the partitioned `a` refuses reads (`NotLeader`) as soon as `c` is
elected, `c` returns the new balance `200`, and 800 reads spread over the
900 ms lease on a healthy leader cost **zero** extra heartbeat rounds.

## Trade-offs
- **Lease reads trust clocks.** Safety depends on clocks not drifting more than
  `MAX_CLOCK_DRIFT_MS` per lease, and on the leader not pausing (GC, VM
  freeze) between checking the lease and answering. If you cannot bound
  drift, use read index for every read: always correct, but each read costs a
  majority round trip.
- **Minority-side clients lose reads, not just writes.** During the
  partition, clients of `a` get errors instead of answers. That is the price
  of never returning stale data; if a stale-but-labelled read is acceptable
  (e.g. a dashboard), serve it explicitly as a follower read with its age.
- **Follower reads** (read index requested by a follower from the leader)
  spread read load but add a hop; bounded-staleness reads are cheaper still.
- **Simplified model.** There is no log, no commit index and no real
  messages; a read index in real Raft also waits until the state machine has
  applied up to the commit index it recorded. Use a proven implementation
  (etcd, Consul, ZooKeeper's `sync` + read) rather than writing your own.

## Run
```bash
python -m unittest -v test_solution.py
```
