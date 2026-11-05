# 023 · After a switch failure, two nodes both claimed to be leader and customer balances diverged

## Problem
A five-node metadata store (one leader, four followers, spread over two
racks) holds account balances for a wallet service. A top-of-rack switch
failed and split the cluster into two racks: nodes `a` (the leader) and `b`
on one side, `c`, `d` and `e` on the other. The three-node side stopped
hearing from `a` and elected `c`. `a` never noticed anything wrong and kept
serving its clients, and `b` timed out too and declared itself leader. For
the 20 minutes the switch was down, clients on both racks got successful
write acknowledgements. When the network healed, the two halves held
different balances for the same accounts, and engineers had to reconcile
them by hand from application logs.

## Root cause
The election counted a "majority" of the nodes the candidate **could reach**,
not of the **configured cluster**. Inside each half of a partition, every
reachable node agrees, so each half (even a single isolated node) always
wins. Writes had the same flaw: the leader applied a write to whichever
nodes it could reach and acknowledged it. Nothing tied a leader to a point
in time either, so the old leader had no way to learn that it had been
replaced. `NaiveCluster` reproduces all three: in the tests `a`, `b` and `c`
all end up as leaders, and after healing `a` holds `100` while `e` holds `200`.

## Solution
`QuorumCluster` applies the rules used by Raft (and, in similar form, by
ZooKeeper's Zab and Paxos):

- **Majority of the configured cluster.** A candidate needs
  `cluster_size // 2 + 1` votes (3 of 5). Two disjoint sides cannot both have
  more than half of the same fixed membership, so at most one side can elect.
- **Terms.** Every election increments the term. A node grants at most one
  vote per term and never votes for an older term. A node that sees a newer
  term becomes a follower, which is how a stale leader steps down.
- **Majority to commit.** A write is acknowledged only when a majority of the
  configured cluster accepts the leader's term. The old leader stranded on
  the 2-node side gets `NotCommitted` instead of silently diverging, and after
  the heal it gets `NotLeader` as soon as it meets a node with a newer term.

## Trade-offs
- **Availability is lost on the minority side.** Clients connected to `a`
  and `b` cannot write until the partition heals. With an even split (2 + 2
  in a 4-node cluster) *no* side can elect a leader; this is why clusters use
  an odd number of nodes (3 or 5).
- **Reads still need care.** A stale leader can still serve stale *reads*
  from local memory. Linearizable reads need a read index (confirm leadership
  with a majority first) or a time-based leader lease with bounded clock drift.
- **Term inflation.** A minority node that keeps retrying elections keeps
  raising its term, and when the network heals it forces a needless
  re-election. Raft's *pre-vote* step and check-quorum avoid this.
- **Simplified model.** Here a write is applied only once a quorum is known.
  Real Raft appends entries to a log, commits by index, and also requires a
  candidate's log to be at least as up to date as the voter's. Use a proven
  implementation (etcd, ZooKeeper, Consul) instead of writing your own.
- **Alternative:** a single external lock service with fencing tokens
  (problem 004) moves the quorum problem into that service.

## Run
```bash
python -m unittest -v test_solution.py
```
