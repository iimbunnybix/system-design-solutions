# 065 · After a dead disk was replaced, 150 user profiles came back old or "not found" although nothing was deleted

## Problem
A user-profile store keeps 3 replicas of every key (A, B, C) with quorum
reads and writes: a write succeeds after 2 acks (W=2) and a read asks
2 replicas (R=2) and returns the newest version. Because R + W > N
(2 + 2 > 3), every read set is supposed to overlap every acknowledged write.

Earlier in the week replica B restarted for a kernel patch. While it was
down, writes kept succeeding through A and C. Two days later C's disk failed.
The on-call engineer replaced the disk, started the node with C's old name
and an empty data directory, and put it straight back into rotation.

The client sends reads to the two fastest replicas, and an empty disk answers
fastest. In the simulation (1,000 keys written to all three replicas, then
100 keys updated and 50 keys created while B was down):

- Before the disk failure every read is correct.
- After the replacement, the 100 updated profiles read back at their **old
  version** and the 50 new profiles read back as **"not found"**: 150 of
  1,050 keys wrong. Users saw reverted settings and new accounts that
  "did not exist", and support tickets started within minutes.

## Root cause
The R + W > N overlap argument assumes every replica that answers a read
still holds what it acknowledged. The replacement node had C's identity but
none of C's data, so it answered as a full member of the quorum with an
empty disk. For the 150 keys acknowledged by A and C, the only surviving
copy was on A, and a read set of {C, B} never reached it.

This is not the stale-replica problem (problem 014, where read repair
fixes one out-of-date copy) or anti-entropy between live replicas
(problem 015): it is a **membership** bug. An empty node was allowed to
vote before it had the data its identity promised.

## Solution
Each replica has a state (`ReplicaState`):

1. `replace()` starts the new node as **BOOTSTRAPPING**. It receives every
   new write, so it does not fall further behind, but reads never ask it and
   its ack does not count toward W.
2. `bootstrap()` streams every key from **all** serving peers, keeping the
   newest version of each. Copying from one peer is not enough: the newest
   copy of a key may now exist on only one survivor.
3. `Replica.put()` never replaces a newer version with an older one, so a
   live write that arrives during the stream is not overwritten by a
   streamed old copy.
4. Only then is the node marked **SERVING** and allowed into read sets.
5. If fewer than R replicas are serving, reads raise `QuorumUnavailable`
   instead of answering from too few copies.

In the same simulation, 0 of 1,050 reads are wrong while C bootstraps, and
0 are wrong after it is serving again at the front of the read order.

## Trade-offs
- During bootstrap the cluster runs with two serving replicas. Losing one
  more makes reads and writes unavailable; that is deliberate, since the
  alternative is silently wrong answers.
- Not counting the bootstrapping node toward W means a write during
  bootstrap needs both A and B. Cassandra handles this the other way round:
  it sends writes to the pending node *in addition to* the normal W.
- Streaming everything is slow for large nodes. Real systems stream only
  the token ranges the node owns and throttle the stream so it does not
  hurt live traffic; Merkle-tree repair (problem 015) then fixes anything
  missed.
- Read repair alone would not have saved these keys: a read from {C, B}
  never sees A's copy, so there is nothing newer to repair with.
- The operational fix matters as much as the code: a node that comes back
  with an empty disk should get a new identity (or an explicit "replace"
  flag), so the cluster knows it is not the old member.
- The model has no deletes or tombstones, versions are given by the caller,
  and replicas are listed in a fixed latency order.

## Run
```bash
python -m unittest -v test_solution.py
```
