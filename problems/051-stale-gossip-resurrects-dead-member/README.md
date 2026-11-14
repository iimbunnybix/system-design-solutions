# 051 · Clients kept getting connection errors from a cache node that had crashed, because it kept flipping back to "alive"

## Problem
A 60-node cache cluster uses gossip for membership: every second each node
sends its view of the cluster ("cache-c is alive", "cache-c is dead") to a few
random peers, and clients route keys only to members marked alive. When
`cache-c` crashed, the node that first noticed declared it dead, but for
several seconds most peers had not heard yet and kept gossiping their older
"alive" view. Every node's table flipped between alive and dead as the two
rumours crossed, so a share of requests kept going to the dead node and timed
out, and rebalancing for its key range started and stopped repeatedly.

## Root cause
The merge rule was **"the last message received wins"**. Gossip has no global
order: a message that arrives later is not necessarily newer, so a stale
"alive" carried by a slow path undoes the "dead" verdict. In the test stream
(alive, dead, then 10 rounds of stale "alive" interleaved with "dead", then
one more "alive"), the naive table changes `cache-c`'s status **22 times in
23 messages** and ends with the dead node marked alive (`ReproduceProblem`).

## Solution
Use SWIM-style **incarnation numbers** (`MembershipTable`, `overrides`):
- Every member state carries an incarnation number that only the member itself
  may increase. A message with a higher incarnation always wins.
- For the same incarnation the more severe status wins: dead > suspect > alive.
  A stale "alive" at incarnation 0 can never undo "dead" at incarnation 0, so
  the same stream settles on dead after **1 change** and stays there.
- A live node that hears it is suspected (a false alarm from packet loss)
  refutes it by bumping its own incarnation and gossiping "alive" with the new
  number, which beats the suspicion everywhere. A crashed node that restarts
  rejoins the same way, with a higher incarnation than the "dead" entry.
- `apply` returns the message only when it changed our view, so unchanged
  rumours are not re-broadcast forever.

## Trade-offs
- The member must remember its incarnation across restarts (or start from a
  value guaranteed higher, e.g. a timestamp); if it restarts at 0 it cannot
  rejoin until the "dead" entry is garbage-collected.
- "Dead" entries must be kept for a while (like tombstones) so stale "alive"
  rumours still meet them; dropping them too early reintroduces the bug.
- Suspects still get traffic here, as in SWIM, to avoid evicting healthy nodes
  on one lost packet; a stricter system would stop routing to suspects at the
  cost of more false evictions. Lifeguard-style tuning of suspicion timeouts
  and a strongly consistent membership store (etcd, ZooKeeper) are
  alternatives when the cluster is small.

## Run
```bash
python -m unittest -v test_solution.py
```
