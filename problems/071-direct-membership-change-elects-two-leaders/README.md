# 071 · Growing the metadata cluster from 3 to 5 nodes produced two leaders, and both accepted writes

## Problem
A Raft-style metadata cluster of three nodes (A, B, C) stores the routing
table for a sharded database. To survive two failures instead of one, the
team grew it to five nodes by starting D and E and then pushing the new
member list {A, B, C, D, E} to every node at once.

The push reached C, D and E first. A top-of-rack switch then flapped and cut
A and B off from the other three for about 20 seconds. Both sides timed out
and started an election in the same term. A won on its side and C won on
its side, and both accepted writes: one moved shard 17 to a new host, the
other split it. When the network healed, the two logs disagreed and the
routing table had to be rebuilt by hand while clients were sent to the
wrong shard.

In the model, `elect_on_each_side({A, B}, old config, {C, D, E}, new
config)` returns two leaders, and `disjoint_winning_splits` finds **3**
different splits of the five nodes that give two leaders during a direct
3 -> 5 change.

## Root cause
**Nodes count votes against different member lists during a direct
membership change, and a majority of the old list does not have to overlap
a majority of the new one.** Leader election is safe only because any two
majorities of the *same* list share a node, and that node votes once per
term (problem 023 relies on this with fixed membership). Nodes cannot
switch configuration at the same instant, so for a while some count with
{A, B, C} (2 votes win) and others with {A, B, C, D, E} (3 votes win).
{A, B} and {C, D, E} share nobody, so both can win.

Replacing a node in one step ({A, B, C} -> {A, B, D}) has the same problem:
{A, C} is a majority of the old list and {B, D} of the new one.

## Solution
**Change one server at a time, and commit each change before starting the
next** (the single-server change approach from the Raft dissertation).

- `single_server_plan` turns {A,B,C} -> {A,B,C,D,E} into {A,B,C} ->
  {A,B,C,D} -> {A,B,C,D,E}, and a replacement into add-then-remove.
- When two lists differ by one server, any majority of one overlaps any
  majority of the other: the two majority sizes always add up to one more
  than the number of distinct nodes (for example 2 + 3 = 5 nodes total for
  {A,B,C} and {A,B,C,D}), so they must share a node. `plan_is_safe` checks
  this exhaustively over every split for each step.
- `MembershipManager` refuses a change of more than one server, and refuses
  a new change while one is still pending, because two changes in flight
  would put three lists in use, and the first and last differ by two.

## Trade-offs
- **Slower changes:** growing by two servers takes two committed steps
  instead of one. For metadata clusters this is seconds.
- **Joint consensus** (the original Raft paper) allows arbitrary changes in
  one step by requiring a majority of both old and new lists during the
  transition. It is more general but more complex; most production systems
  (etcd, for example) use single-server changes and learners instead.
- **New nodes should catch up first.** A new node added as a voting member
  with an empty log can slow commits; add it as a non-voting learner, let it
  copy the log, then promote it.
- This model checks vote counting only. A real implementation must also use
  the newest config in its log (even uncommitted) and refuse votes from
  removed members that keep timing out.

## Run
```bash
python -m unittest -v test_solution.py
```
