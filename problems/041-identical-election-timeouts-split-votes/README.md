# 041 · After the metadata leader crashed, the cluster sat with no leader for minutes while every node kept starting elections and logs showed the term climbing

## Problem
A five-node Raft-style metadata cluster (it stores the shard map and leases
for a large storage service) lost its leader to a kernel panic. Failover was
expected to take a few hundred milliseconds. Instead the cluster had no
leader for minutes: every write to the shard map failed, so shard moves,
lease renewals and new bucket creation all stopped across the service. Logs
on all four survivors showed the same thing over and over: "election
timeout, starting election for term N", "vote denied: already voted in term
N", with N climbing by one roughly every 150 ms. The leader came back only
after an engineer stopped two of the nodes by hand.

## Root cause
**Every follower used the same fixed election timeout (150 ms).** The
followers had all received the dead leader's last heartbeat within a couple
of milliseconds of each other, so their timers fired within a couple of
milliseconds of each other too. Each one became a candidate, voted for
itself and sent vote requests. Raft lets a node vote only once per term, and
each node had already voted for itself before any other request arrived
(one-way delay 3–8 ms, including persisting the vote), so every candidate got
one vote out of the three it needed: a **split vote**. All four then reset the
same 150 ms timer and repeated the split in the next term, forever. The
skew between nodes never grows, because nothing in the system adds any.

In the simulation (5 nodes, leader crashed at t=0, survivors last heard it
0–2 ms apart, message delay 3–8 ms, give up after 10 s): with the fixed
timeout, **1,000 out of 1,000 seeded failovers elected no leader within 10
seconds**. For seed 0 all four nodes started an election in every one of 66
terms (264 candidacies) and none won.

## Solution
`solution.py` uses Raft's randomized election timeout: each time a node
resets its timer (on start-up, on becoming a candidate, on granting a vote)
it picks a fresh random timeout between 150 and 300 ms. Usually one node
times out clearly before the others; its vote requests reach them while they
are still followers, they grant the vote and reset their own timers, and it
wins. If two nodes still tie, they draw new random timeouts for the next
term, so the tie does not repeat.

Results over the same 1,000 seeded failovers:

| Timeout policy | No leader after 10 s | Median time to leader | Worst |
|---|---|---|---|
| Fixed 150 ms | 1,000 | – | – |
| **Random 150–300 ms** | **0** | **187 ms** | **691 ms** |

863 of the 1,000 randomized failovers needed only one election.

The vote rules (one vote per term, step down on a newer term, ignore votes
for an old term, ignore an old timer after a reset) are the standard Raft
rules and are tested separately.

## Trade-offs
- The random range is a latency trade: the mean failover time grows with
  the upper bound, but a range that is narrow compared with the message delay
  brings split votes back. The Raft paper recommends a timeout an order of
  magnitude above the broadcast time; tune both to your real network delay.
- Randomization makes split votes unlikely, not impossible (137 of 1,000
  runs needed more than one election here). Elections stay safe either way;
  only availability suffers.
- The simulation leaves out the log up-to-date check, so it shows liveness
  only, not which node should win. Real Raft also refuses votes to a
  candidate whose log is behind.
- Alternatives and additions: Pre-Vote (a node checks it could win before
  bumping the term, which also stops a partitioned node from disrupting the
  cluster), ranked or priority-based elections (a fixed order, with a
  deterministic fallback), or an external lock service (ZooKeeper, etcd)
  instead of a home-grown election.

## Run
```bash
python -m unittest -v test_solution.py
```
