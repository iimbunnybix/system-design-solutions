# 020 · The database cluster failed over dozens of times a day, and each time the "dead" primary turned out to be healthy

## Problem
A replicated database cluster elects a primary and has every node send a
heartbeat to the others once a second. If the primary is silent for more than
3 seconds, the replicas declare it dead and elect a new one.

One primary ran on a JVM-based proxy with periodic garbage-collection pauses of
a few seconds. It was healthy, but every pause triggered a failover: in-flight
transactions were aborted, clients reconnected, and caches on the new primary
started cold. Raising the timeout to 10 seconds stopped the false failovers but
meant a real crash on any node now took 10 seconds to detect.

## Root cause
A **fixed timeout** treats every node the same: "silent > 3 s" means dead. It
cannot tell "this node is often a bit late" from "this node is usually exact
and has now stopped". In the simulation (600 heartbeats roughly 1 s apart, with
a 4 s gap every 20th beat), the fixed detector reports the healthy node dead
**30 times, once for every one of its 30 pauses**.

## Solution
A **phi accrual failure detector** (Hayashibara et al., 2004; used by Cassandra
and Akka). Instead of a yes/no timeout, it keeps the last 100 gaps between each
node's heartbeats and computes

`phi = -log10(P(the next heartbeat arrives later than the current silence))`

using a normal distribution with that node's mean and standard deviation.
phi 1 means a 10% chance the node is just late, phi 8 a one-in-10^8 chance. The
node is suspected when phi reaches `PHI_THRESHOLD = 8`.

- For the GC-pausing node the history contains 4 s gaps (mean 1.14 s, standard
  deviation 0.66 s), so a 4 s silence has phi 5.2 and is tolerated. The only
  two false suspicions are the first two pauses, before any pause was in the
  history; after that there are **none**.
- For a steady node, the same 4 s silence has phi 9.0, so it is suspected.
  When the node really dies, the steady node is detected after **3.9 s** and
  the GC-pausing node after **4.9 s** (checks every 0.1 s): each gets the slack
  its own behaviour needs.
- The standard deviation is floored at 0.5 s so a perfectly regular node does
  not become hair-trigger, and with fewer than 3 samples the detector falls
  back to the old fixed timeout instead of guessing.

## Trade-offs
- Heartbeat gaps are not really normally distributed; GC pauses make a long
  tail. Akka adds an "acceptable heartbeat pause" to the mean for the same
  reason. The normal model is a good enough ranking of how unusual a silence is.
- The detector needs a learning period, so the first pauses of a new node are
  still flagged (2 in the simulation). Seeding the history with expected gaps
  shortens this.
- Adapting is a double-edged sword: a node that slowly gets worse teaches the
  detector to tolerate it. Alert separately on rising mean heartbeat gap.
- The threshold is still a tuning knob: lower detects faster with more false
  alarms. Failure detection in an asynchronous network can never be both
  perfectly fast and perfectly accurate.
- Fixing the pauses themselves (GC tuning, sending heartbeats from a thread
  that GC cannot stop) is worth doing too; the detector only makes the cluster
  tolerate them.

## Run
```bash
python -m unittest -v test_solution.py
```
