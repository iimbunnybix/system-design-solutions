# 022 · Users' profile edits silently reverted, but only when saved through one data center

## Problem
A multi-region profile service replicates every key between three
data centers with last-write-wins (LWW): each write is stamped with the
local server's wall clock, and every replica keeps the value with the
highest stamp. After an NTP misconfiguration one region's servers ran about
5 seconds behind. Users who edited their profile, saw the change, and edited
again within a few seconds (typical when fixing a typo) had the second edit
vanish: it was saved, acknowledged, replicated, and then discarded
everywhere. No error was logged because LWW was "working as designed".

## Root cause
Wall clocks are not ordered across machines. The first edit was stamped by
an accurate server at time T; the second, made 1 s later on the lagging
server, was stamped T − 4 s. LWW compared the numbers and kept the older
edit, even though the second edit was made *after* its server had already
received the first. `test_newer_write_from_lagging_node_is_dropped`
reproduces this: both replicas end with `v1`.

## Solution
Stamp writes with a **hybrid logical clock** (Kulkarni et al., 2014), the
scheme used by CockroachDB and YugabyteDB. A stamp is
`(wall_ms, logical, node_id)`, compared in that order:

- **Local write**: if the wall clock is ahead of the last stamp, use it with
  `logical = 0`; otherwise keep the last wall time and increment `logical`.
  Stamps never go backwards, even if NTP steps the clock back.
- **Receiving a write**: move to the largest of local wall time, the last
  stamp and the remote stamp, and bump `logical` so the next local stamp is
  strictly greater than anything seen. The lagging region's second edit now
  sorts after the first and wins on every replica.
- **Node id** breaks exact ties so all replicas pick the same winner for
  truly concurrent writes.
- **Drift guard**: a remote stamp more than `MAX_CLOCK_DRIFT_MS` (60 s) ahead
  of local time is rejected; otherwise one broken clock would drag every
  node's stamps into the future.

When clocks are in sync, `wall_ms` equals real time and `logical` stays 0,
so stamps remain readable timestamps.

## Trade-offs
- HLC orders writes the node had **seen**. Two edits made concurrently on
  different nodes, neither aware of the other, are still resolved by
  roughly-wall-clock order and one is lost. If both must survive, use
  multi-value registers or CRDTs (see problem 006) or route a key's writes
  to one leader.
- Stamps are larger (wall + counter + node id) than a plain timestamp.
- The drift limit must exceed real clock error; too tight and healthy
  messages are rejected, too loose and a bad clock can still push stamps far
  ahead. Monitor clock offset separately.
- Alternatives: version vectors (exact causality, but size grows with the
  number of writers) or TrueTime-style commit wait (needs bounded-error
  clocks).

## Run
```bash
python -m unittest -v test_solution.py
```
