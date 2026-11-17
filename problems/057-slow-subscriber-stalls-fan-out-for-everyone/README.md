# 057 · During a cricket final, live-score updates reached every fan minutes late because one subscriber was on a bad connection

## Problem
A sports app pushes live-score updates over WebSockets. Each edge server
holds thousands of connections and a single publisher loop that, for every
new update, writes it to each connected subscriber in turn and waits for the
write to complete.

During a final, scores on every phone connected to one edge server started
lagging, first by seconds and then by minutes, while the other servers were
fine. Users saw the result on TV and social media long before the app, and
the incident review found that a few clients on congested mobile links were
reading slowly. Their TCP send buffers filled up, so each write to them
blocked, and every other subscriber waited behind them.

In the simulation in the tests (one update per tick, two healthy clients and
one that takes 10 ticks per message), after 100 updates the healthy clients
are 1,090 ticks behind, and the delay keeps growing with every update.

## Root cause
The fan-out was **synchronous and shared**: the time to publish one update
is the *sum* of all subscribers' delivery times, so the slowest consumer
sets the pace for everyone (head-of-line blocking). Once the slow client's
cost per message is larger than the time between updates, the backlog grows
without limit; there was no bound on how far behind a subscriber may fall.

## Solution
`isolated_broadcast` decouples the publisher from subscribers:

- Each subscriber has its **own bounded queue and its own worker**. The
  publisher only enqueues, which never waits, so a healthy subscriber gets
  every update within 1 tick no matter how slow the others are.
- When a subscriber's queue is full, its **overflow policy** decides:
  - `disconnect` for consumers that need every message: the connection is
    closed and the client reconnects and resyncs from a snapshot. In the
    tests the slow client is cut off at tick 9.
  - `drop_oldest` for "latest value wins" feeds like scores: the stalest
    queued update is discarded. Its delay is then bounded by
    `queue_limit × ticks_per_message` (81 ticks in the tests, with a limit
    of 8), instead of growing forever, and it still ends on the newest score.
- Dropped messages are counted, so slow clients show up on dashboards.

## Trade-offs
- Memory is `subscribers × queue_limit` messages; choose the limit from how
  stale a client may be allowed to get, not from free RAM.
- Disconnecting creates reconnect storms if many clients are slow at once
  (for example a regional mobile network issue); add jittered reconnect
  backoff on clients and a snapshot endpoint that is cheap to serve.
- `drop_oldest` is only correct when each message replaces the previous
  one. For deltas, conflate (merge) queued updates or send a fresh snapshot.
- Real servers get part of this from non-blocking I/O (asyncio, Netty), but
  an unbounded per-connection write buffer only moves the problem into
  memory; the explicit limit and policy are the important part.

## Run
```bash
python -m unittest -v test_solution.py
```
