# 072 · After the orders database failed over, 79 orders customers had been told were placed did not exist

## Problem
The orders service writes to one primary database that replicates to two
replicas: one in another availability zone of the same region (about 2 ms
behind) and a disaster-recovery replica in another region (about 80 ms
behind). At peak it takes about 1,000 orders per second.

The primary's host lost power. The on-call engineer followed the runbook,
which said "promote the DR replica", and the service was back in four
minutes. Over the next day, support received complaints from customers
holding confirmation emails for orders that did not exist: the card had
been authorised and the email sent, but the order row was gone.

In the model (one order per millisecond, crash at 10 s), **79 acknowledged
orders** are missing from the promoted DR replica. Even promoting the more
up-to-date zone replica would still lose **1**.

## Root cause
**The primary acknowledged writes before any replica had them
(asynchronous replication), and failover promoted a replica without
checking how far behind it was.** With async replication, the replication
lag is a window of acknowledged writes that exist on exactly one machine.
When that machine dies, the window is gone (the recovery point objective
is not zero). Promoting the furthest-behind replica made the window as
large as possible.

This is not a stale read (problems 005 and 032): the data is not late, it
is never coming back.

## Solution
Two changes, and both are needed:

1. **Semi-synchronous acknowledgment** (`semi_sync_ack_time`): the primary
   tells the client "order placed" only after the fastest reachable
   replica has confirmed the write. Every acknowledged write now exists on
   at least two machines. A write whose confirmation was still in flight
   when the primary died was never acknowledged; the client saw a timeout
   and retries it (with an idempotency key) against the new primary.
2. **Promote the most caught-up replica** (`promote_most_caught_up`):
   compare log positions and promote the highest one, not whichever the
   runbook lists first.

Results from the model:

| Ack policy | Promotion | Acknowledged orders lost |
|---|---|---|
| Async | First listed (DR replica) | 79 |
| Async | Most caught up | 1 |
| Semi-sync | First listed (DR replica) | 78 |
| Semi-sync | Most caught up | 0 |

The cost is write latency: each ack waits about 2 ms longer (the zone
replica's round trip). If the zone replica is down, acks wait for the DR
replica (80 ms) and still lose nothing. If no replica can confirm, writes
are refused instead of being acknowledged with a single copy.

## Trade-offs
- **Latency and availability:** every write now pays a replica round trip,
  and with no reachable replica the database stops accepting writes. Some
  systems (MySQL semi-sync with `rpl_semi_sync_master_timeout`) fall back to
  async after a timeout instead; that keeps writes flowing but reopens the
  loss window, so alert when it happens.
- **One confirming replica protects against one failure.** Losing the
  primary and the confirming replica together still loses data; quorum
  replication (Raft, Paxos, or "ack from 2 of 3") is the general version.
- **Promotion needs fencing.** The old primary must be prevented from
  coming back and accepting writes (STONITH or fencing tokens, problem
  004), or the two primaries diverge.
- **Alternatives:** managed databases with synchronous standby (for example
  multi-AZ deployments) do this for you; an outbox or event log replayed
  after failover can find lost orders but only after the fact.

## Run
```bash
python -m unittest -v test_solution.py
```
