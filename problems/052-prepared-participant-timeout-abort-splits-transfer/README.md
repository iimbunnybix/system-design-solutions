# 052 · Customers saw money leave their account but never reach the recipient after a coordinator restart

## Problem
A wallet service stores accounts on separate ledger shards. A transfer between
accounts on different shards runs as a two-phase commit: a coordinator asks
both shards to **prepare** (check the balance, lock the account, vote yes),
then tells each to **commit**. During a deploy, a coordinator was restarted
while transfers were in flight. Some customers saw ₹500 debited from their
balance while the recipient never received it; reconciliation found the two
ledgers no longer summed to the same total, and finance had to correct
transfers by hand.

## Root cause
Two rules combined into a split decision:
1. A prepared shard that heard nothing within its timeout **aborted on its
   own** to release the account lock (`naive_on_timeout`).
2. The coordinator **never recorded its decision**, so after a restart nobody
   knew what had been decided.

The coordinator decided "commit", told `ledger-a`, and died before reaching
`ledger-b`. `ledger-a` committed the debit; `ledger-b` timed out and aborted
the credit. In `ReproduceProblem` the two shards start with ₹1000 each and
end with ₹1500 in total: ₹500 disappeared.

## Solution
- **Log the decision before sending it.** `run_transaction` writes commit or
  abort to the durable `DecisionLog` before any shard hears it, so the outcome
  survives a crash.
- **A shard that voted yes never decides alone.** `LedgerShard.on_timeout`
  reads the logged decision when the coordinator is reachable; otherwise it
  stays `PREPARED`, keeping the lock, and retries later. Before voting yes,
  aborting alone is still safe (no one can have committed).
- **Recovery finishes what was started.** `recover_coordinator` re-sends the
  logged decision to every shard. If nothing was logged, the crash happened
  before a decision, so no shard can have committed and aborting is safe
  ("presumed abort"). `finish` ignores repeats, so re-sending is harmless.

With these rules the same crash ends with `ledger-a` at ₹500, `ledger-b` at
₹1500 and the total back at ₹2000 (`test_recovered_coordinator_resends_commit`).

## Trade-offs
- Two-phase commit is a **blocking** protocol: while the coordinator is down,
  an in-doubt shard keeps its account locked and refuses other transfers on it
  (`test_in_doubt_shard_refuses_other_transactions`). Keep coordinators highly
  available (restart quickly, or replicate the decision log with Raft/Paxos, as
  Spanner does) and alert on transactions in doubt for too long.
- Writing the decision costs one durable write per transaction on the
  coordinator, plus one round trip per shard in each phase.
- Letting shards ask *each other* (cooperative termination) can resolve some
  in-doubt cases without the coordinator, but not when every shard is
  prepared and none has heard the decision.
- Many systems avoid cross-shard atomic commits altogether with sagas and
  compensating transfers (problem 013) or by keeping both accounts of a
  transfer on one shard; those give up isolation instead of availability.

## Run
```bash
python -m unittest -v test_solution.py
```
