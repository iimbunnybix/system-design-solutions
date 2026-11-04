# 004 · Two schedulers both thought they were leader and charged the same subscriptions twice

## Problem
A billing scheduler runs as three replicas; only the elected leader may
run the nightly "charge due subscriptions" job and write charge records.
Leadership uses a 10-second lease in a coordination service, renewed every
3 seconds. One night a leader hit a long stop-the-world GC pause. When it
resumed it carried on with the batch it had started, while a new leader,
elected during the pause, ran the same batch. A few hundred customers were
charged twice and the ledger had interleaved writes from two processes.

## Root cause
A lease only tells you that you **were** leader when you last checked.
The old leader checked ("I hold the lease"), then paused for longer than
the lease TTL. The lease expired, another replica acquired it, and when the
old process resumed it acted on its stale belief. No timing margin fixes
this: a pause can happen *between* the check and the write. The storage
accepted writes from anyone who reached it. (`test_naive_storage_lets_zombie_leader_overwrite`
reproduces the scenario: the zombie's write lands last and wins.)

## Solution
**Fencing tokens** (as described by Martin Kleppmann for distributed locks):

- `LeaseManager` hands out a **strictly increasing token** with every new
  grant (1, 2, 3, ...). Renewing keeps the same token; a new holder always
  gets a bigger one. Only the current holder can renew or release, so a
  zombie can't free the new leader's lease.
- Every write to the protected resource carries the token.
  `FencedStorage` remembers the highest token seen and rejects anything
  lower with `StaleToken`, atomically with the write. Writes without a token
  are rejected.
- The zombie leader from the GC scenario is rejected; only the new leader's
  write is in the log. Under 20 concurrent writers with random arrival
  order, the accepted tokens are always non-decreasing and the highest token
  wins.
- Workers still check their lease locally with a safety margin before
  writing. That reduces wasted work, but correctness relies on the
  resource-side check.

In a SQL database the same check is a conditional write:
`UPDATE jobs SET ..., fence = :token WHERE id = :id AND fence <= :token`
(0 rows updated → stale leader). etcd revisions and ZooKeeper zxids make
natural tokens.

## Trade-offs
- Every protected resource must enforce the token. A downstream API you
  don't control (a payment provider) can't, so also use **idempotency keys**
  for external side effects.
- Fencing prevents a stale leader from writing; it does not make the
  leader's in-memory view correct. The new leader must re-read state.
- Shorter leases mean faster failover but more false failovers from brief
  network blips; longer leases mean longer outages when a leader really
  dies.
- A lease service built on a single node (e.g. one Redis) can itself lose
  the lease state on failover; use a consensus-backed store (etcd,
  ZooKeeper, Consul) for the lease.

## Run
```bash
python -m unittest -v test_solution.py
```
