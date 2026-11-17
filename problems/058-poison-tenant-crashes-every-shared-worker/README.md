# 058 · One customer's corrupt upload took the PDF rendering service down for every customer within seconds

## Problem
A SaaS invoicing product renders PDFs on a pool of eight worker processes
behind a load balancer. All tenants share the pool and requests are spread
round-robin, which kept utilisation even.

One tenant uploaded a template with a malformed font that crashed the
renderer. The tenant's client retried the request, as it was designed to;
each retry was routed to the next worker and crashed it too. Within seconds
all eight workers were down and every customer's invoice generation failed,
although only one tenant had a bad file. Workers restarted, the client
retried again, and the outage repeated until the tenant was blocked by hand.

## Root cause
Every tenant could reach every worker, so the **blast radius** of one bad
request was the whole fleet. Round-robin plus client retries is the worst
case: it guarantees a poison request visits each worker. Adding workers does
not help, because the poison request reaches each of them too.

## Solution
**Shuffle sharding** (`shuffle_shard`): each tenant is assigned a stable
pseudo-random set of 2 of the 8 workers, and its requests (including
retries) only ever go to those two.

- The shard is derived from a SHA-256 hash of the tenant name, so every load
  balancer instance computes the same shard without coordination.
- There are `8 choose 2 = 28` possible shards. The poison tenant can crash
  only its own two workers; another tenant loses service only if it has
  exactly the same pair. In the tests, 28 of 1,000 other tenants (2.8%) go
  down, close to the expected 1 in 28, instead of 100%.
- Tenants that share just one worker with the poison tenant (47% of them in
  the tests) keep working, because their client retries on the other worker
  in their shard.

## Trade-offs
- Each tenant has only 2 workers of capacity, so a big tenant can saturate its
  shard; size shards by tenant tier, or use larger shards for big tenants.
- The fix limits the damage; it does not stop it. Pair it with crash
  isolation (sandboxing the renderer), quarantining a request that crashed a
  worker, and per-tenant circuit breakers.
- With few workers the protection is weak: 4 workers with 2 per tenant give
  only 6 shards. The number of combinations grows quickly with fleet size
  (100 workers, 5 per tenant give about 75 million).
- Adding or removing a worker changes many tenants' shards with this simple
  scheme; a production version uses a stable assignment (for example
  rendezvous hashing per slot) so shards move as little as possible.

## Run
```bash
python -m unittest -v test_solution.py
```
