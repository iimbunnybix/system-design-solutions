# 035 · One customer uploaded 2,000 reports and every other customer's export sat "queued" for over a minute and a half

## Problem
A B2B analytics product renders PDF/CSV exports on a shared pool of workers
fed by one job queue. Most tenants submit a handful of exports per hour,
each taking about 50 ms of worker time. One Monday morning a large tenant's
integration submitted 2,000 exports at once. Every other tenant's export
landed behind those 2,000 jobs: the export button spun, support tickets
piled up ("exports are broken"), and some customers clicked again, adding
even more jobs. Nothing was down; the workers were 100% busy doing correct
work, just for one tenant.

In the reproduction (one worker, 2,000 × 50 ms bulk jobs queued at t = 0, two
other tenants each submit one 50 ms job at t = 1 s) the two small jobs wait
**99,000 ms and 99,050 ms** before they start.

## Root cause
**A single FIFO queue shares worker time in proportion to how many jobs a
tenant submits.** Order of arrival is the only rule, so whoever queues the
most work gets the whole pool until their backlog drains. There is no notion
of "whose turn is it", so a short job from a quiet tenant cannot overtake a
long backlog from a noisy one. Adding workers only shortens the wait; the
next, bigger burst fills them again.

## Solution
Fair queuing by tenant with **deficit round robin** (DRR, Shreedhar &
Varghese 1995), `DeficitRoundRobinQueue` in `solution.py`:

1. Each tenant gets its **own FIFO queue**; tenants with queued jobs sit in a
   round-robin ring.
2. On each visit, a tenant's **deficit** budget grows by a fixed quantum
   (50 ms of work by default). It may run jobs while the next job's cost fits
   the budget, then the worker moves on to the next tenant.
3. Unused budget carries over, so a job larger than the quantum runs after a
   few visits (tested: a 180 ms job with a 50 ms quantum runs on the 4th
   visit). A tenant whose queue empties leaves the ring and **loses** its
   leftover budget, so idling cannot be saved up for a later burst.

Because budgets are measured in work time, not job count, every busy tenant
gets the same share of the worker: a tenant with 500 ms jobs and a tenant
with 50 ms jobs each get exactly 5,000 ms of the first 10,000 ms (tested).

Result in the same scenario: the two small jobs wait **0 ms and 50 ms**, and
the bulk tenant still finishes at **100,100 ms**, exactly when it does with
FIFO. The scheduler is work-conserving: it changes the order, never the
total.

## Trade-offs
- **Job cost must be known or estimated** up front (rows to export, bytes,
  a per-type average). Bad estimates skew shares; charging the real duration
  after the job finishes (as in start-time fair queuing) corrects this.
- Equal shares are not always the business rule: give paying tiers a larger
  quantum (weighted DRR), or cap each tenant's concurrent jobs across the pool.
- A tenant that splits itself into many "tenants" (per-user keys) gets more
  shares; choose the fairness key carefully (account, not API key).
- With many workers, the per-tenant queues need to live in a shared store
  (Redis lists per tenant plus a ring of active tenants) or the pool must be
  partitioned; a single in-process scheduler is the simple case shown here.
- Fairness does not reduce total load. A tenant that floods the system still
  needs admission control (rate limits or a per-tenant queue length cap).

## Run
```bash
python -m unittest -v test_solution.py
```
