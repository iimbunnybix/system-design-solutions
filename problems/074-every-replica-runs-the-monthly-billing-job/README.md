# 074 · Every customer was charged three times on the first of the month after billing scaled to three replicas

## Problem
A subscription service ran its monthly billing job from an in-process
scheduler: at 00:00 on the 1st, walk all customers and charge each
subscription. For two years the service ran as a single instance. Ahead of
a launch, the team scaled it to three replicas behind the load balancer for
availability.

On the 1st, every customer was charged three times. Card declines spiked
for customers near their limits, support received thousands of tickets,
and finance spent a week issuing refunds and explaining the duplicate
charges to the card networks.

## Root cause
**A singleton job was hidden inside a horizontally scaled service.** The
scheduler lived in the application process, so every replica had its own
copy and every copy fired on time. Nothing coordinated them, and the
charges carried no idempotency key, so the payment provider accepted each
one. In the model, 3 replicas and 200 customers give **600** charges, every
customer charged **3** times.

This is not overlapping runs of one scheduler (problem 027 in
devops-cloud-solutions, where a slow run overlaps the next tick) or a
thundering-herd cron (where many hosts run a harmless job at the same
second). Here each replica correctly ran the job once, and the job was
not safe to run more than once in total.

## Solution
1. **Claim the run, not the job.** Each scheduled run has a unique key
   (`monthly-billing@2026-12-01`). Before starting, a replica calls
   `try_claim`, an atomic conditional write in a shared store. Only one
   replica wins, the others skip. A finished run is never claimed again.
2. **Claims expire.** The claim has a 300-second TTL. If the winner dies
   mid-run, another replica (or the same one after restarting) can claim the
   run once the TTL has passed, instead of leaving it half done forever.
3. **Idempotent charges.** Each charge uses the key `<run key>:<customer>`.
   A takeover replays the whole customer list, and the provider skips
   customers already charged under that key.

Results in the model: with three replicas firing at once, only `billing-1`
runs and each of the 200 customers is charged once. If `billing-1` dies
after 80 charges, nobody takes over at 60 s (claim still live), the takeover
at 300 s finishes the run, and the total is still 200 charges, one per
customer.

## Trade-offs
- **The TTL is a guess about run time.** A healthy run that takes longer
  than the TTL can be claimed by a second replica while still running. The
  idempotency keys keep that safe, but work is duplicated. Renew the claim
  as the run progresses (see problem 073), or size the TTL well above the
  longest run.
- **Idempotency is what actually protects customers.** The claim prevents
  wasted work. The per-customer key prevents double charges, even if the
  claim logic has a bug. Keep both.
- **Alternatives:** run the job outside the service (a Kubernetes CronJob
  with `concurrencyPolicy: Forbid`, or a managed scheduler) so there is only
  one scheduler to begin with; or use leader election so only the leader
  schedules. Both still need idempotent charges, because jobs get retried.
- The shared store is now on billing's critical path. If it is down, no
  replica can claim, and the run waits: the safe direction for payments.

## Run
```bash
python -m unittest -v test_solution.py
```
