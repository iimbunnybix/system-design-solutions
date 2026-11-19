# 061 · Readers in Asia saw answers "replying to a deleted comment" for questions that had just been posted

## Problem
A tech Q&A site serves readers from three regions (EU, US, APAC). Each
region accepts writes locally and replicates every new comment to the other
two asynchronously, over independent links, so posting is fast everywhere.

After a network change made the EU->APAC link slower than EU->US and
US->APAC, APAC readers started seeing answers shown as "replying to a
deleted comment" for a few seconds, then the question would appear. The
notification service in APAC, which looked up the parent comment to notify
its author, threw errors for those replies and dropped the notifications.

## Root cause
Replication delivered each write **as soon as it arrived** (`naive_receive`).
The answer was written in the US after the US had seen the question, so the
answer causally depends on the question. But the question travelled
EU->APAC on one link and the answer US->APAC on another; with no ordering
between links, the answer could arrive first. Nothing in the write said "do
not show me before comment q1".

## Solution
**Causal delivery with version vectors** (`Replica.receive`):

- Each replica counts how many writes from each region it has applied
  (`applied`, e.g. `{eu: 1, us: 0, apac: 0}`).
- A local write increments its own region's count and carries a copy of the
  vector: the answer from the US carries `{eu: 1, us: 1, apac: 0}`, meaning
  "written after seeing 1 EU write".
- A replica applies an incoming write only if it is the **next** write from
  its origin and the replica has applied at least as many writes from every
  other region as the vector says. Otherwise it waits in `pending`.
- Every time a write is applied, pending writes are re-checked, because one
  arrival can unblock several.

In the test, APAC receives the answer first: it waits in `pending`, is not
visible, and is applied right after the question arrives, so no orphan is
visible at any moment. Gaps in one region's sequence are waited for,
duplicates are dropped, and unrelated writes are applied in arrival order.

## Trade-offs
- The vector has one entry per region (or per writer). That is cheap for a
  handful of regions; with many writers, use per-region vectors as here and
  not per-user ones.
- A lost write blocks everything that depends on it. Replication links must
  retry until delivered, and `pending` size and age need monitoring and
  alerting.
- Causal delivery tracks what the author's replica had seen, which can
  hold back writes that do not truly depend on each other (false
  dependencies). Tracking explicit dependencies (here, the `parent_id`)
  holds back less but must cover every relationship that matters.
- Some databases offer this as "causal consistency" (for example MongoDB
  causally consistent sessions); a single-leader setup avoids the problem
  at the cost of cross-region write latency.

## Run
```bash
python -m unittest -v test_solution.py
```
