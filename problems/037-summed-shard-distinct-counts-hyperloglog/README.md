# 037 · The weekly-active-users dashboard showed 7x more users than had signed up, and it grew with every shard added

## Problem
A consumer app's analytics service is split across 8 ingestion shards
(sessions are routed by session id, so one user's sessions can land on any
shard). Each shard keeps a set of the user ids it saw per day and stores
the size of that set: "distinct users on shard S on day D". The weekly
active users (WAU) figure on the executive dashboard was the sum of those 56
numbers. WAU came out several times larger than the number of registered
accounts, and it jumped every time the platform team added a shard, though
traffic had not changed. Growth targets and ad-revenue forecasts had been
based on the inflated number.

In the reproduction (40,000 users, each active on 1-7 random days with
1-3 sessions per active day, 8 shards, seed 7) the summed report gives
**294,569** weekly active users for a real **40,000** (7.4x). Even a single
day is wrong: day 3 reports **42,178** for **22,827** real users, because a
user whose sessions hit two shards is counted twice.

## Root cause
**Distinct counts are not additive.** `|A ∪ B| = |A| + |B| - |A ∩ B|`;
adding bucket sizes ignores the overlap, so every user is counted once per
(shard, day) bucket they appear in. More shards or longer ranges mean more
buckets per user and a bigger overcount. The exact fix (keep every bucket's
id set and take the union at query time) is correct but needs memory and
network transfer proportional to the number of users for every bucket and
every query.

## Solution
Store a **HyperLogLog** sketch per (shard, day) instead of a number
(`HyperLogLog` and `ActiveUserStore` in `solution.py`):

1. Each user id is hashed with a stable 64-bit hash (`blake2b`; Python's
   `hash()` is randomized per process, so sketches from different servers
   would not be comparable).
2. The top 14 bits of the hash select one of 16,384 registers; the register
   keeps the longest run of leading zeros seen in the remaining bits. Long
   runs are rare, so they indicate many distinct ids. Adding the same id
   again changes nothing.
3. **Merging** two sketches is a register-wise maximum and gives exactly
   the sketch of the union of their ids (tested), so a WAU query merges the
   56 sketches and estimates once. Duplicates across shards and days
   disappear automatically.
4. Small sets use the linear-counting correction, so a handful of users is
   counted exactly (tested with 1 and 10).

Result: WAU estimate **39,469** for 40,000 real users (-1.3%), day 3 within
0.6% of the truth. Each sketch is a fixed **16 KiB** whatever the number of
users, with a standard error of 1.04 / √16384 ≈ **0.81%**.

## Trade-offs
- The answer is an estimate. Error stays around 1% (about 2.5% at three
  standard errors) but finance or billing that needs exact counts must keep
  exact sets or query the raw events.
- Precision is a memory/accuracy dial: 2^p registers give 1.04 / √(2^p)
  error. All sketches that will be merged must use the same precision and
  the same hash function.
- Sketches support union, not reliable intersection ("users active on both
  Monday and Friday"); inclusion-exclusion on estimates has large error.
  Theta sketches handle set operations better.
- Production systems ship this ready-made: Redis `PFADD`/`PFMERGE`/
  `PFCOUNT`, BigQuery `HLL_COUNT`, Druid and Apache DataSketches. Redis and
  this code use the same register layout idea (16,384 registers).
- `ActiveUserStore` keeps exact sets too, only to reproduce the original
  report next to the fix; a real store would keep only the sketches.

## Run
```bash
python -m unittest -v test_solution.py
```
