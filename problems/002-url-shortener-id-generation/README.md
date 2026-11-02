# 002 · URL shortener: random short codes started colliding, and the DB sequence became a bottleneck

## Problem
A marketing link shortener generated a random 6-character code per URL and
retried on collision. At 40 million links, about 1 in 1,400 inserts hit an
existing code, and every collision meant an extra round trip under a unique-index
lock; the rate kept climbing as the table grew. A rewrite used one database auto-increment
sequence instead, but every app server then waited on the same primary DB
row lock, capping throughput at ~3,000 links/s and making the DB a single
point of failure.

## Root cause
- **Birthday problem:** with 62⁶ ≈ 56.8 billion codes, collisions start long
  before the space is "full": the chance of *any* collision passes 50% at only
  ~280,000 codes, and the chance a new code hits an existing one (n / 62⁶)
  grows linearly with every link created.
- A single global counter is **centralized coordination** on every write.

## Solution
**Snowflake-style IDs**, generated locally on each server with no coordination,
then **Base62-encoded** for the URL:

```
| 41 bits: ms since custom epoch | 10 bits: worker id | 12 bits: sequence |
```

- Unique across up to 1,024 workers, and up to 4,096 IDs per ms per worker (~4M/s each).
- Roughly time-sortable, which keeps B-tree index inserts efficient.
- The sequence overflowing within a millisecond → wait for the next ms.
- **Clock moving backwards** (NTP adjustment) → refuse rather than risk duplicates
  (small rollbacks are waited out).
- A 64-bit ID in Base62 is at most 11 characters; recent IDs are ~10.

Worker IDs are assigned at startup from config / ZooKeeper / a k8s StatefulSet ordinal.

## Trade-offs
- Codes are 10–11 chars instead of 6. If short codes are a hard product need,
  use a counter with **block allocation** (each server reserves 10,000 IDs at a
  time from the DB) and shuffle the order so codes are not guessable.
- Sequential IDs leak creation volume; use a reversible permutation of the bits
  if that matters.
- Worker ID collisions break uniqueness, so assignment must be strict.

## Run
```bash
python -m unittest -v test_solution.py
```
