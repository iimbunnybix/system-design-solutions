# 008 · The "is this username available?" check became the biggest source of database load

## Problem
The sign-up form checks username availability as the user types (debounced
to ~300 ms), and each check runs a uniqueness query on the users database.
During a campaign that brought a wave of sign-ups, this endpoint became the
single largest source of queries on the primary, ahead of login and feed
traffic, and its latency rose with everything else. Almost all of those
queries answered "available": people try many prefixes and variations that
nobody has taken.

## Root cause
A **negative lookup with no cache**. Ordinary caches store values for keys
that exist; they do not help when most lookups are for keys that *don't*
exist, and caching every "not found" would need unbounded memory and
invalidation on every sign-up. So every keystroke reached the database. In
the test workload (20,000 checks, 10% for taken names) the database receives
**all 20,000** queries.

## Solution
**A Bloom filter of taken usernames in each API instance**, in front of the
database:

- Not in the filter → **definitely available** (no false negatives), answered
  in memory.
- In the filter → *maybe* taken → ask the database, which remains the source
  of truth. The actual sign-up still relies on the DB's unique index.

Sizing uses the standard formulas `m = -n·ln p / (ln 2)²` bits and
`k = (m/n)·ln 2` hash functions. For 50,000 names at a 1% target that is
479,253 bits (~59 KB, ~9.6 bits per name) and `k = 7`. The k positions come
from one SHA-256 via double hashing (`h1 + i·h2`).

Measured in the tests:

- No false negatives for any of the 50,000 inserted names.
- False-positive rate on 50,000 unseen names: **0.93%** (formula: 1.00%).
- Database queries for the 20,000-check workload drop from 20,000 to
  **2,160** (the taken names plus ~1% of the free ones), with identical
  answers.
- A filter loaded to 5× its capacity has an **84%** false-positive rate.
  `needs_rebuild()` flags it once it passes capacity.

New sign-ups are added to the filter right after the DB commit. Names are
normalized (trim, lowercase) identically for inserts and lookups.

## Trade-offs
- **No deletes**: a released username stays "maybe taken" (costing a DB query,
  never a wrong answer) until the filter is rebuilt. A counting Bloom filter
  or a cuckoo filter supports deletion at 3–4× the memory.
- Each instance has its own copy. Names registered on *other* instances are
  missing until they sync (via an event stream or periodic rebuild from a
  snapshot). Until then the filter can say "available" for a name that was
  just taken; the unique index on sign-up still prevents duplicates, and the
  form shows the conflict then. If that is unacceptable, share one filter
  (e.g. RedisBloom) at the cost of a network hop.
- Size for growth: capacity must exceed the number of usernames you expect
  before the next rebuild, or the FP rate (and DB load) climbs fast.
- Bloom filters reveal membership probabilistically. Don't ship one for
  sensitive sets (e.g. email addresses) to clients.

## Run
```bash
python -m unittest -v test_solution.py
```
