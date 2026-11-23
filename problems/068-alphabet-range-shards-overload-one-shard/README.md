# 068 · The "s-z" user shard ran out of disk while the other three were half empty

## Problem
A community app stores user profiles in a range-sharded table, keyed by
username. Range sharding was chosen on purpose: the "find people" box does
prefix searches (`sam*`), and with ranges a prefix lives on one shard
instead of being scattered by a hash. The four shards were given equal
slices of the alphabet: `a-f`, `g-l`, `m-r` and `s-z`.

Within a year the `s-z` shard was the first to hit its disk alarm and had
the highest CPU and replication lag, while the others had plenty of room.
Profile pages for users on it were the slowest in the app, and the database
team could only scale by buying a bigger machine for one shard.

## Root cause
**Equal slices of the alphabet are not equal slices of the data.**
Usernames do not start uniformly: some initials (in this app, "s") are far
more common than others ("q", "x", "u"). The boundaries `g`, `m`, `s` were
picked from the key space, not from the keys (`naive_split_points`).

In the test's synthetic population (40,000 usernames, with first-letter
weights in `test_solution.py` where "s" is the most common initial), the
alphabet split gives shards of **9,473 / 7,794 / 7,867 / 14,866** users:
the `s-z` shard holds 37% of all users, **1.49x** the average.

## Solution
Keep range sharding, but **choose boundaries from the data**:

1. `split_points_from_sample` sorts a sample of real keys (2,000 here) and
   takes the keys at the 25%, 50% and 75% positions as boundaries. Duplicate
   boundaries are dropped so no range is empty forever.
2. `shard_for` routes by the **whole lower-cased key** with `bisect_right`,
   so a boundary key belongs to the right-hand range, prefix neighbours
   (`sam0001`, `sam0002`) stay together, and case does not matter.
3. `split_hot_shard` handles growth: when the largest shard exceeds
   `HOT_SHARD_RATIO` (1.3x the average), it adds one boundary at that
   shard's median. Only that shard's keys move; every other key keeps its
   shard.

With boundaries from a sample drawn independently of the 40,000 users, the
shards hold **9,160 / 10,341 / 9,743 / 10,756** users (largest 1.08x the
average). Applied to the original alphabet split, `split_hot_shard` cuts
`s-z` at its median into two shards of 7,433 each (largest shard now 1.18x
the average across five shards).

## Trade-offs
- Boundaries are only as good as the sample. If the key distribution
  shifts (a marketing campaign brings thousands of `promo_*` users), ranges
  drift out of balance again; keep measuring shard sizes and split hot
  ranges, as `split_hot_shard` does, rather than computing boundaries once.
- Splitting moves data. Moving half a shard while it serves traffic needs
  a copy-then-cutover procedure (see problem 044 on losing writes during a
  shard move) and a shard map that clients refresh (problem 026).
- Even key counts are not even load. A shard can hold a quarter of the
  users and still be hot because of one celebrity account; size-based
  splitting does not fix hot keys (see problem 029 for replicating hot keys).
- Hash sharding spreads keys evenly without any of this, but turns every
  prefix search into a query to all shards. Range sharding with sampled
  boundaries is what Bigtable, HBase and CockroachDB-style systems do, with
  automatic splits when ranges grow.

## Run
```bash
python -m unittest -v test_solution.py
```
