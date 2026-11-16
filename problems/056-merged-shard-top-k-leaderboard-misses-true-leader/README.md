# 056 · The season leaderboard showed the wrong top 10, and every total on it was too low

## Problem
A game stores match results in 16 shards, partitioned by match ID, so each
player's season points are spread across every shard they played on. The
global top-10 leaderboard asked each shard for its local top 10 by partial
score and summed those lists. Players complained that their totals on the
leaderboard were lower than the sum on their profile page, and that
consistently strong players were missing entirely.

In the seeded test season (20,000 players, 16 shards) the merged leaderboard
leaves out 4 of the true top 10 (the true 5th, 6th, 7th and 10th), shows the
true 3rd place in 9th, and gets **none** of the ten totals right: the leader
is shown with 913 points instead of 1,258.

## Root cause
"Top-k of the sums" is not "sum of the top-k". A player who is good on every
shard but best on none never appears in any local list, and a player who
appears on some shards' lists has the points from the other shards dropped.
The tiny case in the tests: Sam scores 30 on each of three shards (90 in
total), but every shard has two players above 30, so the merged top 2 is two
players with 50 each and Sam is invisible.

## Solution
`tput_top_k` implements TPUT, the Three-Phase Uniform Threshold algorithm
(Cao & Wang, 2004). It is exact for non-negative scores and only fetches
what it can prove matters:

1. **Phase 1** – fetch each shard's local top-k and sum them. These are lower
   bounds; the k-th best is a threshold `T` that the final top-k must reach.
2. **Phase 2** – a total of at least `T` over `m` shards needs at least one
   shard to hold `T / m` of it (pigeonhole). Fetch every partial score
   `>= T / m`. A shard that did not report a player holds less than `T / m`
   for them, which gives an upper bound per player; players whose upper
   bound is below the k-th best lower bound are dropped.
3. **Phase 3** – fetch exact scores for the remaining candidates from every
   shard and rank them.

In the test season TPUT returns the same top 10 as a full scan while
shards send 15,715 entries instead of 168,906 (about 9%). It also matches
the full scan on 300 small random seasons with many ties.

## Trade-offs
- Three round trips instead of one, and the savings depend on the data: if
  scores are flat, `T / m` is low and phase 2 returns most entries. Measure on
  your own distribution.
- It needs non-negative scores (the bounds assume a missing score is at
  least 0); `ScoreShard` rejects negative ones.
- If the leaderboard is read far more often than scores change, it is
  simpler to keep a per-player total, sharded by player ID, updated on each
  match result and served from a sorted set. That moves the cost to writes
  and makes the top-k a single lookup.
- Asking each shard for more than k entries (say 10k) makes the naive merge
  less wrong but never correct; there is no safe fixed over-fetch factor.

## Run
```bash
python -m unittest -v test_solution.py
```
