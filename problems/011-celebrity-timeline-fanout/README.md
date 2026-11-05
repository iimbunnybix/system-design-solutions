# 011 · Whenever a celebrity posted, everyone's home feed froze for minutes and new posts from friends showed up late

## Problem
A social app builds home timelines with fan-out on write: when someone
posts, a worker inserts the post id into the cached timeline of each of their
followers, so reading a feed is one cache lookup. This worked until a few
accounts grew to millions of followers. Each of their posts enqueued millions
of timeline writes; the fan-out workers fell minutes behind, so *every* user's
posts (including friends' posts with 200 followers) appeared late, the timeline
cache cluster's write load spiked, and "why don't I see new posts?" became the
top support complaint during live events, exactly when engagement mattered most.

## Root cause
**Fan-out on write costs one write per follower, and follower counts are
heavily skewed.** Write cost per post is proportional to the author's follower
count, so a handful of accounts produce most of the fan-out work, and they share
one queue with everyone else.

In the reproduction, one post by an account with 50,000 followers costs
**50,001 writes** (its own outbox plus every follower's inbox). Ten such posts
cost 500,010 writes, 50 times the 10,010 writes for ten posts by a regular
author with 1,000 followers. At millions of followers the same arithmetic gives
millions of writes per post.

## Solution
`HybridFeed` in `solution.py`, the hybrid push/pull model large social
networks use:

1. **Push for regular authors.** Below `celebrity_threshold` followers
   (10,000 by default) posts are fanned out to follower inboxes as before, so
   most reads stay a single lookup.
2. **Pull for celebrities.** Their posts are written once, to their own
   outbox (**1 write** instead of 50,001 in the test).
3. **Merge at read time.** A timeline read takes the user's inbox plus the
   outboxes of the celebrities they follow (usually a few), k-way merges them
   newest-first by `(timestamp, post_id)`, and stops after `limit` posts.
   Reading `u5`'s feed costs 2 list reads: the inbox and one celebrity outbox.
4. **Correctness details:** posts pushed before an author crossed the
   threshold are deduplicated by id; posts from unfollowed authors are filtered
   out; inboxes are bounded (`inbox_limit`). An author whose posts were ever
   *not* pushed keeps being pulled at read time even after dropping below the
   threshold, so one unfollow cannot make those posts vanish from the other
   followers' timelines.

A randomized test (203 authors including 3 celebrities, 2,000 posts with
frequent timestamp ties) checks that 30 users' timelines equal a brute-force
"all posts by everyone I follow, newest first" reference.

## Trade-offs
- **Reads get more expensive** for users who follow many celebrities. Cap
  the number of pulled sources, or cache each celebrity's recent posts
  (they are read by everyone, so they cache very well).
- The threshold is a tuning knob; it can also depend on activity (inactive
  followers need not be pushed to, and their feed can be rebuilt on login).
- Feeds become **eventually consistent** in two ways (async push vs.
  read-time pull), so a post can appear in slightly different positions on
  refresh. Ranked feeds mostly hide this.
- Fan-out on read for *everyone* avoids write amplification entirely but
  makes every read a large merge; it suits low read-to-write ratios.
- Separate the fan-out queue by author tier, so even pushed bursts from
  large-but-not-celebrity accounts cannot delay small authors.

## Run
```bash
python -m unittest -v test_solution.py
```
