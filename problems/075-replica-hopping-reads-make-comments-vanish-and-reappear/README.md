# 075 · Readers saw a new comment, refreshed and it was gone, refreshed again and it was back

## Problem
A news site's comments service serves about 40,000 thread reads a second
from three asynchronous PostgreSQL read replicas. The read router picks a
replica at random for every request; writes go to the primary. During a
busy evening one replica fell behind by a few seconds because of a long
analytics query, and another by about one second.

Readers on breaking-news threads started reporting that comments were
"being deleted": a comment appeared, the next refresh did not show it, and
the refresh after that showed it again. Moderators were accused of
censoring and then un-censoring posts, social media picked it up, and the
trust-and-safety team spent the night answering tickets about deletions
that never happened.

## Root cause
**Every page load could be served by a different replica, and the replicas
are at different points in the log.** A replica that has applied LSN 1,000
shows comment `c4` (committed at LSN 990); one at LSN 900 does not. A
random router happily sends refresh 1 to the fresh replica and refresh 2 to
the stale one, so time appears to run backwards for the reader. This is a
**monotonic-reads** violation.

It is not a read-your-writes problem (problem 005 handles a user's own
writes): the readers here never wrote anything, so "send a user to the
primary after they write" does not help. It is also not a quorum problem
(problem 014): each read is answered by one server that is simply behind.

In the model (replicas at LSN 1,000, 960 and 900; 1,000 readers each
refreshing 10 times) the random router made a comment vanish on **2,997**
refreshes, and **99.9%** of readers saw at least one flicker.

## Solution
**A session token with the highest position the reader has seen.**

1. Every response includes the applied LSN of the server that answered (in
   PostgreSQL, `pg_last_wal_replay_lsn()` on the replica). The client keeps
   the highest one in a cookie or header (`SessionToken`).
2. The router only considers replicas whose applied LSN is at least the
   token (`choose_server`), choosing randomly among them so load still
   spreads.
3. If no replica is fresh enough (the fresh one crashed or was removed), the
   read goes to the primary, which has always applied everything.
4. After the response the token becomes `max(token, server LSN)`, so it
   never moves backwards.

A replica that catches up becomes eligible again. In the same model, with
the token, **0** refreshes lost a comment and **0%** of readers saw a
flicker, and every read was still served by a replica.

## Trade-offs
- **Load drifts toward the freshest replica.** Once a reader has seen LSN
  1,000, only replicas at 1,000 qualify. In the model (where lag is frozen)
  replica-a served **8,509** of 10,000 reads with the token versus **3,419**
  without. In production replicas keep catching up, so this is much milder,
  but alert on replica lag and on the primary-fallback rate.
- **Waiting instead of falling back:** a variant waits a few milliseconds for
  a replica to reach the token before using the primary, trading latency for
  primary load.
- The guarantee is **per session**. Two different readers (or one reader on
  a new device with no token) can still see different states; that is
  eventual consistency, and it is fine for comments.
- **Sticky sessions** (pin each reader to one replica) also give monotonic
  reads and need no token, but break when that replica fails or is drained,
  and balance load poorly with few heavy readers.
- The token is a plain number, not a secret. Clients can tamper with it, so
  cap it at the primary's current LSN before using it.

## Run
```bash
python -m unittest -v test_solution.py
```
