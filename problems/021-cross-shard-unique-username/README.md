# 021 · Two customers signed up with the same username after the user table was sharded

## Problem
A social app moved its `users` table from one PostgreSQL instance to 16
shards, placing each user on shard `user_id % 16`. Each shard kept the old
`UNIQUE(username)` index. Weeks later support found pairs of accounts with
identical usernames: mentions (`@alice`) went to the wrong person, password
reset by username picked an arbitrary account, and a creator's handle was
taken over by an impersonator who happened to land on a different shard.
Nothing failed loudly; every insert succeeded.

## Root cause
A unique index only sees the rows on its own shard. Users are placed by
`user_id`, so two sign-ups for "alice" usually go to different shards and
neither index sees the other row. `test_same_username_on_two_shards_is_accepted_twice`
reproduces this; a duplicate is only caught when both users happen to hash
to the same shard.

## Solution
Add a **global username index partitioned by a hash of the normalized
username** (`sha256(name) % partitions`). All claims for one name meet in one
partition, which can enforce uniqueness atomically. Sign-up becomes:

1. **Reserve** the name in its index partition (state `pending`).
2. **Create** the user row on its user shard.
3. **Confirm** the reservation; if step 2 fails, **release** it.

Crash handling:
- A `pending` claim younger than `RESERVATION_TTL_SECONDS` (60 s) blocks
  everyone else; after that it can be reclaimed.
- Before reclaiming, the index asks the owner's user shard whether that user
  was in fact created (crash between create and confirm). If so the claim is
  confirmed instead of being handed to someone else.
- The same user retrying keeps its claim, so client retries are safe.

Names are normalized with `strip().casefold()` before hashing so "Alice" and
" alice" collide. In the tests, 50 concurrent sign-ups for one name from 50
different users produce exactly one account.

## Trade-offs
- Sign-up now touches two places, so it is slower and the index partition
  is a dependency of sign-up. Reads by user_id are unaffected.
- Reclaiming waits for the TTL, so a crashed sign-up keeps the name busy for
  up to 60 s.
- The recovery check calls a user shard while holding the partition lock;
  at high volume you would do it outside the lock with a compare-and-set.
- Renames need the same reserve/confirm dance plus releasing the old name.
- Alternatives: shard users by username instead (then lookups by user_id
  need their own index), or use a database with global secondary unique
  indexes (Spanner, CockroachDB), which do this two-place write for you.
- Fuller Unicode normalization (NFKC, confusables) is a separate concern.

## Run
```bash
python -m unittest -v test_solution.py
```
