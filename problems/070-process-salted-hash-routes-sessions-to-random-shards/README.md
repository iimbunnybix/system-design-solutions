# 070 · After moving to multi-worker app servers, logged-in users were bounced to the login page on most clicks

## Problem
A web app keeps login sessions in a cache cluster of 8 shards. The session
service picked a shard with `hash(session_id) % 8`. It had always run as one
process per container, and every test passed.

To use the CPUs better, the team switched each container to gunicorn with
several worker processes. Right after the deploy, users started being sent to
the login page on most clicks. Logging in again worked, and the next page or
two usually failed again. Cache memory grew because every login wrote
another copy of the session, while the hit rate for sessions fell to about
one in eight. The support queue filled with "I keep getting logged out".

In the model (`test_solution.py`, 2,000 sessions, two worker processes with
hash seeds 1 and 2), the two workers sent only **13.65%** of sessions to the
same shard, and **1,727 of 2,000 users** (86%) were logged out when their
next request reached the other worker.

## Root cause
**Python's built-in `hash()` of a string is salted per process.** Since
Python 3.3, every interpreter picks a random `PYTHONHASHSEED` at start-up so
attackers cannot craft keys that all collide in a dict (hash flooding). The
result is stable inside one process and different in every other one. With
a single process per container the bug was hidden for sessions created and
read by the same process, which every test was. With several workers (and
several containers), each process routes the same session id to its own,
effectively random, shard: two workers agree only about 1 time in 8.

## Solution
`stable_shard_for` routes with a hash defined only by the key's bytes: the
first 8 bytes of SHA-256 of the UTF-8 session id, modulo the shard count.
Every process, host, Python version and deploy computes the same shard, so a
session written by one worker is found by all the others (agreement 100%,
0 logouts in the same simulation).

The tests start real worker processes with different `PYTHONHASHSEED`
values (`routes_in_new_process`), because the seed is fixed when the
interpreter starts and cannot be changed from inside a running test. They
also pin one key's shard, so an accidental change of the routing function,
which would move almost every session at once, fails loudly.

## Trade-offs
- **Modulo routing still reshuffles on resize.** Changing 8 shards to 9 moves
  most keys even with a stable hash; use consistent hashing (problem 001) if
  shards are added often.
- **SHA-256 is more than needed.** A non-cryptographic hash (xxHash,
  MurmurHash, CRC32) is faster, but needs a dependency or is weaker at
  spreading keys (CRC32). For routing, the cost of SHA-256 on a short key is
  small next to the network call that follows.
- **Setting `PYTHONHASHSEED=0` everywhere also "works"** but turns off the
  hash-flooding protection for the whole process and breaks again as soon as
  one service (or a non-Python client) routes differently. Routing must be a
  documented function of the key, shared by every client.
- The same bug appears anywhere `hash()` leaves the process: cache keys,
  partitioning files in a batch job, A/B test bucketing, or sharding in a
  Celery task.

## Run
```bash
python -m unittest -v test_solution.py
```
