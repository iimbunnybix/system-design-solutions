# System Design Solutions

Real scaling problems from distributed systems (sharding, ID generation, queues, consistency), each with root cause, a working Python implementation and tests.

Every problem folder has the same structure:

| File | Contents |
|---|---|
| `README.md` | The incident or requirement, root cause, solution and trade-offs |
| `solution.py` | Working implementation (Python standard library only) |
| `test_solution.py` | Tests that reproduce the problem and prove the fix |

## Problems

<!-- INDEX:START -->
| # | Problem |
|---|---|
| 001 | [Adding one cache server during a sale emptied the whole cache and took down the DB](problems/001-consistent-hashing-cache-scaling) |
| 002 | [URL shortener: random short codes started colliding, and the DB sequence became a bottleneck](problems/002-url-shortener-id-generation) |
| 003 | [Database CPU spikes to 100% every few minutes, exactly when the homepage cache entry expires](problems/003-cache-stampede-single-flight) |
| 004 | [Two schedulers both thought they were leader and charged the same subscriptions twice](problems/004-lease-fencing-tokens) |
| 005 | [Users saved their profile, the page reloaded with the old value, and some edits seemed to vanish](problems/005-read-your-writes-replica-lag) |
| 006 | [Items customers added to their cart on one device disappeared when they checked out on another](problems/006-multi-region-cart-crdt) |
<!-- INDEX:END -->

## Run the tests

```bash
python run_tests.py                       # all problems
cd problems/<folder> && python -m unittest -v
```

Requires Python 3.10+. No third-party packages.

## Author

Hrishikesh Jadhav
