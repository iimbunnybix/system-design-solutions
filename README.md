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
| 007 | [One viral post made "like" requests time out, and the whole site slowed down with it](problems/007-hot-row-sharded-counter) |
| 008 | [The "is this username available?" check became the biggest source of database load](problems/008-bloom-filter-username-check) |
| 009 | [Basic-plan API keys got 2,000 requests a minute instead of 100, and the first fix throttled paying customers](problems/009-distributed-rate-limit-shared-store) |
| 010 | [Product search got slower with every catalog import, and "shoes running" returned nothing](problems/010-inverted-index-product-search) |
| 011 | [Whenever a celebrity posted, everyone's home feed froze for minutes and new posts from friends showed up late](problems/011-celebrity-timeline-fanout) |
| 012 | ["Finding drivers near you" took longer every month as the fleet grew, and the matching service fell over on Friday nights](problems/012-geohash-nearby-drivers) |
| 013 | [Products showed "out of stock" while the warehouse shelves were full, and some customers were charged for orders that never shipped](problems/013-saga-compensation-stuck-inventory) |
| 014 | [Users' saved settings flipped back to old values at random, and got much worse after a storage node came back from maintenance](problems/014-quorum-reads-stale-replica) |
| 015 | [Two replicas quietly disagreed for weeks, and the nightly repair job that should have fixed it ran for hours and saturated the network](problems/015-merkle-tree-replica-repair) |
| 016 | [During the flash sale half of all checkouts failed with 503, while analytics pings kept getting through](problems/016-priority-load-shedding) |
| 017 | [One database shard pegged at 100% CPU while the other seven idled, and event ingestion lagged by hours](problems/017-timestamp-key-hot-shard) |
| 018 | [Reminder notifications started arriving minutes late, and got later every week as the queue grew](problems/018-delayed-jobs-timing-wheel) |
| 019 | [A storage node lost power and then crash-looped on restart, and after a manual fix one account showed a wrong balance](problems/019-torn-write-log-recovery) |
| 020 | [The database cluster failed over dozens of times a day, and each time the "dead" primary turned out to be healthy](problems/020-phi-accrual-failure-detector) |
| 021 | [Two customers signed up with the same username after the user table was sharded](problems/021-cross-shard-unique-username) |
| 022 | [Users' profile edits silently reverted, but only when saved through one data center](problems/022-lww-clock-skew-hybrid-logical-clock) |
| 023 | [After a switch failure, two nodes both claimed to be leader and customer balances diverged](problems/023-split-brain-majority-leader-election) |
| 024 | [Search pages took half a second at p99 even though every shard answered in 10 ms on average](problems/024-hedged-requests-tail-latency) |
| 025 | [Customers got three "your video is ready" emails, and the transcoding queue never emptied](problems/025-visibility-timeout-heartbeat-redelivery) |
| 026 | [After a shard split, some users' profile changes "saved" successfully and then reverted](problems/026-stale-shard-map-misrouted-writes) |
| 027 | [A three-second database slowdown turned into a full outage that only ended when traffic was cut](problems/027-layered-retry-amplification) |
| 028 | [A 1,000-unit flash deal sold 1,250 units, and 250 customers got "sorry, cancelled" emails after paying](problems/028-flash-deal-oversell-escrow) |
| 029 | [One product went viral and a quarter of all page loads timed out on a single cache node, while the other nine sat idle](problems/029-hot-cache-key-replication) |
| 030 | [Fraud scoring was OOM-killed 16 seconds into a sale, and 16,400 orders were never checked](problems/030-credit-based-flow-control) |
| 031 | [The "newest listings" feed ended after 13 pages, and sellers said most of their listings never showed up](problems/031-scatter-gather-pagination-merge) |
| 032 | [Merchants changed a price, and shoppers kept seeing the old one for exactly 10 minutes even though the cache had been cleared](problems/032-cache-fill-from-lagging-replica) |
| 033 | [Messages users had deleted reappeared in their chats after a database node came back from a three-day repair](problems/033-tombstone-gc-zombie-resurrection) |
| 034 | [One tenant's month-end dashboard refresh timed out every query on one worker while nine others sat half idle](problems/034-bounded-load-consistent-hashing) |
| 035 | [One customer uploaded 2,000 reports and every other customer's export sat "queued" for over a minute and a half](problems/035-noisy-tenant-fifo-queue-fair-scheduling) |
| 036 | [Order events piled up for seconds at the lunch peak, though the log server's CPU and disk bandwidth were nearly idle](problems/036-per-write-fsync-group-commit) |
| 037 | [The weekly-active-users dashboard showed 7x more users than had signed up, and it grew with every shard added](problems/037-summed-shard-distinct-counts-hyperloglog) |
| 038 | [Large video uploads from phones kept restarting at 0% and most never finished, while users' mobile data ran out](problems/038-flaky-network-upload-restarts-from-zero) |
| 039 | [Customers saw "payment received" on orders that had already shipped, and warehouse dashboards showed paid orders stuck for days](problems/039-round-robin-partitions-reorder-order-events) |
| 040 | [Every night at 2 a.m. product pages slowed to a crawl and the database hit 100% CPU, right when the analytics export started](problems/040-nightly-scan-flushes-lru-cache) |
| 041 | [After the metadata leader crashed, the cluster sat with no leader for minutes while every node kept starting elections and logs showed the term climbing](problems/041-identical-election-timeouts-split-votes) |
| 042 | [The hourly ledger audit kept reporting thousands of dollars created or lost, but every account balanced when checked by hand](problems/042-cross-shard-audit-total-drifts-snapshot-reads) |
| 043 | [After a rack switch failed, customers on one rack kept seeing their old balance for 20 minutes even though every write was committed](problems/043-partitioned-leader-serves-stale-reads) |
| 044 | [After the rebalancer moved a busy shard to a new server, users found profile edits from the last few minutes undone and some new accounts gone](problems/044-shard-move-loses-writes-during-copy) |
| 045 | [After the cart cache restarted, shoppers' recently added items vanished even though every "add to cart" had returned success](problems/045-write-behind-cache-loses-acknowledged-writes) |
| 046 | [Minutes after a new API node joined, it fell over under every client's traffic while the old nodes sat idle](problems/046-least-loaded-balancer-herds-onto-new-node) |
| 047 | [The team tripled the consumers during a 3.6-million-message backlog, and the backlog drained no faster](problems/047-consumers-beyond-partition-count-sit-idle) |
| 048 | [After a big enterprise customer went live, one web server ran at six times its share and timed out while the other seven idled](problems/048-nat-source-ip-affinity-overloads-one-backend) |
<!-- INDEX:END -->

## Run the tests

```bash
python run_tests.py                       # all problems
cd problems/<folder> && python -m unittest -v
```

Requires Python 3.10+. No third-party packages.

## Author

Hrishikesh Jadhav
