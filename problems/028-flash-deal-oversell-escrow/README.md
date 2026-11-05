# 028 · A 1,000-unit flash deal sold 1,250 units, and 250 customers got "sorry, cancelled" emails after paying

## Problem
A retailer ran a limited flash deal: 1,000 discounted headphones, sold from
three regions (EU, US, AP) that each serve their local customers. Each region
keeps a replicated copy of the deal's "units sold" counter. Before selling,
a region checks `sold < 1000` against its local copy, sells, and increments
its own count, which then replicates to the other regions. Cross-region
replication normally takes a few hundred milliseconds.

The deal drew about 1,000 buy attempts per second (60% US, 30% EU, 10% AP)
and sold out in about a second. The warehouse only had 1,000 units, but the
order system had taken payment for 1,250. Support had to cancel and refund
250 orders, and the "deal" made the news for the wrong reasons.

## Root cause
**Each region decided based on a stale view of the others' sales.** With
500 ms of replication lag, each region sees its own sales at once but the
other two regions' sales as they were half a second ago. In the last half
second before the true count reaches 1,000, every region still sees stock
and keeps selling. The total oversell grows with lag × sales rate.

The simulation (`simulate_replicated_check`, 100 ms ticks, 30 / 60 / 10 buy
attempts per tick in EU / US / AP) gives:

| Replication lag | Units sold | Oversold |
|---|---|---|
| 200 ms | 1,080 | 80 |
| 300 ms | 1,140 | 140 |
| 500 ms | 1,250 | 250 |
| 1 s | 1,540 | 540 |

A "check then write" on a replicated counter cannot enforce a global limit:
the check and the other regions' writes are not ordered. Adding more
replicas or faster replication only shrinks the window.

## Solution
**Escrow (allocation).** Instead of sharing one counter, split the units
between the regions. Each region may sell only the units it holds:

- `EscrowCoordinator` starts by splitting 1,000 units into 334 / 333 / 333.
- `RegionAllocation.try_take_local` is an atomic local decrement. A sale needs
  no cross-region call and no replicated read.
- When a region runs out, it asks the coordinator for up to 50 more units
  (`request_refill`). The coordinator gives units from its pool. If the pool
  is empty, it asks every other region to give back half of its unsold units
  (rounded up, so even the last unit can move).
- Units are only ever **moved** between a region and the coordinator, under
  locks, never copied. So `sold + unsold allocations + pool == 1000` at all
  times, and no interleaving can sell more than 1,000. The tests check this
  with 50 seeded random histories and with 6 concurrent buyer threads.
- When a reclaim finds nothing anywhere, the coordinator marks the deal sold
  out, and regions answer "sold out" locally from then on.

Same demand, same 100 ms ticks:

| Design | Units sold | Oversold | Coordinator calls |
|---|---|---|---|
| Replicated check, 500 ms lag | 1,250 | 250 | none |
| Static split, no rebalancing | 967 | 0 | none |
| Escrow with rebalancing | 1,000 | 0 | 14 |

A static split is safe but wasteful: US buyers are turned away after 333
units while 33 units sit unsold in AP. With rebalancing, the deal sells out
in 10 ticks (one second), the units go where the demand is (EU 300, US 600,
AP 100), and only 14 of the about 1,000 sales attempts in that second needed
the coordinator (13 refills plus the call that detected the sell-out).

## Trade-offs
- **The coordinator is a single point of coordination.** It is called only
  when a region runs out, so it is off the hot path, but while it is down
  regions can only sell what they already hold. That fails safe (no
  oversell), at the cost of possible early "sold out" in a busy region. In
  production the coordinator would be a strongly consistent store (a
  single-leader database row, or a consensus-backed service) and allocations
  would be persisted in each region before they are used.
- **Refill latency.** A region that runs dry waits for a cross-region round
  trip. Larger refill chunks mean fewer calls but more units sitting in a
  quiet region; asking for more before running out (a low-water mark) hides
  the latency.
- **Not exactly first-come, first-served across regions.** A late buyer in a
  region with units left can succeed while an earlier buyer elsewhere is told
  "sold out". For a flash deal this is usually acceptable.
- **Lost allocations.** If a region crashes, units allocated to it are stuck
  until it recovers or the coordinator revokes its lease on them. Use
  time-limited allocations for that.
- **Alternatives:** route every sale for the deal to one home region
  (simplest and exact, but adds latency for other regions and makes that
  region a hot spot); a consensus-backed counter (exact, but a cross-region
  round trip per sale); or sell with oversell allowed and cancel later (what
  the incident did by accident). Problem 009 in this repo covers the related
  case of per-node rate-limit counters overshooting a shared limit.

## Run
```bash
python -m unittest -v test_solution.py
```
