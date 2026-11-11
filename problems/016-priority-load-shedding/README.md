# 016 · During the flash sale half of all checkouts failed with 503, while analytics pings kept getting through

## Problem
An e-commerce API gateway sits in front of the order, search and telemetry
services. At the start of a flash sale traffic doubled: about 200 requests per
tick hit a gateway that could serve 100. The gateway did what it was built to
do and rejected requests with `503` once it was full.

Customers could browse but around half of all "Place order" clicks failed,
and abandoned carts spiked. Meanwhile the dashboards showed analytics beacons
and prefetch requests — which nobody would have missed — being served at the
same ~50% rate as checkout.

## Root cause
Shedding was **priority-blind**. The gateway admitted whatever arrived while a
slot was free. Arrival order is effectively random, so every class of traffic
lost the same share: in the simulation, checkout, browsing and analytics all
succeed about 50% of the time. Under overload the system spent half its
capacity on requests that had no business value.

## Solution
**Priority-aware admission control** (`PriorityGateway`):

- Each request carries a priority: `CRITICAL` (checkout, payments), `DEFAULT`
  (browsing, search) or `BACKGROUND` (analytics, prefetch).
- Each priority has a utilisation ceiling. A request is admitted only while
  the gateway's total in-flight count is below its priority's ceiling:
  background up to 60%, default up to 70%, critical up to 100%.
- So as load rises, background traffic is shed first, then default, and the
  top 30% of capacity can only ever be used by critical traffic.

Simulation results (capacity 100, 200 ticks, fixed seed):

| Traffic | Gateway | Checkout | Browse | Analytics |
|---|---|---|---|---|
| Overload: 30 / 70 / 100 per tick | Naive | 50% | 50% | 50% |
| Overload: 30 / 70 / 100 per tick | Priority | **100%** | 40% | 30% |
| Normal: 10 / 30 / 20 per tick | Priority | 100% | 100% | 100% |

## Trade-offs
- Ceilings apply to **total** utilisation, so lower priorities are shed
  before the gateway is completely full. With 80 requests per tick
  (10 / 40 / 30) the priority gateway already sheds about 25% of analytics and
  5% of browsing, even though 100 slots exist. That is the price of keeping
  the reserve free for checkout.
- During the overload run the priority gateway averages about 88 admitted
  requests per tick instead of 100: critical requests that arrive early count
  towards the lower classes' ceilings. A reserve sized larger than real
  critical demand wastes capacity; one sized smaller lets checkout fail again.
  Size it from measured peak critical traffic.
- Ceilings are whole percentages and slot limits use integer maths
  (`capacity * percent // 100`), so 70% of 90 is 63 slots, not the 62 that
  `int(90 * 0.7)` gives. On tiny gateways every class still gets at least one
  slot (70% of 1 slot would round to 0 and shed browsing on an idle box), so a
  single-slot gateway has no reserve left for critical traffic.
- Priorities must be set by the gateway (by route), not trusted from client
  headers, or every client will mark itself critical.
- Static ceilings do not adapt. Production systems (for example Google's
  "criticality" in its SRE book, or Netflix's adaptive concurrency limits)
  move the thresholds based on measured latency or CPU.
- Shedding at the edge is only half the story: retries from shed clients add
  load, so return `Retry-After` and use client backoff (problem 001 in devops-cloud-solutions).

## Run
```bash
python -m unittest -v test_solution.py
```
