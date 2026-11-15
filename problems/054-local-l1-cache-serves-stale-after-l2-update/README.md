# 054 · After a price drop, customers saw the old price on some page loads and the new one on others for five minutes

## Problem
A storefront runs 40 API pods. Each pod keeps an in-process (L1) cache of
product prices for 5 minutes in front of a shared Redis/database (L2), which
took most reads off Redis. When merchandising cut a price, the admin
request landed on one pod, which wrote the store and deleted the key from
*its own* L1. The other 39 pods kept the old price. Because the load balancer
spreads requests, a shopper refreshing the page flipped between prices, and
the cart (priced from a different pod) disagreed with the product page.
Support tickets and manual refunds followed every flash sale.

## Root cause
An in-process cache is private to one process, so a delete only reaches the
pod that handled the write. Nothing told the other pods, so their copies
stayed valid until the TTL: in the test, the reader serves the old price at
299 s and only picks up the new one at 300 s.

## Solution
Every write is **broadcast on an invalidation bus** (Redis pub/sub, Kafka, NATS)
that all pods subscribe to:

- `update` writes the store, then publishes `(key, version)`. Every pod drops
  its L1 copy of that key, so all pods serve the new price immediately.
- Entries remember the store version they were read at. An event only evicts
  a copy **older** than its version, so a delayed message about an old write
  does not throw away a newer value (no needless reload storm).
- Pub/sub delivery is best effort, so each event carries a **sequence
  number**. A pod that sees a gap (it was disconnected or restarted its
  subscriber) cannot know which keys it missed and **clears its whole L1**.
- The 5-minute TTL stays as a backstop for anything else that goes wrong.

## Trade-offs
- A lost message is only noticed when the next event arrives. On a quiet
  bus the pod stays stale until then (a test pins this); publishing a
  heartbeat event every few seconds bounds that window.
- Clearing all of L1 after a gap sends a burst of reads to the store; with
  many pods reconnecting at once (bus failover) that is a mini stampede.
- The write and the publish are not atomic: a crash between them skips the
  broadcast. Publishing from the database change log (CDC) or an outbox
  closes that gap at the cost of another pipeline.
- This does not fix the read/fill race where a slow read stores an old value
  after an invalidation (see backend-api-solutions 011); combine both for
  strict freshness.
- Simpler alternatives: a much shorter L1 TTL (a few seconds), or no L1 for
  data where stale reads cost money, accepting more L2 traffic.

## Run
```bash
python -m unittest -v test_solution.py
```
