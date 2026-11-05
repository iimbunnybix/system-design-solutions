# 006 · Items customers added to their cart on one device disappeared when they checked out on another

## Problem
An e-commerce site runs active-active in two regions. Each region accepts cart
writes locally and replicates to the other within about a second. The mobile
app and the website are often routed to different regions. Customers began
reporting that an item added on their phone was missing when they checked out
on their laptop a few minutes later. It happened most during sales, when
people add items quickly from several devices, and it showed up as a measurable
drop in items per order on promotion days.

## Root cause
Carts were replicated as **whole documents with last-write-wins (LWW)** by
wall-clock timestamp. Two edits made in different regions within one
replication window are *concurrent*: neither saw the other. LWW keeps one
cart and throws the other away, along with any item only that copy had.

Clock skew makes it worse: if region A's clock is behind, its *later* edit
carries an *earlier* timestamp and loses. Both cases are reproduced in
`ReproduceLww`.

## Solution
Model the cart as a **CRDT**: an Observed-Remove Set (`ORSetCart`).

- **Every add gets a unique tag** `(replica_id, incarnation, counter)`, with
  a quantity. The incarnation is a random id chosen at every process start.
- **Remove only tombstones the tags this replica has observed**, so it can't
  delete an add it never saw.
- **Merge is set union** of adds and tombstones. That makes it commutative,
  associative and idempotent: replicas can gossip in any order, repeat
  messages, or merge partial states, and still converge. The test runs 30
  seeded random histories over 3 regions and checks that all 6 merge orders
  give the same cart.
- **Add wins** over a concurrent remove. For a cart, an unwanted item is easy
  to remove at checkout; a silently lost item is lost revenue.
- No wall clocks are involved, so clock skew cannot pick a winner.
- Tags do not depend on persisted counters. A replica restored from a stale
  backup would otherwise reissue a counter it had already replicated, and one
  tombstone would then remove two different items. With a fresh incarnation
  per start, that cannot happen (tested with a stale-backup restore).

## Trade-offs
- **Bigger tags.** The incarnation id adds bytes to every tag; a 64-bit
  random id or a durable boot counter would do if size matters.
- **Metadata grows.** Every add and remove leaves tags and tombstones.
  `compact()` drops removed adds, but tombstones can only be garbage-collected
  once every replica has seen them (track that with version vectors), or by
  resetting the cart at checkout, which is usually enough for carts.
- **Quantity semantics.** Concurrent "+1" on two devices both count (good). Two
  concurrent "set quantity" edits are also both kept and sum (3 and 2 give 5),
  which is surprising. Show the merged quantity and let the user fix it, or
  model quantity with a different CRDT (e.g. a multi-value register that
  surfaces the conflict).
- Add-wins can resurrect an item the user removed on one device while adding
  it on another at the same moment. That is the intended bias here, but a
  wishlist or a permissions list might want remove-wins.
- CRDTs fit data whose merge rule is known in advance. Inventory reservation
  and payments need a single writer or consensus. Do not use this for stock counts.
- Riak, Redis Enterprise (CRDB) and Azure Cosmos DB offer CRDT-style types;
  Amazon's Dynamo paper describes the original cart-merge problem.

## Run
```bash
python -m unittest -v test_solution.py
```
