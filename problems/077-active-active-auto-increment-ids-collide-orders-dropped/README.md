# 077 · After going active-active across two regions, about half of all new orders disappeared from one region or the other

## Problem
A food-delivery company moved its orders database from one primary with a
standby to two primaries, one in each region, replicating to each other, so
each region could take orders locally and survive the other's outage.

In the first hour after the cutover, restaurants in the east saw orders that
customers in the west had never placed, couriers were sent to the wrong
addresses, and customers received "order not found" for orders they had just
paid for. Replication reported no errors and no lag. The cutover was rolled
back, and finance had to reconcile two hours of orders by hand.

## Root cause
Both primaries kept the default auto-increment settings: step 1, starting
at the same value. Each region took orders independently, so **both handed
out the same ids** (east and west both created order 1, then both created
order 2, ...). When a row arrived through replication, its primary key was
already used by a local order. Replication had been configured to skip
duplicate-key errors (MySQL `replica_skip_errors=1062`, a common
"keep replication running" setting), so the incoming order was silently
dropped.

In the reproduction (100 rounds, one order per region per round), each
region skips **100** replicated rows, each side is missing **100** of the 200
orders, and order 1 means a different order in each region.

## Solution
Give every primary its **own interleaved id sequence**, like MySQL's
`auto_increment_increment` (step) and `auto_increment_offset`:
- With step 10, east uses offset 1 (1, 11, 21, ...) and west offset 2 (2, 12,
  22, ...). The two sequences can never meet. A step larger than the number
  of regions leaves room to add more later without renumbering.
- `next_id` picks the smallest id in the node's own sequence above the current
  maximum (including replicated rows), so ids still roughly increase.
- `check_sequence_config` runs before a node starts and rejects mixed steps,
  a step smaller than the number of nodes and reused or out-of-range offsets.

With the fix both regions end with the same 200 orders, no row is skipped,
and every id ends in its region's offset.

## Trade-offs
- Ids are no longer dense and do not show creation order across regions
  (west's 12 may be older than east's 11). Do not use them for ordering or
  "orders per day" counts.
- The step is hard to change later; picking 10 or 100 up front wastes id
  space but avoids a migration when a region is added.
- Globally unique ids that need no coordination at all, such as UUIDs or
  Snowflake-style ids with a region/worker field (see problem 002), avoid
  the problem and work beyond one database product, at the cost of larger
  keys.
- Unique ids only fix key collisions. Two regions updating the **same** row
  still conflict, and active-active needs a conflict policy for that (see
  problems 006 and 022). Never skip replication errors silently; alert on
  them.

## Run
```bash
python -m unittest -v test_solution.py
```
