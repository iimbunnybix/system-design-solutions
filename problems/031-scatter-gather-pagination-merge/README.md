# 031 · The "newest listings" feed ended after 13 pages, and sellers said most of their listings never showed up

## Problem
A second-hand marketplace stores listings in 8 database shards, sharded by
seller. The home page has a global "newest listings" feed, 20 per page,
served by a scatter-gather API: it queries every shard and merges the results.
Page 1 looked right in review and in production. Support then got two kinds
of tickets: buyers said the feed "ran out" after a dozen pages even though
the marketplace had thousands of fresh listings, and sellers said their
new listings never appeared in the feed at all, so they paid for "bump"
promotions that did not help. Listing views from the feed dropped, and the
team only noticed when a seller compared his listings against the feed by hand.

## Root cause
**Each shard was asked for its own page N, not for its part of the global
page N.** For page N the API ran
`ORDER BY created_at DESC, id DESC OFFSET N*20 LIMIT 20` on every shard,
merged the 160 rows and kept the newest 20.

That is right only for page 1. A global page holds roughly 20 / 8 rows from
each shard, so global page 2 is made of each shard's rows 3 to 5 or so, but
the API asked each shard for rows 21 to 40. Every page keeps 20 of the 160
rows it fetched and throws away 140, and the next page's offset is already
past them, so they are never shown. Once each shard's offset passes its row
count, the feed returns an empty page and ends.

With 1,800 listings over one day, spread at random over 8 shards
(`naive_offset_page`, seeded): page 1 is exact, page 2 is already wrong, the
feed ends after **13 pages** instead of 90, and **1,555 of the 1,800 listings
(86%) never appear on any page**.

This is a sharding bug, not the single-database pagination bugs in
backend-api problems 004 (rows shifting under OFFSET) and 027 (a cursor reused
with different filters): every shard query here is correct on its own; the
merge asks for the wrong rows.

## Solution
Two correct ways to page a scatter-gather query (`solution.py`):

1. **Offset paging with the full prefix** (`offset_page_correct`). Any row on
   global page N is among the first `OFFSET + LIMIT` rows of its own shard, so
   ask each shard for `OFFSET 0 LIMIT OFFSET+LIMIT`, merge, and skip `OFFSET`.
   Every one of the 90 pages matches the reference order. The cost grows with
   depth: page 1 reads 160 rows, page 10 reads 1,600, and the last pages read
   the whole table (1,800). This is what Elasticsearch does for `from + size`,
   and why it caps `from + size` at 10,000 by default.
2. **Cursor (keyset) paging with a k-way merge** (`cursor_page`). The cursor
   is the sort key `(created_at, id)` of the last listing shown. Each shard
   returns its next 20 rows after the cursor (an index seek, not an offset),
   and the API merges the 8 sorted lists with `heapq.merge` and keeps 20.
   A shard can contribute at most 20 rows to the next page, so 20 per shard
   is always enough. Each page costs at most 8 x 20 = 160 rows, however deep.

The sort key includes the listing id, so listings with the same timestamp on
different shards are neither skipped nor shown twice (tested).

Browsing the whole feed (90 pages):

| Method | Pages | Listings shown | Rows read from shards |
|---|---|---|---|
| Original (each shard's page N) | 13 | 245 | 15,917 |
| Offset with full prefix | 90 | 1,800, in order | 152,717 |
| Cursor + k-way merge | 90 | 1,800, in order | 13,797 |

## Trade-offs
- **No "jump to page 37"** with cursors; the UI becomes "next / previous" or
  infinite scroll. If page numbers are required, keep offset paging with the
  full prefix and cap the depth.
- **Over-fetching:** each cursor page reads up to 8 x 20 rows to show 20.
  With many shards, ask each shard for fewer rows and fetch more from a shard
  only when the merge runs out of its rows, at the cost of extra round trips.
- **Cursor stability:** new listings added above the cursor do not shift
  later pages (unlike offsets), but they are not shown until the user goes
  back to the top. Deleted rows simply disappear.
- The cursor is visible to clients. Sign it or encode it opaquely, and bind it
  to the query's filters (see backend-api problem 027).
- A feed that must be cheap at any depth can be served from a separate,
  globally sorted index (a search engine or a time-ordered table fed by
  change-data-capture) instead of scatter-gather.

## Run
```bash
python -m unittest -v test_solution.py
```
