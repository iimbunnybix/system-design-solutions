# 040 · Every night at 2 a.m. product pages slowed to a crawl and the database hit 100% CPU, right when the analytics export started

## Problem
An online store serves product pages through a read-through cache in front
of the product database. During the day the cache answers more than 99% of
reads: shoppers keep looking at the same few thousand products. Each night
a batch job exports the whole catalogue (a million product ids, each read
once) to the analytics warehouse, and it reads through the same cache
client.

Every night, as soon as the export started, the cache hit rate for shopper
traffic collapsed, product pages took seconds to load, and the database CPU
went to 100%. Night-shift shoppers in other time zones saw timeouts, and
the on-call engineer was paged most nights. The hit rate stayed low for a
while even after the export finished.

In the simulation (cache of 10,000 entries, 5,000 hot products with a
Zipf-like popularity (exponent 0.6), 1,000,000 catalogue ids scanned with
one shopper read per 100 scanned ids, seed 1), the plain LRU cache gives:

| Phase | Shopper hit rate |
|---|---|
| Daytime | **99.94%** |
| During the scan | **8.92%** (9,108 of 10,000 shopper reads go to the database) |
| First 5,000 reads after the scan | **49.30%** |

## Root cause
**LRU treats a key read once as just as valuable as a key read every
second.** Each scanned id becomes the most recently used entry. After
10,000 scanned ids (here, every 100 shopper reads) the whole hot working
set has been pushed out, and the scan keys that replaced it are never read
again. This is the classic *cache pollution by a sequential scan*.
Afterwards the cache holds only the tail of the export and has to warm up
again from the database.

## Solution
A **segmented LRU** (`SegmentedLRUCache` in `solution.py`), the core idea of
the 2Q algorithm:

1. The cache has two LRU segments: **probation** (20% of capacity) and
   **protected** (80%).
2. A new key enters probation. Eviction only ever removes the oldest
   probation entry.
3. A key hit again while in probation is **promoted** to protected. Hits in
   protected just refresh it.
4. When protected is full, its oldest key is **demoted** to probation
   rather than dropped, so it gets one more chance.

The scan reads each id once, so scan keys only ever churn through probation
and evict each other. The hot products, which shoppers read many times,
live in protected and are untouched.

Results on the same simulation:

| Phase | LRU | Segmented LRU |
|---|---|---|
| Daytime | 99.94% | 99.94% |
| During the scan | 8.92% | **99.97%** (3 database reads instead of 9,108) |
| First 5,000 reads after the scan | 49.30% | **99.98%** |

The whole night runs in about 1.6 s for both caches together.

## Trade-offs
- **A genuinely new hot key has to prove itself.** Until its second hit it
  sits in probation and can be evicted by a scan (tested). With a busy scan
  a new product launch may need a few extra misses before it sticks.
- The split ratio is a tuning knob. A larger protected share protects more
  of the hot set but makes probation smaller, so new keys must be reused
  sooner to be promoted. 80/20 is the split Caffeine uses for its main
  region.
- Full 2Q also keeps a "ghost" list of recently evicted keys (keys only,
  no values) so a key that comes back soon after eviction is promoted
  directly. ARC and W-TinyLFU (Caffeine) adapt the split and use access
  frequency; they are more robust but more complex.
- A simpler operational fix is to let the batch job **bypass the cache**
  (read the database or a replica directly, or use a "do not cache" flag).
  That is worth doing as well, but a scan-resistant cache also protects
  against scans nobody planned: a crawler, a migration script, a new report.
- The export still puts its own load on the database (it reads a million
  rows). Run it against a read replica or a snapshot.

## Run
```bash
python -m unittest -v test_solution.py
```
