# 059 · A price cut never appeared on the bestselling product page, while quiet products updated within minutes

## Problem
An e-commerce catalogue service keeps product prices in an in-memory cache
in front of the pricing database. The cache was configured with a 5-minute
TTL using *sliding expiration*: every read reset the entry's timer, so idle
products fell out of memory and busy ones stayed warm. Prices were not
invalidated on change; the team relied on the TTL to pick them up.

During a sale, merchandising cut the price of the bestselling product.
Products nobody looked at showed their new prices within minutes, but the
bestseller (viewed every couple of seconds) kept showing the old price for
over a day. Checkout used the database price, so shoppers saw one price on
the product page and were charged another, and support was flooded with
"wrong price" tickets until someone restarted the cache.

## Root cause
With sliding expiration the **only** timer is "time since the last read".
A key that is read more often than once per TTL never reaches that timer, so
it is never reloaded, however old its value is. The hotter the key, the
longer it stays stale, which is exactly backwards for the pages that matter
most. In the tests (`NaiveSlidingCache`, a read every 2 seconds) the old
price is served for the full simulated day after the change and the database
is queried once in total, while a product read every 400 seconds picks up the
new price on its next read.

## Solution
`BoundedAgeCache` keeps two separate limits on every entry:

- **Idle timeout** (sliding, 5 minutes) based on `last_read_at`: still evicts
  entries nobody reads, which was the reason for the sliding TTL.
- **Maximum age** (absolute, 5 minutes) based on `loaded_at`: set once when
  the value is loaded from the database and never moved by a read.

An entry is served only while both limits hold. Staleness is now bounded by
the maximum age regardless of traffic: in the tests the hot product shows
the new price 298 seconds after the change. A full day of reads every
2 seconds (43,200 page views) costs 288 database loads, one per 5 minutes.

## Trade-offs
- **Staleness is bounded, not removed.** Shoppers can still see the old price
  for up to the maximum age. Where that matters, also invalidate on write
  (publish a change event and delete the key) and keep the maximum age as a
  safety net for lost invalidations.
- **Synchronized reloads.** Every hot key reloads on a fixed schedule, and a
  key loaded by many processes at once expires everywhere at once; add TTL
  jitter (backend-api-solutions problem 023) and single-flight loading
  (problem 003) on busy caches.
- **Stale-while-revalidate** can hide the reload latency: serve the old value
  once and refresh it in the background, at the cost of one extra stale read.
- Many caches offer both settings (for example Caffeine's `expireAfterWrite`
  and `expireAfterAccess`); setting only the access-based one causes this bug.

## Run
```bash
python -m unittest -v test_solution.py
```
