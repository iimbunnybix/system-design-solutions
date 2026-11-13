# 050 · Product pages returned 500s on and off for the 20 minutes a pricing release took to roll out

## Problem
A pricing service runs dozens of pods behind a load balancer and caches each
product in a shared Redis cluster under `product:{id}`. Release 2 added
multi-currency support and changed the cached value from
`{"price": 1999}` (paise) to `{"price": {"amount": 1999, "currency": "INR"}}`.
During the rolling deploy, product pages failed intermittently: a request
landing on an old pod crashed if a new pod had written the entry last, and
vice versa. Error rates swung up and down for the whole rollout, and rolling
back did not help at once, because the cache was still full of the new format.

## Root cause
Both releases used **the same cache key for two incompatible formats**. In a
rolling deploy, old and new code run at the same time and share the cache,
so each version reads entries the other wrote. The old pod divides a dict by
100 and the new pod indexes an int with `["currency"]`; both raise
`TypeError` (reproduced in `ReproduceProblem`). A shared cache is a
shared data contract, but it was treated as private to each release.

## Solution
- `versioned_cache_key` includes the schema version: `product:v1:42` and
  `product:v2:42`. Each release reads and writes only entries in its own
  format, so the two can run side by side. In the test, both pods serve the
  right price repeatedly with just **2 database loads** in total (one per
  version) and then hit their own entries.
- `_read_cached` treats an entry that cannot be decoded or rendered as a
  **cache miss**: it counts it in `decode_failures`, reloads from the
  database and overwrites the bad entry. A bad cache value becomes extra
  latency, never an error page.
- Rolling back is safe: v1 pods find their `v1` entries still in place.

## Trade-offs
- During the rollout the cache holds both formats, so memory use for these
  keys temporarily doubles and the new version starts with a cold namespace
  (a burst of database reads). Pre-warming the new keys before the rollout,
  or a TTL that lets old keys age out, limits both.
- The alternative is an additive, backward-compatible format change (new
  fields only, readers ignore unknown fields) over two releases. That keeps
  one namespace but needs discipline and an "expand, then contract" plan.
- Bumping the version must be part of code review whenever the cached shape
  changes; a test that decodes the previous release's sample entries helps.

## Run
```bash
python -m unittest -v test_solution.py
```
