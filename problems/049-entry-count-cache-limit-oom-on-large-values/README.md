# 049 · Product-API pods were OOM-killed every few hours although the cache "held at most 10,000 entries"

## Problem
Each product-API pod keeps an in-process LRU cache of rendered responses,
limited to **10,000 entries**. It was sized when every cached value was a
~2 KB product card, so the cache used about 20 MB. A later feature started
caching whole category pages (200-400 KB each) in the same cache. Pods began
getting OOM-killed by Kubernetes every few hours, each restart cleared the
cache, and the database took a burst of misses. Memory graphs showed the
cache growing far past its expected size while the entry-count metric sat
flat at 10,000.

In the test replay of 20,000 requests (80% product cards, 20% category
pages), the count-bounded cache stays at 10,000 entries but peaks at
**about 542 MB** of cached values.

## Root cause
The limit counted **entries**, but memory is consumed by **bytes**. An entry
limit is only a memory limit when values have a known, uniform size. As soon
as value sizes vary by 100-200x, 10,000 entries can mean 20 MB or
several GB, and nothing in the cache notices.

## Solution
`ByteBoundedLRU`:
- Tracks the size of each value and a running `total_bytes`, and after each
  `put` evicts least-recently-used entries until the total fits
  `max_bytes`. In the same replay with a 64 MB budget, peak usage is
  exactly the 64 MB budget, never above it.
- Refuses values larger than `max_item_bytes` (1 MB here) and counts them in
  `rejected_too_large`. One huge value would otherwise evict hundreds of hot
  small ones; it is cheaper to serve it uncached.
- Overwriting a key subtracts the old size first, and an oversized overwrite
  removes the stale value rather than leaving it cached.

## Trade-offs
- `len(value)` counts payload bytes only. Real memory per entry includes
  object headers, the key and the dict/linked-list overhead, so set the
  budget below the pod's memory limit with headroom, and alert on process
  RSS, not only on the cache's own counter.
- With mixed sizes, large values still take most of the budget: in the
  replay the 64 MB cache ends with only 1,074 entries. Separate caches (or
  a size-aware admission policy such as TinyLFU or GDSF) protect the hit
  rate of the small, hot items.
- Measuring size is easy for bytes; for Python objects it needs
  serialisation or an estimate, which costs CPU on every put.

## Run
```bash
python -m unittest -v test_solution.py
```
