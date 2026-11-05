# 010 · Product search got slower with every catalog import, and "shoes running" returned nothing

## Problem
An outdoor-gear marketplace's search box ran
`SELECT … FROM products WHERE lower(name) LIKE '%<query>%'`. It was fine with
a few thousand products. As sellers imported their catalogs, search latency
grew with the catalog, the primary database's CPU tracked search traffic,
and during a sale search queries slowed down checkout because they shared a
database. Merchandising raised relevance complaints at the same time: "shoes
running" found nothing, "cafe" did not find the brand "Café Bleu", "pro"
returned waterproof jackets, and results came back in table order.

## Root cause
- **A leading wildcard cannot use an index.** A B-tree index on `name` helps
  only for prefix patterns (`LIKE 'abc%'`). `'%abc%'` forces a full scan, so
  every search reads every row: the cost is O(catalog size) per query, and it
  grows with every import.
- **Substring matching is the wrong model for search.** It depends on word
  order and exact characters, matches inside other words, and gives no ranking.

In the reproduction (20,000 generated product names), every `LIKE` query
examines all 20,000 rows; "shoes running" and "cafe bleu kettle" return 0
results; and "pro" returns 1,985 products that only match because they
contain "waterproof".

## Solution
An **inverted index** (`InvertedIndex` in `solution.py`), the core data
structure behind Lucene, Elasticsearch and OpenSearch:

1. **Normalise** text the same way at index and query time: Unicode NFKD,
   strip accents, lowercase, split into alphanumeric tokens
   (`"Café BLEU – Pro-Max 2"` → `cafe bleu pro max 2`).
2. **Postings:** token → set of product ids. Adding, renaming and removing a
   product update only that product's tokens.
3. **AND query by intersection, smallest list first:** start from the rarest
   term's postings and keep only ids present in the others. For
   "waterproof jacket", "kids socks red" and "trail tent olive" this touches
   2,582, 2,668 and 2,604 postings, about 13% of the 20,000 rows a scan reads.
   With a real catalog's much larger vocabulary, posting lists are far shorter
   relative to the catalog and the gap is wider.
4. **Search-as-you-type:** the last term is a prefix, expanded through a
   sorted vocabulary with binary search and capped at 50 terms, so "headph"
   works and "s" cannot explode into a huge union.
5. **BM25 ranking** (k1 = 1.2, b = 0.75): rarer terms and repeated terms score
   higher, long names are normalised. Ties break by id for stable paging.
   The catalog-wide statistics (document count, average length from a
   running `total_len`) are computed once per query. An earlier version
   re-summed every product's length for each candidate, so ranking cost
   matches × catalog size: on this machine "black" (3,345 of 20,000 matches)
   took about 500 ms, and 3-5 ms after the fix (best of 3, several runs). A
   query that broad still scores every match, so it stays slower than the
   unranked scan (about 2.5 ms); selective queries like "waterproof jacket"
   take about 0.2 ms vs 2.6 ms for the scan.

Correctness is checked against a brute-force oracle for eight queries,
including word-order swaps, accents, prefixes and a query with no matches.

## Trade-offs
- **A second copy of the data.** The index must be kept in sync with the
  database (change-data-capture or an outbox feeding the indexer), and it
  is eventually consistent: a new product appears after a short delay.
- **Memory:** postings for a large catalog do not fit one process. Search
  engines shard the index by document and merge top-k results from each shard.
- No typo tolerance, synonyms ("sneakers" = "running shoes") or stemming
  ("jackets" vs "jacket") here; those are analyzer features in Elasticsearch/
  OpenSearch and the main reason to adopt one instead of building your own.
- For smaller catalogs, the database's built-in full-text search (PostgreSQL
  `tsvector` + GIN index, or a `pg_trgm` trigram index for `LIKE '%…%'`)
  avoids running a separate system.

## Run
```bash
python -m unittest -v test_solution.py
```
