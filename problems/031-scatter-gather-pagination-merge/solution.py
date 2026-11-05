"""Paging through "newest first" results that are spread over many shards.

The incident: a marketplace keeps listings in 8 shards (by seller) and shows a
global "newest listings" feed, 20 per page. To serve page N the API asked
every shard for *its own* page N (OFFSET N*20 LIMIT 20), merged the 160 rows
and kept the newest 20. Page 1 is always right that way, so it passed review.
From page 2 on, each page throws away the 140 rows it did not keep, and those
rows are past every later page's offset, so most listings never appear on any
page and the feed runs out long before the catalog does.

The fix: a global page needs each shard's rows *up to* that point, not each
shard's page N. ``offset_page_correct`` fetches OFFSET+LIMIT rows from each
shard (correct, but cost grows with page depth). ``cursor_page`` keeps a
cursor (created_at, id) of the last row shown, asks each shard for LIMIT rows
after it and k-way merges them: correct and constant cost per page.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass

SHARD_COUNT = 8
PAGE_SIZE = 20
LISTING_COUNT = 1_800
DAY_SECONDS = 86_400


@dataclass(frozen=True, order=True)
class SortKey:
    """Feed order: newest first, ties broken by id so the order is total."""

    neg_created_at: int
    neg_listing_id: int


@dataclass(frozen=True)
class Listing:
    listing_id: int
    created_at: int              # seconds since an arbitrary start

    def sort_key(self) -> SortKey:
        """Smaller key = earlier in the feed."""
        return SortKey(-self.created_at, -self.listing_id)


class Shard:
    """One database shard; ``rows_read`` counts rows it returned to the API."""

    def __init__(self) -> None:
        self.listings: list[Listing] = []
        self.rows_read = 0

    def insert(self, listing: Listing) -> None:
        """Add a listing (kept unsorted; queries sort, like an index scan would)."""
        self.listings.append(listing)

    def _ordered(self) -> list[Listing]:
        return sorted(self.listings, key=Listing.sort_key)

    def query_offset(self, offset: int, limit: int) -> list[Listing]:
        """ORDER BY created_at DESC, id DESC OFFSET offset LIMIT limit.

        A real database still reads and discards the first ``offset`` rows,
        so they count as read.
        """
        rows = self._ordered()[: offset + limit]
        self.rows_read += len(rows)
        return rows[offset:]

    def query_after(self, cursor: SortKey | None, limit: int) -> list[Listing]:
        """WHERE (created_at, id) < cursor ORDER BY ... LIMIT limit (keyset seek)."""
        rows = []
        for listing in self._ordered():
            if cursor is not None and listing.sort_key() <= cursor:
                continue
            rows.append(listing)
            if len(rows) == limit:
                break
        self.rows_read += len(rows)
        return rows


def merge_newest(per_shard: list[list[Listing]], limit: int) -> list[Listing]:
    """K-way merge of already-sorted shard results; returns the first ``limit``."""
    merged = heapq.merge(*per_shard, key=Listing.sort_key)
    page = []
    for listing in merged:
        page.append(listing)
        if len(page) == limit:
            break
    return page


# ---------------------------------------------------------------- original
def naive_offset_page(shards: list[Shard], page_number: int, limit: int = PAGE_SIZE) -> list[Listing]:
    """Original: ask every shard for its own page N, merge, keep the newest ``limit``."""
    offset = page_number * limit
    per_shard = [shard.query_offset(offset, limit) for shard in shards]
    return merge_newest(per_shard, limit)


# --------------------------------------------------------------------- fix
def offset_page_correct(shards: list[Shard], page_number: int, limit: int = PAGE_SIZE) -> list[Listing]:
    """Correct offset paging: each shard returns its first OFFSET+LIMIT rows.

    Any row on global page N is among the first OFFSET+LIMIT rows of its own
    shard, so merging those and skipping OFFSET gives the exact page. The cost
    is shards x (OFFSET+LIMIT) rows, growing with every page.
    """
    offset = page_number * limit
    per_shard = [shard.query_offset(0, offset + limit) for shard in shards]
    merged = merge_newest(per_shard, offset + limit)
    return merged[offset:]


def cursor_page(shards: list[Shard], cursor: SortKey | None,
                limit: int = PAGE_SIZE) -> tuple[list[Listing], SortKey | None]:
    """Keyset paging: LIMIT rows after the cursor from each shard, then merge.

    Each shard's next ``limit`` rows after the cursor include every row it can
    contribute to the next global page. Returns the page and the cursor for
    the following page (None when the feed is exhausted).
    """
    per_shard = [shard.query_after(cursor, limit) for shard in shards]
    page = merge_newest(per_shard, limit)
    if len(page) < limit:
        return page, None
    return page, page[-1].sort_key()


# ---------------------------------------------------------------- workload
def build_marketplace(seed: int = 7) -> list[Shard]:
    """8 shards holding one day of listings, each on a random seller's shard."""
    rng = random.Random(seed)
    shards = [Shard() for _ in range(SHARD_COUNT)]
    for listing_id in range(1, LISTING_COUNT + 1):
        shard = shards[rng.randrange(SHARD_COUNT)]
        shard.insert(Listing(listing_id, rng.randrange(DAY_SECONDS)))
    return shards


def browse_naive_feed(shards: list[Shard], limit: int = PAGE_SIZE) -> list[list[Listing]]:
    """Click "next" on the original feed until it returns an empty page."""
    pages = []
    page_number = 0
    while True:
        page = naive_offset_page(shards, page_number, limit)
        if not page:
            return pages
        pages.append(page)
        page_number += 1


def browse_cursor_feed(shards: list[Shard], limit: int = PAGE_SIZE) -> list[list[Listing]]:
    """Click "next" on the cursor feed until it reports no further page."""
    pages = []
    cursor = None
    while True:
        page, cursor = cursor_page(shards, cursor, limit)
        if page:
            pages.append(page)
        if cursor is None:
            return pages


def global_order(shards: list[Shard]) -> list[Listing]:
    """Reference answer: every listing from every shard in feed order."""
    everything = [listing for shard in shards for listing in shard.listings]
    return sorted(everything, key=Listing.sort_key)


def total_rows_read(shards: list[Shard]) -> int:
    """Sum of rows the shards returned; a proxy for database work."""
    return sum(shard.rows_read for shard in shards)
