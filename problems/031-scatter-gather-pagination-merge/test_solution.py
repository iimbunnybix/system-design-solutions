import unittest

from solution import (LISTING_COUNT, PAGE_SIZE, SHARD_COUNT, Listing, Shard, browse_cursor_feed,
                      browse_naive_feed, build_marketplace, cursor_page, global_order,
                      naive_offset_page, offset_page_correct, total_rows_read)


def flatten(pages: list[list[Listing]]) -> list[Listing]:
    """All listings shown across the given pages, in display order."""
    return [listing for page in pages for listing in page]


def shards_with(rows_per_shard: list[list[tuple[int, int]]]) -> list[Shard]:
    """Build shards from (listing_id, created_at) pairs, one list per shard."""
    shards = []
    for rows in rows_per_shard:
        shard = Shard()
        for listing_id, created_at in rows:
            shard.insert(Listing(listing_id, created_at))
        shards.append(shard)
    return shards


class ReproduceProblem(unittest.TestCase):
    def setUp(self) -> None:
        self.shards = build_marketplace()
        self.expected = global_order(self.shards)

    def test_first_page_is_correct_which_is_why_the_bug_passed_review(self):
        self.assertEqual(naive_offset_page(self.shards, 0), self.expected[:PAGE_SIZE])

    def test_second_page_is_wrong(self):
        self.assertNotEqual(naive_offset_page(self.shards, 1), self.expected[PAGE_SIZE:2 * PAGE_SIZE])

    def test_feed_runs_out_after_13_pages_instead_of_90(self):
        self.assertEqual(len(browse_naive_feed(self.shards)), 13)
        self.assertEqual(len(self.expected) // PAGE_SIZE, 90)

    def test_most_listings_never_appear_on_any_page(self):
        shown = set(flatten(browse_naive_feed(self.shards)))
        self.assertEqual(len(shown), 245)
        self.assertEqual(LISTING_COUNT - len(shown), 1_555)


class FixGlobalPagination(unittest.TestCase):
    def setUp(self) -> None:
        self.shards = build_marketplace()
        self.expected = global_order(self.shards)

    def test_offset_with_full_prefix_returns_every_page_exactly(self):
        for page_number in range(90):
            start = page_number * PAGE_SIZE
            self.assertEqual(offset_page_correct(self.shards, page_number),
                             self.expected[start:start + PAGE_SIZE])

    def test_cursor_feed_shows_every_listing_once_in_order(self):
        self.assertEqual(flatten(browse_cursor_feed(self.shards)), self.expected)

    def test_correct_offset_paging_gets_more_expensive_with_depth(self):
        before = total_rows_read(self.shards)
        offset_page_correct(self.shards, 0)
        first = total_rows_read(self.shards) - before
        before = total_rows_read(self.shards)
        offset_page_correct(self.shards, 9)
        tenth = total_rows_read(self.shards) - before
        self.assertEqual((first, tenth), (160, 1_600))

    def test_cursor_page_cost_never_exceeds_shards_times_page_size(self):
        cursor = None
        while True:
            before = total_rows_read(self.shards)
            _, cursor = cursor_page(self.shards, cursor)
            self.assertLessEqual(total_rows_read(self.shards) - before, SHARD_COUNT * PAGE_SIZE)
            if cursor is None:
                break

    def test_browsing_the_whole_feed_reads_11x_fewer_rows_with_cursors(self):
        browse_cursor_feed(self.shards)
        cursor_rows = total_rows_read(self.shards)
        offset_shards = build_marketplace()
        for page_number in range(90):
            offset_page_correct(offset_shards, page_number)
        self.assertEqual(cursor_rows, 13_797)
        self.assertEqual(total_rows_read(offset_shards), 152_717)

    def test_equal_timestamps_on_different_shards_are_not_skipped_or_repeated(self):
        shards = shards_with([[(1, 100), (2, 100)], [(3, 100), (4, 100)], [(5, 100)]])
        pages = browse_cursor_feed(shards, limit=2)
        ids = [listing.listing_id for listing in flatten(pages)]
        self.assertEqual(ids, [5, 4, 3, 2, 1])            # ties broken by id, newest id first

    def test_one_shard_holding_all_the_newest_rows_is_paged_correctly(self):
        hot = [(i, 1_000 + i) for i in range(1, 41)]
        cold = [(100 + i, i) for i in range(1, 6)]
        shards = shards_with([hot, cold, []])
        self.assertEqual(flatten(browse_cursor_feed(shards, limit=10)), global_order(shards))

    def test_empty_shards_give_an_empty_feed(self):
        shards = shards_with([[], [], []])
        self.assertEqual(cursor_page(shards, None), ([], None))
        self.assertEqual(offset_page_correct(shards, 0), [])

    def test_page_past_the_end_is_empty(self):
        self.assertEqual(offset_page_correct(self.shards, 90), [])


if __name__ == "__main__":
    unittest.main()
