import unittest

from solution import (Event, RangeShardedStore, bucket_split_points, hottest_shard_share,
                      naive_row_key, naive_time_range, salted_row_key, salted_time_range)

SHARD_COUNT = 8
BUCKET_COUNT = 16
DAY_MS = 86_400_000
HISTORY_START_MS = 1_790_000_000_000
HISTORY_DAYS = 30
TODAY_START_MS = HISTORY_START_MS + HISTORY_DAYS * DAY_MS
EVENTS_TODAY = 4_000
EVENT_SPACING_MS = 250


def todays_events() -> list[Event]:
    """Live traffic: events arriving in time order during today."""
    events = []
    for index in range(EVENTS_TODAY):
        timestamp = TODAY_START_MS + index * EVENT_SPACING_MS
        events.append(Event(f"evt-{index:06d}", timestamp, f"payload-{index}"))
    return events


def naive_store() -> RangeShardedStore:
    """Shards split evenly over the last 30 days of timestamps, as an operator would pre-split."""
    days_per_shard = HISTORY_DAYS / SHARD_COUNT
    split_points = []
    for shard_index in range(1, SHARD_COUNT):
        boundary_ms = HISTORY_START_MS + int(shard_index * days_per_shard * DAY_MS)
        split_points.append(f"{boundary_ms:013d}")
    return RangeShardedStore(split_points)


def salted_store() -> RangeShardedStore:
    return RangeShardedStore(bucket_split_points(BUCKET_COUNT, SHARD_COUNT))


def load_naive(events: list[Event]) -> RangeShardedStore:
    store = naive_store()
    for event in events:
        store.put(naive_row_key(event), event)
    return store


def load_salted(events: list[Event]) -> RangeShardedStore:
    store = salted_store()
    for event in events:
        store.put(salted_row_key(event, BUCKET_COUNT), event)
    return store


class ReproduceProblem(unittest.TestCase):
    def test_all_new_writes_land_on_the_last_shard(self):
        store = load_naive(todays_events())
        self.assertEqual(store.writes_per_shard[-1], EVENTS_TODAY)
        self.assertEqual(hottest_shard_share(store), 1.0)

    def test_naive_keys_sort_in_arrival_order(self):
        events = todays_events()
        keys = [naive_row_key(event) for event in events]
        self.assertEqual(keys, sorted(keys))


class FixWithBucketPrefix(unittest.TestCase):
    def test_writes_spread_across_every_shard(self):
        store = load_salted(todays_events())
        self.assertTrue(all(count > 0 for count in store.writes_per_shard))

    def test_hottest_shard_takes_close_to_an_even_share(self):
        store = load_salted(todays_events())
        even_share = 1 / SHARD_COUNT
        self.assertLess(hottest_shard_share(store), even_share * 1.3)

    def test_time_range_read_returns_same_events_in_time_order(self):
        events = todays_events()
        start_ms = TODAY_START_MS + 100 * EVENT_SPACING_MS
        end_ms = TODAY_START_MS + 900 * EVENT_SPACING_MS
        expected = naive_time_range(load_naive(events), start_ms, end_ms)
        actual = salted_time_range(load_salted(events), start_ms, end_ms, BUCKET_COUNT)
        self.assertEqual(len(actual), 800)
        self.assertEqual(actual, expected)

    def test_range_end_is_exclusive(self):
        events = todays_events()
        end_ms = TODAY_START_MS + 10 * EVENT_SPACING_MS
        result = salted_time_range(load_salted(events), TODAY_START_MS, end_ms, BUCKET_COUNT)
        self.assertEqual([event.event_id for event in result][-1], "evt-000009")

    def test_empty_time_range_returns_nothing(self):
        result = salted_time_range(load_salted(todays_events()), 0, 1, BUCKET_COUNT)
        self.assertEqual(result, [])

    def test_same_event_id_always_gets_the_same_key(self):
        event = Event("evt-42", TODAY_START_MS, "x")
        self.assertEqual(salted_row_key(event, BUCKET_COUNT), salted_row_key(event, BUCKET_COUNT))

    def test_events_with_equal_timestamps_are_all_kept(self):
        events = [Event(f"dup-{index}", TODAY_START_MS, "x") for index in range(50)]
        result = salted_time_range(load_salted(events), TODAY_START_MS, TODAY_START_MS + 1,
                                   BUCKET_COUNT)
        self.assertEqual(sorted(event.event_id for event in result),
                         sorted(event.event_id for event in events))

    def test_bucket_count_must_divide_evenly_into_shards(self):
        with self.assertRaises(ValueError):
            bucket_split_points(10, SHARD_COUNT)


if __name__ == "__main__":
    unittest.main()
