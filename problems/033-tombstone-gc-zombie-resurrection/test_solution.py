import unittest

from solution import (DAY_S, HOUR_S, SAFE_GC_GRACE_S, Cell, FakeClock, RejoinRefused,
                      ReplicatedStore, naive_merge, newest_wins, original_store, run_until,
                      safe_store, simulate)


def store_with_message(make_store):
    """A 3-replica store holding msg-1 everywhere; replica c then goes offline."""
    clock = FakeClock()
    store = make_store(clock)
    store.put("msg-1", "hello")
    store.take_offline("c")
    return clock, store


class ReproduceProblem(unittest.TestCase):
    def test_replica_back_after_three_days_resurrects_every_deleted_message(self):
        self.assertEqual(simulate(original_store, offline_days=3), 200)

    def test_short_gc_grace_resurrects_even_with_version_compare(self):
        # Only the purge is wrong here: the tombstone is gone, so repair sees
        # c's old value and nothing to compare it against.
        clock = FakeClock()
        store = ReplicatedStore(clock, HOUR_S, newest_wins, check_rejoin=False)
        store.put("msg-1", "hello")
        store.take_offline("c")
        store.delete("msg-1")
        run_until(store, clock, 2 * HOUR_S)
        store.rejoin("c")
        self.assertEqual(store.get("msg-1"), "hello")

    def test_live_value_beats_a_newer_tombstone_in_the_original_merge(self):
        old_value = Cell("hello", version=1, written_at_s=0)
        tombstone = Cell(None, version=2, written_at_s=10)
        self.assertEqual(naive_merge(tombstone, old_value), old_value)


class FixTombstoneGrace(unittest.TestCase):
    def test_replica_back_after_three_days_resurrects_nothing(self):
        self.assertEqual(simulate(safe_store, offline_days=3), 0)

    def test_replica_away_longer_than_gc_grace_is_refused(self):
        clock, store = store_with_message(safe_store)
        store.delete("msg-1")
        run_until(store, clock, 12 * DAY_S)
        with self.assertRaises(RejoinRefused):
            store.rejoin("c")
        self.assertFalse(store.replicas["c"].online)

    def test_full_resync_after_long_absence_resurrects_nothing(self):
        self.assertEqual(simulate(safe_store, offline_days=12), 0)

    def test_full_resync_copies_writes_the_replica_missed(self):
        clock, store = store_with_message(safe_store)
        store.put("msg-2", "new while c was away")
        run_until(store, clock, 12 * DAY_S)
        store.full_resync("c")
        store.take_offline("a")
        store.take_offline("b")
        self.assertEqual(store.get("msg-2"), "new while c was away")

    def test_downtime_of_exactly_gc_grace_is_still_safe(self):
        clock, store = store_with_message(safe_store)
        store.delete("msg-1")
        run_until(store, clock, SAFE_GC_GRACE_S)
        store.rejoin("c")                                   # allowed: tombstone still exists
        self.assertIsNone(store.get("msg-1"))

    def test_tombstone_beats_an_older_write(self):
        old_value = Cell("hello", version=1, written_at_s=0)
        tombstone = Cell(None, version=2, written_at_s=10)
        self.assertEqual(newest_wins(tombstone, old_value), tombstone)
        self.assertEqual(newest_wins(old_value, tombstone), tombstone)

    def test_write_after_delete_brings_the_key_back_on_purpose(self):
        clock, store = store_with_message(safe_store)
        store.delete("msg-1")
        store.put("msg-1", "edited")
        clock.advance(DAY_S)
        store.rejoin("c")
        self.assertEqual(store.get("msg-1"), "edited")

    def test_tie_goes_to_the_tombstone(self):
        value = Cell("hello", version=5, written_at_s=0)
        tombstone = Cell(None, version=5, written_at_s=0)
        self.assertEqual(newest_wins(value, tombstone), tombstone)
        self.assertEqual(newest_wins(tombstone, value), tombstone)

    def test_tombstones_are_purged_once_gc_grace_passes_after_repair(self):
        clock, store = store_with_message(safe_store)
        store.delete("msg-1")
        clock.advance(DAY_S)
        store.rejoin("c")
        store.repair()                                      # c now holds the tombstone too
        run_until(store, clock, 12 * DAY_S)
        for replica in store.replicas.values():
            self.assertNotIn("msg-1", replica.cells)
        self.assertIsNone(store.get("msg-1"))


if __name__ == "__main__":
    unittest.main()
