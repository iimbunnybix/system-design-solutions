import unittest

from solution import (MAX_CLOCK_DRIFT_MS, ClockDriftError, HlcNode, HybridLogicalClock,
                      Timestamp, WallClockNode)

TRUE_START_MS = 1_700_000_000_000
SKEW_MS = 5_000          # node B's clock runs five seconds behind


class FakeClock:
    """Wall clock = shared true time + this node's fixed skew."""

    def __init__(self, true_time: "TrueTime", skew_ms: int = 0) -> None:
        self.true_time = true_time
        self.skew_ms = skew_ms

    def __call__(self) -> int:
        return self.true_time.now_ms + self.skew_ms


class TrueTime:
    def __init__(self) -> None:
        self.now_ms = TRUE_START_MS

    def advance(self, ms: int) -> None:
        self.now_ms += ms


def edit_on_lagging_node(node_class):
    """User saves v1 on A, B replicates it, then 1 s later the user saves v2 on B."""
    true_time = TrueTime()
    node_a = node_class("a", FakeClock(true_time))
    node_b = node_class("b", FakeClock(true_time, skew_ms=-SKEW_MS))
    first = node_a.write("profile:42", "v1")
    node_b.receive(first)
    true_time.advance(1_000)
    second = node_b.write("profile:42", "v2")
    node_a.receive(second)
    return node_a, node_b


class ReproduceProblem(unittest.TestCase):
    def test_newer_write_from_lagging_node_is_dropped(self):
        node_a, node_b = edit_on_lagging_node(WallClockNode)
        self.assertEqual(node_a.store.read("profile:42"), "v1")
        self.assertEqual(node_b.store.read("profile:42"), "v1")


class FixHybridLogicalClock(unittest.TestCase):
    def test_write_made_after_seeing_another_wins_on_every_replica(self):
        node_a, node_b = edit_on_lagging_node(HlcNode)
        self.assertEqual(node_a.store.read("profile:42"), "v2")
        self.assertEqual(node_b.store.read("profile:42"), "v2")

    def test_stamp_after_receive_is_greater_than_received_stamp(self):
        true_time = TrueTime()
        node_b = HlcNode("b", FakeClock(true_time, skew_ms=-SKEW_MS))
        remote = Timestamp(TRUE_START_MS, 3, "a")
        node_b.clock.observe(remote)
        self.assertGreater(node_b.clock.now(), remote)

    def test_stamps_never_go_backwards_when_wall_clock_jumps_back(self):
        true_time = TrueTime()
        clock = HybridLogicalClock("a", FakeClock(true_time))
        first = clock.now()
        true_time.advance(-10_000)            # NTP step backwards
        second = clock.now()
        self.assertGreater(second, first)
        self.assertEqual(second.wall_ms, first.wall_ms)

    def test_logical_counter_increments_within_one_millisecond(self):
        clock = HybridLogicalClock("a", FakeClock(TrueTime()))
        stamps = [clock.now() for _ in range(3)]
        self.assertEqual([stamp.logical for stamp in stamps], [0, 1, 2])

    def test_logical_counter_resets_when_physical_time_advances(self):
        true_time = TrueTime()
        clock = HybridLogicalClock("a", FakeClock(true_time))
        clock.now()
        clock.now()
        true_time.advance(1)
        self.assertEqual(clock.now().logical, 0)

    def test_stamps_stay_close_to_wall_time(self):
        true_time = TrueTime()
        clock = HybridLogicalClock("b", FakeClock(true_time, skew_ms=-SKEW_MS))
        clock.observe(Timestamp(TRUE_START_MS, 0, "a"))
        true_time.advance(SKEW_MS + 1)         # B's own clock catches up
        stamp = clock.now()
        self.assertEqual(stamp.wall_ms, TRUE_START_MS + 1)
        self.assertEqual(stamp.logical, 0)

    def test_concurrent_writes_converge_to_the_same_value(self):
        true_time = TrueTime()
        node_a = HlcNode("a", FakeClock(true_time))
        node_b = HlcNode("b", FakeClock(true_time))
        from_a = node_a.write("k", "from-a")
        from_b = node_b.write("k", "from-b")   # same wall time, no causal link
        node_a.receive(from_b)
        node_b.receive(from_a)
        self.assertEqual(node_a.store.read("k"), node_b.store.read("k"))
        self.assertEqual(node_a.store.read("k"), "from-b")   # node id tie-break

    def test_remote_stamp_far_in_the_future_is_rejected(self):
        clock = HybridLogicalClock("a", FakeClock(TrueTime()))
        broken = Timestamp(TRUE_START_MS + MAX_CLOCK_DRIFT_MS + 1, 0, "z")
        with self.assertRaises(ClockDriftError):
            clock.observe(broken)
        self.assertEqual(clock.now().wall_ms, TRUE_START_MS)

    def test_remote_stamp_within_drift_limit_is_accepted(self):
        clock = HybridLogicalClock("a", FakeClock(TrueTime()))
        clock.observe(Timestamp(TRUE_START_MS + MAX_CLOCK_DRIFT_MS, 0, "z"))
        self.assertEqual(clock.now().wall_ms, TRUE_START_MS + MAX_CLOCK_DRIFT_MS)

    def test_older_write_arriving_late_does_not_overwrite(self):
        true_time = TrueTime()
        node_a = HlcNode("a", FakeClock(true_time))
        node_b = HlcNode("b", FakeClock(true_time))
        old = node_a.write("k", "old")
        true_time.advance(10)
        node_b.write("k", "new")
        node_b.receive(old)                    # delayed replication
        self.assertEqual(node_b.store.read("k"), "new")


if __name__ == "__main__":
    unittest.main()
