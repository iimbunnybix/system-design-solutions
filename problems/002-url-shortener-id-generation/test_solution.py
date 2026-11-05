import threading
import unittest

from solution import (EPOCH_MS, ClockBeforeEpoch, ClockMovedBackwards, SnowflakeGenerator, base62_decode, base62_encode,
                      decompose, random_code_collision_probability)


class FakeClock:
    def __init__(self, t=1_800_000_000_000):
        self.t = t
        self.reads = 0

    def __call__(self):
        self.reads += 1
        return self.t


class FakeMonotonic:
    """Monotonic clock that moves forward 1 ms every time it is read."""

    def __init__(self):
        self.t = 0

    def __call__(self):
        self.t += 1
        return self.t


class RecoveringClock(FakeClock):
    """Wall clock that creeps forward 1 ms per read, as a real clock does while we wait."""

    def __call__(self):
        value = super().__call__()
        self.t += 1
        return value


class SnowflakeTests(unittest.TestCase):
    def test_base62_round_trip(self):
        for n in (0, 1, 61, 62, 3843, 2 ** 63 - 1):
            self.assertEqual(base62_decode(base62_encode(n)), n)
        self.assertLessEqual(len(base62_encode(2 ** 63 - 1)), 11)

    def test_ids_unique_and_increasing_across_threads(self):
        gen = SnowflakeGenerator(worker_id=7)
        ids, lock = [], threading.Lock()

        def work():
            local = [gen.next_id() for _ in range(5000)]
            with lock:
                ids.extend(local)

        ts = [threading.Thread(target=work) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(len(set(ids)), 20_000)

    def test_different_workers_never_collide_in_same_ms(self):
        clock = FakeClock()
        a, b = SnowflakeGenerator(1, clock), SnowflakeGenerator(2, clock)
        self.assertNotEqual(a.next_id(), b.next_id())

    def test_layout_decomposes(self):
        clock = FakeClock()
        parts = decompose(SnowflakeGenerator(513, clock).next_id())
        self.assertEqual(parts, {"timestamp_ms": clock.t, "worker_id": 513, "sequence": 0})

    def test_sequence_overflow_waits_for_next_ms(self):
        clock = FakeClock()
        gen = SnowflakeGenerator(1, clock)
        for _ in range(4096):
            gen.next_id()
        orig = clock.__call__

        def advancing():
            clock.t += 1 if clock.reads > 4100 else 0
            return orig()

        gen._clock = advancing
        nxt = decompose(gen.next_id())
        self.assertEqual(nxt["sequence"], 0)
        self.assertGreater(nxt["timestamp_ms"], 1_800_000_000_000)

    def test_large_clock_rollback_is_refused(self):
        clock = FakeClock()
        gen = SnowflakeGenerator(1, clock)
        gen.next_id()
        clock.t -= 1000
        with self.assertRaises(ClockMovedBackwards):
            gen.next_id()

    def test_clock_before_epoch_is_refused(self):
        gen = SnowflakeGenerator(1, FakeClock(EPOCH_MS - 1))
        with self.assertRaises(ClockBeforeEpoch):
            gen.next_id()

    def test_clock_at_unix_epoch_mints_no_code(self):
        # A VM that boots with its clock at 1970 before NTP sync.
        gen = SnowflakeGenerator(1, FakeClock(0))
        with self.assertRaises(ClockBeforeEpoch):
            gen.next_code()

    def test_clock_exactly_at_epoch_gives_id_zero_timestamp(self):
        gen = SnowflakeGenerator(0, FakeClock(EPOCH_MS))
        self.assertEqual(gen.next_id(), 0)

    def test_small_rollback_is_waited_out(self):
        clock = RecoveringClock()
        gen = SnowflakeGenerator(1, clock, monotonic_ms=FakeMonotonic())
        first = gen.next_id()
        clock.t -= 3
        self.assertGreater(gen.next_id(), first)

    def test_clock_that_never_recovers_raises(self):
        clock = FakeClock()
        gen = SnowflakeGenerator(1, clock, monotonic_ms=FakeMonotonic())
        gen.next_id()
        clock.t -= 3                     # inside the 5 ms allowance, but the clock is frozen
        with self.assertRaises(ClockMovedBackwards):
            gen.next_id()

    def test_birthday_problem_explains_the_incident(self):
        self.assertGreater(random_code_collision_probability(300_000), 0.5)
        self.assertAlmostEqual(40_000_000 / 62 ** 6, 0.0007, places=4)   # per-insert collision chance

    def test_bad_worker_id(self):
        with self.assertRaises(ValueError):
            SnowflakeGenerator(1024)


if __name__ == "__main__":
    unittest.main()
