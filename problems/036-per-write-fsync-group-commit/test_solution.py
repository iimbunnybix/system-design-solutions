import unittest

from solution import (FSYNC_US, FakeDisk, GroupCommitLog, NaiveLog, simulate,
                      steady_arrivals)

PEAK_WRITES_PER_SECOND = 5_000
QUIET_WRITES_PER_SECOND = 400
ONE_SECOND_US = 1_000_000


def peak_load() -> list[int]:
    return steady_arrivals(PEAK_WRITES_PER_SECOND, 1)


class ReproduceProblem(unittest.TestCase):
    def test_one_fsync_per_write_caps_throughput_at_500_per_second(self):
        result = simulate(NaiveLog(FakeDisk()), peak_load())
        self.assertEqual(result.fsyncs, 5_000)
        # One second of traffic takes ten seconds to acknowledge.
        self.assertEqual(result.last_ack_us, 10 * ONE_SECOND_US)

    def test_latency_grows_to_seconds_under_peak_load(self):
        result = simulate(NaiveLog(FakeDisk()), peak_load())
        self.assertEqual(result.percentile_us(50), 4_500_200)
        self.assertEqual(result.percentile_us(99), 8_910_200)


class FixWithGroupCommit(unittest.TestCase):
    def test_peak_load_needs_a_tenth_of_the_fsyncs(self):
        result = simulate(GroupCommitLog(FakeDisk()), peak_load())
        self.assertEqual(result.fsyncs, 501)
        self.assertEqual(result.last_ack_us, 1_002_000)

    def test_latency_stays_within_two_fsyncs(self):
        # Worst case: arrive just after a flush started, wait for it, then
        # ride the next one.
        result = simulate(GroupCommitLog(FakeDisk()), peak_load())
        self.assertEqual(result.percentile_us(50), 2_800)
        self.assertEqual(result.percentile_us(99), 3_800)
        self.assertLessEqual(max(result.ack_latency_us), 2 * FSYNC_US)

    def test_quiet_traffic_is_not_delayed_to_build_batches(self):
        arrivals = steady_arrivals(QUIET_WRITES_PER_SECOND, 1)
        result = simulate(GroupCommitLog(FakeDisk()), arrivals)
        self.assertEqual(result.fsyncs, QUIET_WRITES_PER_SECOND)
        self.assertEqual(max(result.ack_latency_us), FSYNC_US)

    def test_batch_size_is_capped(self):
        burst = [0] * 1_000
        result = simulate(GroupCommitLog(FakeDisk(), max_batch=256), burst)
        self.assertEqual(result.fsyncs, 4)          # 256 + 256 + 256 + 232
        self.assertEqual(max(result.ack_latency_us), 4 * FSYNC_US)

    def test_every_acknowledged_record_is_already_durable(self):
        disk = FakeDisk()
        log = GroupCommitLog(disk, max_batch=3)
        for index in range(7):
            log.submit(f"event-{index}")
        while log.pending:
            acknowledged = log.flush()
            self.assertTrue(set(acknowledged) <= set(disk.durable))
            self.assertEqual(disk.buffered, [])

    def test_records_become_durable_in_submission_order(self):
        disk = FakeDisk()
        log = GroupCommitLog(disk, max_batch=2)
        for index in range(5):
            log.submit(f"event-{index}")
        while log.pending:
            log.flush()
        self.assertEqual(disk.durable, [f"event-{index}" for index in range(5)])

    def test_unflushed_records_are_not_durable(self):
        disk = FakeDisk()
        log = GroupCommitLog(disk)
        log.submit("event-0")
        self.assertEqual(disk.durable, [])

    def test_batch_limit_must_be_positive(self):
        with self.assertRaises(ValueError):
            GroupCommitLog(FakeDisk(), max_batch=0)

    def test_no_writes_means_no_fsyncs(self):
        result = simulate(GroupCommitLog(FakeDisk()), [])
        self.assertEqual(result.fsyncs, 0)


if __name__ == "__main__":
    unittest.main()
