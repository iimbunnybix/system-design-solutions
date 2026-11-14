import os
import struct
import tempfile
import unittest

from solution import (FRAME_HEADER, MAX_RECORD_BYTES, AppendOnlyLog, encode_frame,
                      naive_encode, naive_replay, recover)

COMMITTED = [b"acct:42 balance=1000", b"acct:7 balance=250", b"acct:42 balance=900"]
TORN_RECORD = b"acct:7 balance=125000"
TORN_PAYLOAD_BYTES = 10            # power was lost after this many payload bytes


def naive_log_with_torn_tail(header_bytes_written: int, payload_bytes_written: int) -> bytes:
    """Committed records followed by a record the crash cut short."""
    data = b"".join(naive_encode(record) for record in COMMITTED)
    torn = naive_encode(TORN_RECORD)
    return data + torn[:header_bytes_written] + TORN_RECORD[:payload_bytes_written]


def framed_log(records: list[bytes]) -> bytes:
    return b"".join(encode_frame(record) for record in records)


class ReproduceProblem(unittest.TestCase):
    def test_torn_length_header_crashes_replay(self):
        data = naive_log_with_torn_tail(header_bytes_written=2, payload_bytes_written=0)
        with self.assertRaises(struct.error):
            naive_replay(data)

    def test_torn_payload_is_replayed_as_a_wrong_value(self):
        data = naive_log_with_torn_tail(header_bytes_written=4,
                                        payload_bytes_written=TORN_PAYLOAD_BYTES)
        replayed = naive_replay(data)
        self.assertEqual(replayed[:3], COMMITTED)
        self.assertEqual(replayed[3], b"acct:7 bal")      # garbage accepted as a record


class FixWithChecksummedFrames(unittest.TestCase):
    def test_intact_log_replays_every_record(self):
        result = recover(framed_log(COMMITTED))
        self.assertEqual(result.records, COMMITTED)
        self.assertEqual(result.discarded_bytes, 0)

    def test_every_possible_crash_point_keeps_exactly_the_committed_records(self):
        committed = framed_log(COMMITTED)
        torn_frame = encode_frame(TORN_RECORD)
        for cut in range(len(torn_frame)):
            with self.subTest(bytes_written=cut):
                result = recover(committed + torn_frame[:cut])
                self.assertEqual(result.records, COMMITTED)
                self.assertEqual(result.valid_bytes, len(committed))

    def test_flipped_byte_in_last_record_is_rejected(self):
        data = bytearray(framed_log(COMMITTED))
        data[-1] ^= 0xFF
        self.assertEqual(recover(bytes(data)).records, COMMITTED[:2])

    def test_zero_filled_tail_is_not_mistaken_for_empty_records(self):
        # Filesystems can expose preallocated zero bytes after a crash.
        result = recover(framed_log(COMMITTED) + bytes(64))
        self.assertEqual(result.records, COMMITTED)
        self.assertEqual(result.discarded_bytes, 64)

    def test_absurd_length_from_garbage_is_rejected(self):
        garbage = FRAME_HEADER.pack(MAX_RECORD_BYTES + 1, 0)
        self.assertEqual(recover(framed_log(COMMITTED) + garbage).records, COMMITTED)

    def test_genuine_empty_record_still_round_trips(self):
        self.assertEqual(recover(framed_log([b"", b"x"])).records, [b"", b"x"])

    def test_oversized_record_is_refused_at_write_time(self):
        with self.assertRaises(ValueError):
            encode_frame(bytes(MAX_RECORD_BYTES + 1))


class HalfWriteFile:
    """A log file whose write puts half the bytes on disk and then fails, like ENOSPC."""

    def __init__(self, path: str) -> None:
        self.real_file = open(path, "ab")

    def write(self, data: bytes) -> int:
        self.real_file.write(data[:len(data) // 2])
        self.real_file.flush()
        raise OSError("No space left on device")

    def flush(self) -> None:
        self.real_file.flush()

    def fileno(self) -> int:
        return self.real_file.fileno()

    def __enter__(self) -> "HalfWriteFile":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.real_file.close()


class FailingOnceOpener:
    """Opens the log normally, except the append number `failing_append`, which fails half-way."""

    def __init__(self, failing_append: int) -> None:
        self.failing_append = failing_append
        self.appends_seen = 0

    def __call__(self, path: str, mode: str):
        if mode == "ab":
            self.appends_seen += 1
            if self.appends_seen == self.failing_append:
                return HalfWriteFile(path)
        return open(path, mode)


class FixOnDisk(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.directory.name, "node.log")

    def tearDown(self) -> None:
        self.directory.cleanup()

    def write_crashed_log(self) -> None:
        with open(self.path, "wb") as log_file:
            log_file.write(framed_log(COMMITTED) + encode_frame(TORN_RECORD)[:7])

    def test_reopening_truncates_the_torn_tail(self):
        self.write_crashed_log()
        log = AppendOnlyLog(self.path)
        self.assertEqual(log.recovered.records, COMMITTED)
        self.assertEqual(os.path.getsize(self.path), len(framed_log(COMMITTED)))

    def test_appends_after_recovery_are_readable(self):
        self.write_crashed_log()
        log = AppendOnlyLog(self.path)
        log.append(b"acct:7 balance=300")
        self.assertEqual(AppendOnlyLog(self.path).records(), COMMITTED + [b"acct:7 balance=300"])

    def test_failed_append_does_not_hide_later_acknowledged_writes(self):
        log = AppendOnlyLog(self.path, open_file=FailingOnceOpener(failing_append=2))
        log.append(b"acct:7 balance=100")
        with self.assertRaises(OSError):
            log.append(b"acct:7 balance=200")
        log.append(b"acct:7 balance=250")
        expected = [b"acct:7 balance=100", b"acct:7 balance=250"]
        self.assertEqual(log.records(), expected)
        self.assertEqual(AppendOnlyLog(self.path).records(), expected)

    def test_failed_append_leaves_file_at_previous_size(self):
        log = AppendOnlyLog(self.path, open_file=FailingOnceOpener(failing_append=1))
        with self.assertRaises(OSError):
            log.append(b"acct:7 balance=200")
        self.assertEqual(os.path.getsize(self.path), 0)

    def test_new_log_file_starts_empty(self):
        self.assertEqual(AppendOnlyLog(self.path).records(), [])


if __name__ == "__main__":
    unittest.main()
