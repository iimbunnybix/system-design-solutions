import random
import unittest

from solution import (MAX_CONSECUTIVE_DROPS, MIB, ChecksumMismatch, FlakyLink,
                      OffsetMismatch, Outcome, ResumableUploadServer, naive_upload,
                      resumable_upload, sha256_hex)

FILE_SIZE = 8 * MIB
MEAN_BYTES_BETWEEN_DROPS = FILE_SIZE // 5     # a drop every 1/5 of the file on average
CHUNK_SIZE = 256 * 1024
UPLOADS = 100
NAIVE_MAX_ATTEMPTS = 10


def make_file(size: int = FILE_SIZE) -> bytes:
    return random.Random(1).randbytes(size)


class ScriptedLink:
    """Fake link that returns a fixed list of outcomes, then always delivers."""

    def __init__(self, outcomes: list[Outcome]) -> None:
        self.outcomes = list(outcomes)
        self.bytes_sent = 0

    def transmit(self, size: int) -> Outcome:
        self.bytes_sent += size
        if self.outcomes:
            return self.outcomes.pop(0)
        return Outcome.DELIVERED


def run_fleet(upload) -> tuple[int, float]:
    """Upload the same file over 100 seeded flaky links.

    Returns (completed uploads, average bytes sent as a multiple of the file size).
    """
    data = make_file()
    completed = 0
    bytes_sent = 0
    for seed in range(UPLOADS):
        result = upload(data, FlakyLink(MEAN_BYTES_BETWEEN_DROPS, seed))
        completed += result.completed
        bytes_sent += result.bytes_sent
    return completed, bytes_sent / UPLOADS / FILE_SIZE


def naive(data, link):
    return naive_upload(data, link, NAIVE_MAX_ATTEMPTS)


def resumable(data, link):
    return resumable_upload(data, ResumableUploadServer(), link, chunk_size=CHUNK_SIZE)[1]


class ReproduceProblem(unittest.TestCase):
    def test_most_single_request_uploads_never_finish(self):
        completed, _ = run_fleet(naive)
        self.assertEqual(completed, 11)

    def test_failed_uploads_still_burn_almost_twice_the_file_size(self):
        _, average_sent = run_fleet(naive)
        self.assertAlmostEqual(average_sent, 1.93, places=2)


class FixWithResumableUpload(unittest.TestCase):
    def test_every_upload_finishes_with_little_extra_data(self):
        completed, average_sent = run_fleet(resumable)
        self.assertEqual(completed, UPLOADS)
        self.assertAlmostEqual(average_sent, 1.09, places=2)

    def test_server_stores_exactly_the_original_bytes(self):
        data = make_file()
        server = ResumableUploadServer()
        upload_id, result = resumable_upload(
            data, server, FlakyLink(MEAN_BYTES_BETWEEN_DROPS, 0), chunk_size=CHUNK_SIZE)
        self.assertTrue(server.sessions[upload_id].complete)
        self.assertEqual(bytes(server.sessions[upload_id].received), data)

    def test_lost_reply_does_not_duplicate_the_chunk(self):
        data = make_file(4 * CHUNK_SIZE)
        server = ResumableUploadServer()
        link = ScriptedLink([Outcome.DELIVERED, Outcome.REPLY_LOST])
        upload_id, result = resumable_upload(data, server, link, chunk_size=CHUNK_SIZE)
        self.assertTrue(result.completed)
        self.assertEqual(bytes(server.sessions[upload_id].received), data)

    def test_resent_chunk_at_old_offset_is_refused_with_current_offset(self):
        server = ResumableUploadServer()
        data = b"abcdef"
        upload_id = server.create_session(len(data), sha256_hex(data))
        server.put_chunk(upload_id, 0, b"abc", sha256_hex(b"abc"))
        with self.assertRaises(OffsetMismatch) as raised:
            server.put_chunk(upload_id, 0, b"abc", sha256_hex(b"abc"))
        self.assertEqual(raised.exception.current_offset, 3)

    def test_corrupted_chunk_is_rejected_and_offset_unchanged(self):
        server = ResumableUploadServer()
        upload_id = server.create_session(6, sha256_hex(b"abcdef"))
        with self.assertRaises(ChecksumMismatch):
            server.put_chunk(upload_id, 0, b"abX", sha256_hex(b"abc"))
        self.assertEqual(server.current_offset(upload_id), 0)

    def test_assembled_file_with_wrong_checksum_is_not_published(self):
        server = ResumableUploadServer()
        upload_id = server.create_session(3, sha256_hex(b"xyz"))
        with self.assertRaises(ChecksumMismatch):
            server.put_chunk(upload_id, 0, b"abc", sha256_hex(b"abc"))
        self.assertFalse(server.sessions[upload_id].complete)
        self.assertEqual(server.current_offset(upload_id), 0)

    def test_chunk_past_declared_size_is_rejected(self):
        server = ResumableUploadServer()
        upload_id = server.create_session(2, sha256_hex(b"ab"))
        with self.assertRaises(ValueError):
            server.put_chunk(upload_id, 0, b"abc", sha256_hex(b"abc"))

    def test_empty_file_is_complete_immediately(self):
        server = ResumableUploadServer()
        upload_id, result = resumable_upload(b"", server, ScriptedLink([]))
        self.assertTrue(result.completed)
        self.assertTrue(server.sessions[upload_id].complete)

    def test_client_gives_up_after_too_many_drops_in_a_row(self):
        data = make_file(4 * CHUNK_SIZE)
        link = ScriptedLink([Outcome.DROPPED] * MAX_CONSECUTIVE_DROPS)
        _, result = resumable_upload(data, ResumableUploadServer(), link, chunk_size=CHUNK_SIZE)
        self.assertFalse(result.completed)

    def test_restarted_app_resumes_without_resending_stored_bytes(self):
        data = make_file(8 * CHUNK_SIZE)
        server = ResumableUploadServer()
        # First run: 5 chunks get through, then the network is gone.
        first_link = ScriptedLink([Outcome.DELIVERED] * 5 + [Outcome.DROPPED] * MAX_CONSECUTIVE_DROPS)
        upload_id, first = resumable_upload(data, server, first_link, chunk_size=CHUNK_SIZE)
        self.assertFalse(first.completed)
        # Second run after an app restart, with the saved upload id.
        second_link = ScriptedLink([])
        _, second = resumable_upload(data, server, second_link, upload_id, CHUNK_SIZE)
        self.assertTrue(second.completed)
        self.assertEqual(second_link.bytes_sent, 3 * CHUNK_SIZE)
        self.assertEqual(bytes(server.sessions[upload_id].received), data)


if __name__ == "__main__":
    unittest.main()
