"""Resumable chunked uploads that survive dropped connections.

The video upload API accepted a whole file in one HTTP request. On mobile
networks a connection drops every few hundred megabytes on average, and any
drop threw the partial body away, so the app restarted from byte 0. Large
files almost never finished, and the retries burned users' data plans.

The fix is an upload session (the same idea as the tus protocol and the
resumable upload APIs of S3 and Google Cloud Storage): the server remembers
how many bytes it has durably received, the client sends the file in chunks
at an explicit offset, and after a drop it asks for the offset and continues
from there. Per-chunk and whole-file checksums reject corrupted data.
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from enum import Enum

MIB = 1024 * 1024
DEFAULT_CHUNK_SIZE = 1 * MIB
MAX_CONSECUTIVE_DROPS = 8       # give up on a chunk after this many drops in a row


class Outcome(Enum):
    DELIVERED = "delivered"     # request reached the server and the reply came back
    DROPPED = "dropped"         # connection died mid-request; server got nothing usable
    REPLY_LOST = "reply_lost"   # server processed the request, the reply was lost


class FlakyLink:
    """Fake mobile connection that drops after a random number of bytes.

    Bytes between drops follow an exponential distribution, so a drop is
    equally likely at any moment, like a train going into a tunnel.
    """

    def __init__(self, mean_bytes_between_drops: int, seed: int) -> None:
        self.rng = random.Random(seed)
        self.mean_bytes_between_drops = mean_bytes_between_drops
        self.bytes_until_drop = self._draw()
        self.bytes_sent = 0     # includes bytes wasted on dropped requests

    def _draw(self) -> float:
        return self.rng.expovariate(1 / self.mean_bytes_between_drops)

    def transmit(self, size: int) -> Outcome:
        """Try to send one request body of `size` bytes."""
        if size <= self.bytes_until_drop:
            self.bytes_until_drop -= size
            self.bytes_sent += size
            return Outcome.DELIVERED
        # The bytes before the drop were sent (and paid for) but are useless.
        self.bytes_sent += int(self.bytes_until_drop)
        self.bytes_until_drop = self._draw()
        return Outcome.DROPPED


@dataclass
class UploadResult:
    completed: bool
    attempts: int
    bytes_sent: int


def naive_upload(data: bytes, link: FlakyLink, max_attempts: int) -> UploadResult:
    """Original client: send the whole file in one request, restart on failure."""
    for attempt in range(1, max_attempts + 1):
        if link.transmit(len(data)) is Outcome.DELIVERED:
            return UploadResult(True, attempt, link.bytes_sent)
    return UploadResult(False, max_attempts, link.bytes_sent)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class OffsetMismatch(Exception):
    """The client sent a chunk for an offset the server is not at."""

    def __init__(self, current_offset: int) -> None:
        super().__init__(f"server is at offset {current_offset}")
        self.current_offset = current_offset


class ChecksumMismatch(Exception):
    """A chunk or the assembled file does not match its declared checksum."""


@dataclass
class UploadSession:
    total_size: int
    file_sha256: str
    received: bytearray
    complete: bool = False


class ResumableUploadServer:
    """Keeps a durable byte offset per upload session."""

    def __init__(self) -> None:
        self.sessions: dict[str, UploadSession] = {}
        self._next_id = 1

    def create_session(self, total_size: int, file_sha256: str) -> str:
        """Start an upload; the client declares the size and file checksum up front."""
        if total_size < 0:
            raise ValueError("total_size must be non-negative")
        upload_id = f"upload-{self._next_id}"
        self._next_id += 1
        self.sessions[upload_id] = UploadSession(total_size, file_sha256, bytearray())
        if total_size == 0:
            self._finish(self.sessions[upload_id])
        return upload_id

    def current_offset(self, upload_id: str) -> int:
        """How many bytes are stored; the client resumes from here."""
        return len(self.sessions[upload_id].received)

    def put_chunk(self, upload_id: str, offset: int, chunk: bytes, chunk_sha256: str) -> int:
        """Append one chunk at `offset` and return the new offset."""
        session = self.sessions[upload_id]
        # A chunk for the wrong offset is usually a retry of a chunk whose
        # reply was lost. Appending it again would duplicate bytes, so refuse
        # and tell the client where we really are.
        if offset != len(session.received):
            raise OffsetMismatch(len(session.received))
        if offset + len(chunk) > session.total_size:
            raise ValueError("chunk goes past the declared file size")
        if sha256_hex(chunk) != chunk_sha256:
            raise ChecksumMismatch("chunk corrupted in transit")
        session.received.extend(chunk)
        if len(session.received) == session.total_size:
            self._finish(session)
        return len(session.received)

    def _finish(self, session: UploadSession) -> None:
        """Verify the whole file before publishing it."""
        if sha256_hex(bytes(session.received)) != session.file_sha256:
            # Throw the bytes away so the client can upload again cleanly.
            session.received.clear()
            raise ChecksumMismatch("assembled file does not match its checksum")
        session.complete = True


def _send_chunk(server: ResumableUploadServer, upload_id: str, offset: int,
                chunk: bytes, outcome: Outcome) -> int | None:
    """Deliver one chunk according to the network outcome.

    Returns the new offset if the client learned it, or None if the client
    must ask the server (drop or lost reply).
    """
    if outcome is Outcome.DROPPED:
        return None
    try:
        new_offset = server.put_chunk(upload_id, offset, chunk, sha256_hex(chunk))
    except OffsetMismatch as mismatch:
        new_offset = mismatch.current_offset
    if outcome is Outcome.REPLY_LOST:
        return None
    return new_offset


def resumable_upload(data: bytes, server: ResumableUploadServer, link: FlakyLink,
                     upload_id: str | None = None,
                     chunk_size: int = DEFAULT_CHUNK_SIZE) -> tuple[str, UploadResult]:
    """Fixed client: upload in chunks, resume from the server's offset after a drop.

    Pass the `upload_id` of an earlier, unfinished run (saved by the app) to
    continue it after the app was killed.
    """
    if upload_id is None:
        upload_id = server.create_session(len(data), sha256_hex(data))
    offset = server.current_offset(upload_id)
    attempts = 0
    consecutive_drops = 0
    while offset < len(data):
        chunk = data[offset:offset + chunk_size]
        attempts += 1
        new_offset = _send_chunk(server, upload_id, offset, chunk, link.transmit(len(chunk)))
        if new_offset is not None:
            offset = new_offset
            consecutive_drops = 0
            continue
        consecutive_drops += 1
        if consecutive_drops >= MAX_CONSECUTIVE_DROPS:
            return upload_id, UploadResult(False, attempts, link.bytes_sent)
        # A small status request; the bytes already stored are not resent.
        offset = server.current_offset(upload_id)
    return upload_id, UploadResult(True, attempts, link.bytes_sent)
