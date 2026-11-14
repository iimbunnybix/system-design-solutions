"""Recover an append-only log after a crash left a half-written record at its end.

A storage node appends every write to a log file and replays it on restart. The
original format was just "4-byte length, then payload". When the node lost power
mid-append, the tail held a torn record: replay either crashed on a short header
(so the node crash-looped) or silently returned a truncated payload as if it were
real data. This module keeps that version and adds a framed format with a CRC32
per record; recovery keeps every record up to the first frame that is incomplete
or fails its checksum, and truncates the file there so new appends stay readable.
"""
from __future__ import annotations

import os
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Callable

LENGTH_HEADER = struct.Struct(">I")            # original format: big-endian payload length
FRAME_HEADER = struct.Struct(">II")            # framed format: payload length, CRC32
MAX_RECORD_BYTES = 1024 * 1024                 # larger lengths can only be garbage


def naive_encode(payload: bytes) -> bytes:
    """Original record format: length prefix and payload, no integrity check."""
    return LENGTH_HEADER.pack(len(payload)) + payload


def naive_replay(data: bytes) -> list[bytes]:
    """Original replay: trusts every length it reads."""
    records: list[bytes] = []
    offset = 0
    while offset < len(data):
        # Raises struct.error if the crash cut the header short -> node cannot start.
        (length,) = LENGTH_HEADER.unpack_from(data, offset)
        offset += LENGTH_HEADER.size
        # Slicing past the end silently returns fewer bytes -> torn data looks real.
        records.append(data[offset:offset + length])
        offset += length
    return records


def frame_checksum(length_bytes: bytes, payload: bytes) -> int:
    """CRC32 over the length bytes and the payload.

    Covering the length matters: crc32(b"") is 0, so a checksum of the payload
    alone would accept a zero-filled tail (length 0, crc 0) as a valid record.
    """
    return zlib.crc32(length_bytes + payload)


def encode_frame(payload: bytes) -> bytes:
    """Framed record: length, CRC32 of (length + payload), payload."""
    if len(payload) > MAX_RECORD_BYTES:
        raise ValueError(f"record of {len(payload)} bytes exceeds {MAX_RECORD_BYTES}")
    length_bytes = LENGTH_HEADER.pack(len(payload))
    checksum = frame_checksum(length_bytes, payload)
    return FRAME_HEADER.pack(len(payload), checksum) + payload


@dataclass
class RecoveryResult:
    records: list[bytes]
    valid_bytes: int          # offset just past the last good frame
    discarded_bytes: int      # torn or corrupt bytes after it


def read_frame(data: bytes, offset: int) -> tuple[bytes, int] | None:
    """Decode the frame at `offset`; return (payload, next_offset) or None if it is not intact."""
    if offset + FRAME_HEADER.size > len(data):
        return None                                    # header itself was torn
    length, stored_checksum = FRAME_HEADER.unpack_from(data, offset)
    payload_start = offset + FRAME_HEADER.size
    payload_end = payload_start + length
    if length > MAX_RECORD_BYTES or payload_end > len(data):
        return None                                    # payload was torn or length is garbage
    payload = data[payload_start:payload_end]
    length_bytes = data[offset:offset + LENGTH_HEADER.size]
    if frame_checksum(length_bytes, payload) != stored_checksum:
        return None                                    # partially written or corrupted bytes
    return payload, payload_end


def recover(data: bytes) -> RecoveryResult:
    """Replay intact frames, stopping at the first one that is incomplete or corrupt.

    We stop rather than skip ahead: after a bad frame we cannot know where the next
    frame starts, and appends are sequential, so anything after a torn record was
    never acknowledged to a client.
    """
    records: list[bytes] = []
    offset = 0
    while offset < len(data):
        frame = read_frame(data, offset)
        if frame is None:
            break
        payload, offset = frame
        records.append(payload)
    return RecoveryResult(records, offset, len(data) - offset)


class AppendOnlyLog:
    """A file-backed log that repairs a torn tail when it is opened."""

    def __init__(self, path: str, open_file: Callable[[str, str], Any] = open) -> None:
        # open_file is injectable so tests can simulate a write that fails half-way.
        self.path = path
        self.open_file = open_file
        self.recovered = self._recover_file()

    def _recover_file(self) -> RecoveryResult:
        """Read the file, keep its intact prefix and cut off any torn tail."""
        if not os.path.exists(self.path):
            open(self.path, "wb").close()
        with open(self.path, "rb") as log_file:
            result = recover(log_file.read())
        if result.discarded_bytes:
            # Without truncating, new frames would be written after the garbage and
            # the next recovery would stop before reaching them.
            os.truncate(self.path, result.valid_bytes)
        return result

    def append(self, payload: bytes) -> None:
        """Append one record and fsync, so it is durable before we acknowledge it.

        If the write or fsync fails (ENOSPC, EIO) part of the frame may already be
        in the file. We cut the file back to its size before this append, otherwise
        the next successful append lands after the garbage and recovery, which stops
        at the first bad frame, would lose that acknowledged record.
        """
        frame = encode_frame(payload)
        size_before = os.path.getsize(self.path)
        try:
            with self.open_file(self.path, "ab") as log_file:
                log_file.write(frame)
                log_file.flush()
                os.fsync(log_file.fileno())
        except OSError:
            self._truncate_to(size_before)
            raise

    def _truncate_to(self, size: int) -> None:
        """Remove a partially written frame and make the shorter size durable."""
        with open(self.path, "r+b") as log_file:
            log_file.truncate(size)
            log_file.flush()
            os.fsync(log_file.fileno())

    def records(self) -> list[bytes]:
        """Every intact record currently in the file."""
        with open(self.path, "rb") as log_file:
            return recover(log_file.read()).records
