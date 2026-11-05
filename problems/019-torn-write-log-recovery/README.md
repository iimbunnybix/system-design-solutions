# 019 · A storage node lost power and then crash-looped on restart, and after a manual fix one account showed a wrong balance

## Problem
A small key-value store keeps each node's state in an append-only log: every
write is appended to `node.log`, fsynced, then acknowledged. On restart the
node replays the log to rebuild its in-memory table.

During a rack power cut, one node was mid-append. When power came back it
failed on startup with `struct.error: unpack_from requires a buffer of at
least ... bytes` and the orchestrator restarted it in a loop. An engineer
"fixed" it by hand-patching the file, after which it started, but replay had
picked up a half-written record: an account that should have read
`balance=250` (the last acknowledged write) now showed a truncated value that
no client had ever written. The replica was out of service for an hour and
had to be rebuilt from a peer.

## Root cause
The record format was only `4-byte length + payload`, and replay trusted it:

- If the crash cut the **length header** short, unpacking it raised an
  exception, so the node could never start (`test_torn_length_header_crashes_replay`).
- If the crash cut the **payload** short, Python's slicing quietly returned the
  bytes that were there, so `acct:7 balance=125000` was replayed as the record
  `acct:7 bal` (`test_torn_payload_is_replayed_as_a_wrong_value`).

Nothing in the format could tell a complete record from a torn or corrupted
one. Disks only promise that sector-sized writes are atomic; a multi-sector
append can be partly on disk after power loss.

## Solution
Every record is written as a **frame**: `length (4 bytes) | CRC32 (4 bytes) |
payload`. Recovery (`recover`) reads frames in order and stops at the first one
that is incomplete, has an impossible length (over `MAX_RECORD_BYTES`), or fails
its checksum. Everything before that point is kept; everything after is
discarded.

- **Why stop instead of skipping?** Appends are sequential and each is fsynced
  before it is acknowledged, so a torn frame can only be the last write, which
  was never acknowledged. After a bad frame there is also no reliable way to
  find where the next frame would start.
- **The CRC covers the length bytes too.** `crc32(b"")` is 0, so a checksum of
  the payload alone would accept a zero-filled tail (length 0, CRC 0) as an
  endless run of empty records. Covering the length rejects it.
- **`AppendOnlyLog` truncates the file** to the last good frame when it opens.
  Otherwise new appends would land after the garbage and the next recovery
  would stop before reaching them.

The test `test_every_possible_crash_point_keeps_exactly_the_committed_records`
cuts the 29-byte torn frame at every byte from 0 to 28 and checks that the
three committed records (81 bytes) are always exactly what is recovered.

## Trade-offs
- CRC32 detects torn writes and random corruption, not deliberate tampering;
  use an HMAC or a hash chain if an attacker can edit the file.
- Stopping at the first bad frame also discards any good records after
  **mid-file** corruption (bit rot). Databases handle that separately with
  page checksums and repair from replicas; this design assumes only the tail
  can be torn.
- An fsync per append is slow. Real systems batch several records per fsync
  (group commit) and acknowledge them together; framing works the same way.
- Alternatives: write to a new file and atomically `rename` it (good for
  snapshots, not for a hot log), or use SQLite/RocksDB, whose WAL already does
  all of this.

## Run
```bash
python -m unittest -v test_solution.py
```
