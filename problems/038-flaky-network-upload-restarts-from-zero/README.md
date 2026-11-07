# 038 · Large video uploads from phones kept restarting at 0% and most never finished, while users' mobile data ran out

## Problem
A video-sharing app uploaded each recording to the API in a single HTTP
`POST`. Files of 1-2 GB were common. On mobile networks the connection
drops regularly (switching cells, going through a tunnel, the app moving to
the background), and every drop discarded the partial body on the server, so
the app restarted the upload from the first byte. Support tickets said
uploads "sat at 80% and then started over"; the upload success rate for
large files fell below 15%, and users on capped data plans were billed for
gigabytes that never produced a video.

The reproduction scales this down but keeps the ratio: an 8 MiB file over a
link that drops on average every 1.6 MiB (one fifth of the file), 100
uploads with seeds 0-99, at most 10 attempts. Only **11 of 100** uploads
finish, and on average each upload sends **1.93x** the file size, most of it
for uploads that fail anyway.

## Root cause
**The unit of retry was the whole file.** With drops arriving at random at
an average rate, the chance that one attempt of length `L` survives is
`exp(-L / mean_distance_between_drops)` (here `exp(-5)` ≈ 0.7%). It falls
exponentially with file size, so retrying harder does not help: the larger
the file, the less likely any attempt gets through. The server had no way to
keep the bytes it had already received.

## Solution
A **resumable upload session** (`ResumableUploadServer` and
`resumable_upload` in `solution.py`), the same design as the tus protocol
and the resumable upload APIs of Google Cloud Storage and S3 multipart:

1. The client creates a session declaring the file size and its SHA-256.
2. The file is sent in 256 KiB chunks, each with an explicit **offset** and
   its own checksum. The server stores the chunk and advances its offset.
3. After a drop the client asks the server for its offset and continues from
   there; only the interrupted chunk is resent.
4. A chunk sent for the wrong offset (typically a retry whose reply was
   lost) is refused with the server's real offset, so bytes are never
   appended twice (tested).
5. Corrupted chunks are rejected, and the assembled file is verified against
   the declared checksum before it is published (tested).
6. The app saves the upload id, so after being killed it resumes the same
   session without resending stored bytes (tested: 5 of 8 chunks stored,
   the second run sends only the last 3).

Result on the same 100 uploads: **100 of 100** finish, sending on average
**1.09x** the file size.

## Trade-offs
- Chunk size balances waste against overhead: a drop wastes at most one
  chunk, but every chunk is a request with headers, a checksum and a
  server-side write. Real clients use a few MiB on mobile and adapt the size
  to measured throughput.
- The server must keep incomplete sessions somewhere durable (object store
  parts, a temp file) and expire abandoned ones, or storage leaks; S3 needs a
  lifecycle rule to abort incomplete multipart uploads for this reason.
- Sequential chunks are simple and give a single offset. Parallel chunk
  uploads (S3 multipart) are faster on good networks but need per-part
  tracking instead of one offset.
- Chunk checksums catch corruption in transit; TLS already protects against
  most of it, so the whole-file checksum is the more important one (it also
  catches client bugs that resume with the wrong bytes).
- Authorization must be tied to the session: anyone holding an upload id
  should not be able to append to someone else's file.

## Run
```bash
python -m unittest -v test_solution.py
```
