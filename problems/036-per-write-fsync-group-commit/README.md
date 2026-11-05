# 036 · Order events piled up for seconds at the lunch peak, though the log server's CPU and disk bandwidth were nearly idle

## Problem
An order service writes every state change (created, paid, packed, shipped)
to an append-only event log before acknowledging it to the caller; the log
is the source of truth for downstream services. To guarantee no event is
lost on a crash, each write is appended and then `fsync`ed before the
acknowledgement. On a normal day the log takes a few hundred writes per
second. At the lunch peak traffic reached about 5,000 writes/s: acknowledgement
latency climbed to several seconds, callers timed out and retried (adding
more writes), and checkout slowed down, while the log server showed low CPU
and the disk was moving only a few MB/s.

In the reproduction (5,000 writes evenly spread over one second, 2 ms per
fsync) the log performs **5,000 fsyncs**, needs **10 s** to acknowledge one
second of traffic, and the median / p99 acknowledgement latency is
**4.5 s / 8.9 s**.

## Root cause
**Throughput was bounded by fsync latency, not bandwidth.** An fsync waits
for the storage device to confirm the data is on stable media; its cost is
mostly a fixed round trip (2 ms here), almost independent of how many bytes
it covers. With one fsync per write and one disk, the log can never exceed
1 / 2 ms = **500 writes/s**. Above that rate the queue grows without bound,
and every added write makes all later writes wait longer.

## Solution
**Group commit** (`GroupCommitLog` in `solution.py`), the technique used by
for example by PostgreSQL's WAL and MySQL's redo log and binlog:

1. Writes are queued instead of each flushing on its own.
2. When the disk is free, the log writes **every waiting record** (up to
   `max_batch` = 256) and issues **one** fsync for the whole batch.
3. Every record in the batch is acknowledged only after that fsync returns,
   so the durability guarantee is unchanged: an acknowledged write is on
   stable storage (tested: acknowledged records are always in the durable
   set, in submission order).
4. Writes that arrive while an fsync is running simply ride the next one.
   No timer delays a write to build bigger batches, so at quiet times every
   write still gets its own immediate fsync (tested at 400 writes/s:
   400 fsyncs, 2 ms latency).

Result at 5,000 writes/s: **501 fsyncs**, all writes acknowledged by
**1.002 s**, median / p99 latency **2.8 ms / 3.8 ms**, and no write waits
longer than two fsyncs (4 ms). Batches grow automatically with load, so the
same code handles a burst of 1,000 simultaneous writes in 4 fsyncs.

## Trade-offs
- Individual writes can wait up to one extra fsync (2 ms → up to 4 ms here)
  in exchange for 10x throughput. Some systems add a tiny deliberate wait
  (PostgreSQL's `commit_delay`) for even bigger batches; that helps only
  under heavy load and hurts latency otherwise.
- A real implementation needs concurrency: one writer thread (or the first
  waiting request, the "leader") performs the flush while the others wait on
  a condition variable or future for their batch. The simulation here models
  that timing with a fake clock.
- `max_batch` bounds the size and time of one flush; a byte limit is often
  more useful than a record count.
- Dropping fsync altogether, or acknowledging before it (async commit,
  `synchronous_commit = off`), gives more speed but loses acknowledged writes
  on a crash; group commit keeps the guarantee.
- Batching only helps writes that share one log/disk. If writes are spread
  over many independent logs, each still pays its own fsync.

## Run
```bash
python -m unittest -v test_solution.py
```
