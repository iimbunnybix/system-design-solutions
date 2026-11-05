# 025 · Customers got three "your video is ready" emails, and the transcoding queue never emptied

## Problem
A video platform transcodes uploads with a pool of workers that pull jobs
from an SQS-style queue. The queue's visibility timeout was 60 seconds,
chosen when most uploads were short clips. After the product started
accepting long 4K videos, jobs took several minutes. Customers received the
same "your video is ready" email several times, transcoding costs roughly
tripled, and the queue's "messages in flight" graph never went back to
zero, so autoscaling kept adding workers that did the same jobs again.

## Root cause
`receive` hides a message for a fixed visibility timeout; if it is not
deleted in that time it becomes visible again, because the queue assumes the
worker died. A job that runs longer than the timeout is therefore handed to
another worker **while the first one is still working**, then to a third,
and so on. Each delivery gets a new receipt handle and only the newest one
can delete the message, so the first workers' deletes fail and the message
stays in the queue.

In the simulation (3 workers, a 180-second job, 60-second timeout), the job
is completed **7 times in 10 minutes** and received 10 times, and the message
is still in the queue at the end.

## Solution
- **Heartbeat.** While working, a worker calls `extend(receipt, 60)` every
  20 seconds, a third of the timeout so that one late heartbeat is not
  fatal. The message stays hidden for as long as the worker is alive and
  making progress, however long the job is: the 180-second job is received
  once and completed once.
- **Crash recovery still works.** A worker that dies stops heart-beating,
  the lease runs out after 60 seconds, and another worker picks the job up
  (tested).
- **Stale receipts are rejected.** Each delivery gets a new receipt;
  `extend` and `delete` with an old one raise `LeaseLost`. A worker that
  froze past its lease (a long GC pause, a VM migration) learns this at its
  next heartbeat or at the lease check right before committing, and
  abandons the job instead of producing a duplicate (tested with a
  120-second pause).

## Trade-offs
- The lease check before committing narrows the window but cannot close it:
  the lease can expire between the check and the side effect. Make the side
  effect idempotent (dedup by message id, see the backend repo's
  idempotency and outbox problems) or use a fencing token.
- A worker that is alive but stuck (deadlocked, infinite loop) will keep
  heart-beating forever if the heartbeat runs on a separate thread. Tie the
  heartbeat to real progress, and cap total extensions (SQS caps visibility
  at 12 hours per receive).
- Simply raising the timeout to "longer than any job" makes crash recovery
  slow: a crashed worker's job waits the whole timeout before retrying.
- SQS's own `ChangeMessageVisibility` and Google Pub/Sub's lease extension
  ("modifyAckDeadline", done automatically by its client libraries)
  implement the same idea.

## Run
```bash
python -m unittest -v test_solution.py
```
