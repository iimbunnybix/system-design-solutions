# 073 · A customer's video sat at "processing 40%" for three days while its worker was deadlocked and still heartbeating

## Problem
A video platform transcodes uploads with a pool of workers. A coordinator
leases each job to one worker for 30 seconds, and every worker runs a
background heartbeat thread that renews its lease every 10 seconds. If a
worker crashes, its heartbeats stop, the lease expires, and another worker
picks the job up. That handled crashed and killed workers well.

Then a customer escalated: their video had been "processing" for three
days. The worker that held it was alive. Its main thread was deadlocked
inside the encoding library, but its heartbeat thread was perfectly healthy
and renewed the lease on schedule. No other worker could take the job, and
no dashboard showed anything wrong, because every lease looked fresh. A
later search found dozens of jobs stuck the same way.

## Root cause
**The heartbeat proved the process was alive, not that the work was
moving.** A separate heartbeat thread keeps running when the thread doing
the work is stuck in a deadlock, an infinite loop or a blocked call without
a timeout. The coordinator took "alive" to mean "working". In the model
(worker A hangs 20 seconds into a 50-second job, worker B is idle), the job
is **never** completed in 10 simulated minutes: A renews its lease forever
and B never gets it.

This differs from problem 025 (visibility timeout without heartbeats, where
slow jobs were redelivered while still running) and problem 004 (lease
fencing for a writer paused by GC). Here the heartbeat itself is what kept
a dead job alive.

## Solution
- **Heartbeats carry progress.** Each heartbeat reports a counter the work
  itself advances (frames encoded, bytes written). The coordinator records
  when it last changed.
- **No progress, no renewal.** If progress has not moved for more than
  60 seconds, the coordinator refuses to renew. The lease runs out on its
  own and the job becomes available to other workers. A slow worker that
  keeps making progress keeps its lease indefinitely.
- **Fencing tokens.** Every new lease gets a larger token. Renewals and
  completions must present the current token on an unexpired lease, so if
  the hung worker wakes up later, it cannot renew or report a result for a
  job that now belongs to someone else.

In the model, A's last progress is at 20 s. Its lease is renewed until 80 s,
the renewal at 90 s is refused, the lease expires at 110 s, and worker B
picks the job up and completes it at **160 s**.

## Trade-offs
- **The stall limit must exceed the longest honest pause.** A single huge
  frame or a slow upload of the output might show no progress for a while.
  Too short and healthy jobs bounce between workers. Too long and stuck jobs
  wait longer. Measure progress gaps and set the limit above their p99.9.
- **Progress must be reported by the work, not the heartbeat thread.** If
  the heartbeat thread computes "progress" from a clock, the bug comes back.
- **The hung worker is still hung.** Reassigning the job does not free its
  memory or CPU. Have the worker notice the refused renewal and restart
  itself, and alert on refused renewals: they mark a bug in the encoder.
- **At-least-once.** Two workers may both have done part of a job. Outputs
  must be written idempotently (e.g. to a path keyed by job and token) so a
  late write from the old worker is ignored.

## Run
```bash
python -m unittest -v test_solution.py
```
