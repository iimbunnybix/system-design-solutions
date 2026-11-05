# 018 · Reminder notifications started arriving minutes late, and got later every week as the queue grew

## Problem
A notifications service schedules delayed jobs: "remind me in 2 hours",
"cancel unpaid order after 30 minutes", "retry this webhook in 5 minutes".
Jobs are created with a due time and a scheduler loop runs once a second to
fire whatever is due.

While the queue held tens of thousands of jobs nobody noticed. As usage grew
to millions of pending jobs (most of them days away), each one-second loop
took longer than a second. The scheduler fell behind, reminders arrived
minutes late, unpaid orders held stock longer than allowed, and adding CPU only
delayed the problem.

## Root cause
Every tick **scanned every pending job** to ask "are you due yet?". The work
per tick is proportional to the size of the queue, not to the number of jobs
actually due. In the simulation, 20,000 pending jobs spread over two days and
60 ticks cost **1,199,543** due-time checks to fire **10** jobs. Ten times as
many pending jobs means ten times as much work per tick, even when nothing is
due.

## Solution
A **hierarchical timing wheel** (`TimingWheelScheduler`), the structure used by
Kafka's purgatory and Netty's `HashedWheelTimer`:

- **Seconds wheel:** 60 slots. A job less than a minute away goes in slot
  `due % 60`, which is visited exactly at its due second.
- **Minutes wheel:** 60 slots for jobs 1–60 minutes away, filed by due minute.
  At each minute boundary only that minute's slot is "cascaded" into the
  seconds wheel.
- **Overflow heap:** jobs an hour or more away wait in a min-heap. Each tick
  peeks at the heap's top (O(1) when nothing is close) and moves jobs into the
  wheel once they are within the one-hour horizon.

A tick now touches only the jobs that are due, plus the jobs moving down a
level. For the same 20,000 jobs and 60 ticks it examined **24** jobs (10 fired,
the rest cascaded or pulled from the heap) and fired the same 10. Tests check
that, over two hours, the wheel fires exactly the same jobs at exactly the same
ticks as the naive scanner, and that every job across all three levels fires at
its due tick, including jobs on the 59/60/61 and 3599/3600/3601-second
boundaries and jobs added mid-run.

## Trade-offs
- **Resolution is one tick** (one second here). Jobs fire at the start of their
  due second, never earlier; sub-second timers need a finer wheel.
- A minute boundary does a burst of work proportional to that minute's jobs;
  spreading due times or using more, smaller levels smooths it.
- A plain min-heap of all jobs is also a valid fix (O(log n) per schedule and
  fire, simpler code). Wheels win when there are many timers that are often
  cancelled or rescheduled, because insert is O(1); cancellation is not
  implemented here and would need a job-id → slot map or a "cancelled" flag.
- This is a single-process in-memory structure. For durability, the jobs
  table stays the source of truth and the wheel holds only jobs due within the
  next window, reloaded on startup (or use a store with indexed due-time
  queries, such as a sorted set keyed by due time).

## Run
```bash
python -m unittest -v test_solution.py
```
