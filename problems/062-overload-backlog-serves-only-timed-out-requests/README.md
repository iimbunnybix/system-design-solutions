# 062 · After a 10-second traffic spike, the pricing service timed out every request for another 11 seconds

## Problem
A travel site's pricing service computes a fare in about 10 ms. Each shard
has one worker (100 requests/s), and the search page gives up on a price
after 1 second and shows "price unavailable". Requests that arrive while the
worker is busy wait in an in-memory FIFO queue with no limit.

A marketing push sent a 10-second spike of about 167 requests/s to one
shard. Dashboards then showed the worker 100% busy, yet nearly every search
showed "price unavailable", and the errors went on for **11.4 seconds after
the spike ended**, when traffic was back to half of capacity. In the
simulation of this 30-second window, only 680 of 2,667 requests got a price
in time; 1,987 were computed and thrown away because the caller had left.

## Root cause
An unbounded FIFO queue under overload turns into a backlog of requests
that are already dead:

- While arrivals exceed capacity, the queue grows without limit. Once a
  request has more than about 100 others ahead of it, it waits over a
  second, so every answer is computed after its caller timed out.
- The worker still does the full work for each dead request, so goodput
  (answers someone receives) collapses while utilisation looks perfect.
- After the spike, new requests sit behind the old backlog. In the
  simulation, the first post-spike request answered in time arrived at
  21.36 s; only 432 of the 1,000 post-spike requests got an answer.

## Solution
`DeadlineAwareServer`:

1. **Bound the queue by the deadline.** `max_queue_length` allows at most
   `timeout / service - 2` (98) waiting requests: the last one in the queue
   can still finish in time. When the queue is full, a new request is
   rejected immediately with "busy", which the caller can retry on another
   replica or show as unavailable without waiting a second.
2. **Drop expired requests instead of serving them.** When the worker takes
   a request whose answer would arrive after its deadline, it drops it
   without doing the work. This also protects a queue whose limit is set too
   high.

Same traffic, same capacity:

| | Answered in time | Answered too late | Rejected at once |
|---|---|---|---|
| Unbounded FIFO | 680 | 1,987 | 0 |
| Bounded, deadline-aware | 2,098 | 0 | 569 |

With only the deadline check (queue limit 10,000) the result is similar:
2,099 answered in time and 568 dropped before any work was done.

## Trade-offs
- Callers must send their deadline (a header such as gRPC's deadline) for
  the server to know when a request is dead; this model assumes one fixed
  client timeout.
- Fast rejections push the problem to callers: they must back off and not
  retry immediately, or rejected traffic comes straight back (see the
  retry-amplification problem).
- Alternatives: adaptive LIFO (serve the newest request first when the
  queue is long, since it is the one most likely still wanted), CoDel-style
  queues that cap time spent waiting rather than queue length, and priority
  load shedding when some requests matter more than others.

## Run
```bash
python -m unittest -v test_solution.py
```
