# 078 · Password-reset emails sent at night arrived the next morning, and some never arrived at all

## Problem
A consumer app's services publish events (password resets, login alerts,
fraud signals) through a shared producer library that batches messages
before sending them to the event broker, to cut per-request overhead. The
library sent a batch once it held 500 messages. During the day, at about
1,000 events a second per instance, a batch filled in half a second.

Support then received complaints from users in a smaller region: password
reset emails requested in the evening arrived the next morning, and fraud
alerts for overnight card use reached the risk team hours late. Events from
the quietest hours after midnight sometimes never arrived: a deploy restarted
the instances before their batches filled. The broker and consumers showed
no lag, because the messages had never reached them.

## Root cause
The only flush condition was **batch size**. At three messages a minute a
500-message batch takes almost three hours to fill, so the first message in it
waits **9,980 seconds** (499 × 20 s) in the producer's memory. Nothing
flushed a partly full batch: there was no timer, and shutdown discarded the
pending list. In the reproduction 100 night-time messages are all still
pending, and none are delivered even after `close()`.

## Solution
`BatchingProducer` flushes a batch when it is **full or old**:
- It remembers when the oldest pending message arrived and flushes once that
  message has waited `LINGER_SECONDS` (2 s), the same idea as Kafka's
  `linger.ms`. Age counts from the *oldest* message, so a slow trickle
  cannot keep pushing the flush back.
- A background `tick()` (every 0.5 s here) checks the age even when no new
  messages arrive, which is the case that matters at night.
- `close()` flushes the remainder before the process exits.

At night every message now arrives within 2 s (linger plus at most one
tick). At daytime volume batches still fill in 0.5 s and go out full, so
the overhead saving is kept. Order within the producer is preserved.

## Trade-offs
- Linger sets a latency floor for quiet periods and a batch-size ceiling for
  busy ones; pick it from the latency the slowest consumer can accept.
  Kafka's own defaults lean the other way (very small linger), trading batch
  efficiency for latency.
- `close()` only helps on a graceful shutdown. A crash still loses what is
  in memory; events that must not be lost (password resets) need the
  transactional outbox pattern or a synchronous send with acknowledgement.
- The timer adds a thread or event-loop task to every producer; it must be
  stopped cleanly and must not flush concurrently with `send()` without a
  lock (this single-threaded model does not need one).

## Run
```bash
python -m unittest -v test_solution.py
```
