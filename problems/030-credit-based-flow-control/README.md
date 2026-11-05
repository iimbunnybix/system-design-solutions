# 030 · Fraud scoring was OOM-killed 16 seconds into a sale, and 16,400 orders were never checked

## Problem
Checkout writes every order event to a durable log. A relay service reads the
log and pushes each event over a long-lived stream to the fraud-scoring
service, which keeps received events in an in-memory queue and scores about
1,000 events per second. The relay marks an event as delivered as soon as it
has sent it.

At the start of a flash sale, orders jumped to 2,000 events per second for
30 seconds. The relay kept up easily, since reading and forwarding is cheap,
but scoring could not. The scoring service's queue grew by 1,000 events every
second, latency for a fraud decision climbed to 8 seconds, and after 16
seconds the pod hit its 64 MiB memory limit and was OOM-killed. The 16,400
events in its queue were gone, and because the relay had already marked them
delivered, nothing resent them. Those orders shipped without a fraud check,
and the team spent the next day replaying the log by hand to find them.

## Root cause
**No backpressure between a fast producer and a slow consumer.** The relay
sends as fast as it can read, and the consumer accepts everything into an
unbounded queue. Whenever the input rate is above the consumer's rate, the
difference piles up in the consumer's memory, so latency and memory both grow
for as long as the burst lasts. The durable log, which could hold the backlog
safely, was emptied straight into RAM.

The model (`simulate_unbounded_push`, 100 ms ticks, 4 KiB per event in
memory) reproduces it: the consumer is killed at **16.2 s** with **16,400**
events lost, after the worst latency reached **8.1 s**. A bigger memory limit
only moves the crash later; it cannot stop a queue that grows without bound.

This is not load shedding (problem 016, which drops low-priority requests
under overload) or retry amplification (problem 027). Here no request is
retried or dropped by design; the consumer simply accepted more than it could
hold.

## Solution
**Credit-based flow control.** The consumer decides how much the producer may
send:

- `CreditedConsumer` has a fixed buffer of 200 slots (two ticks of work). It
  grants one credit per slot that is neither full nor already promised, so
  `buffer + outstanding credits <= 200` at all times.
- `CreditedProducer` sends one event per credit and stops when it runs out.
  An event sent without credit is refused as a protocol error.
- Events that cannot be sent yet **stay in the durable log**, which is built
  to hold a backlog. As the consumer scores events it frees slots and grants
  new credits, so sending resumes at exactly the consumer's pace.

Same traffic (30 s at 2,000/s, then 500/s, 2 minutes simulated):

| Design | Consumer memory | Events lost | Worst latency | Outcome |
|---|---|---|---|---|
| Unbounded push | over 64 MiB | 16,400 | 8.1 s, then crash | OOM kill at 16.2 s |
| Credit-based | 800 KiB (200 events) | 0 | 30.0 s | caught up at 89.7 s |

With credits, the burst turns into a backlog in the log that peaks at 29,900
events and drains once traffic falls below the consumer's rate. All 105,000
events are scored, none twice.

## Trade-offs
- **Credits do not add capacity.** Lag still reached 30 s. Flow control
  turns a crash and data loss into visible lag. Alert on log backlog and
  autoscale the consumer (or shed optional work) when lag grows.
- **Window size:** credits must cover at least one round trip of work, or
  the consumer sits idle waiting for the next batch. Too large a window brings
  back the memory problem. Here 200 slots is two ticks of scoring.
- **Lost credit messages** would slowly shrink the window. Real protocols
  (HTTP/2 and gRPC flow-control windows, AMQP credit, Reactive Streams
  `request(n)`) send cumulative totals, not deltas, so a lost update is fixed
  by the next one.
- **Only works if the producer can wait.** Here the backlog sits in a durable
  log. A producer that receives live requests it cannot buffer has to reject
  them (429/503) instead, which is load shedding.
- **Alternative:** a bounded queue that blocks or rejects when full gives the
  same memory bound with less protocol. Credits are useful when the producer
  and consumer are separate services and the producer must know how much
  to send before sending it.

## Run
```bash
python -m unittest -v test_solution.py
```
