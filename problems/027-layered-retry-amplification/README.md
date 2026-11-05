# 027 · A three-second database slowdown turned into a full outage that only ended when traffic was cut

## Problem
An order API runs as three layers: an API gateway, an order service, and a
data-access library that talks to the database. Each team added "safe"
retries on its own: the gateway retries a failed upstream call 3 times, the
service retries a failed repository call 3 times, and the data-access library
retries a failed query 3 times. One evening the database stalled for about
three seconds (a lock-heavy migration). The stall ended, but the database
stayed pinned at 100% and the API kept returning errors for every request.
It only recovered when the gateway was switched to maintenance mode to cut
traffic, then slowly re-opened. Checkout was down for the whole time.

## Root cause
**Retries at each layer multiply.** A user request that keeps failing makes
1 + 3 = 4 attempts at the gateway, each of which makes 4 service attempts,
each of which makes 4 database calls: 4 × 4 × 4 = **64 database calls for one
request** (`test_one_failing_request_becomes_64_database_calls`).

The simulation sends 100 requests per second to a database that finishes
150 calls per second, dropping to 50 per second for seconds 10-12. A call
that cannot start within its second times out at the client, but the
database still runs it later, so abandoned calls keep using capacity.

- With 3 retries in every layer, the first blip second already sees 3,250
  database calls. From then on every request fails and makes 6,400 calls per
  second, against a capacity of 150. When capacity returns, the queue of
  abandoned work is already larger than a second's capacity, so new calls
  still time out, get retried, and add to the queue. After 40 seconds the
  queue holds 184,650 abandoned calls and the success rate is still 0. The
  outage sustains itself.
- Retrying in **only one layer** (4 calls per request) is not enough on its
  own: 400 calls per second is still more than 150, the queue grows to 7,650
  and the service never recovers.

## Solution
`budgeted_stack` changes three things:

1. **Retry at exactly one layer.** Only the data-access client retries (up
   to 3 times). The gateway and service pass failures through.
2. **Per-client retry budget** (`RetryBudget`). Over the last 1,000 calls,
   retries may be at most 10% of first attempts. In normal operation that is
   plenty for isolated failures. During an overload it caps extra load at
   about 10%, whatever the per-request retry limit says.
3. **Do-not-retry signalling.** When the retrying layer gives up (budget spent
   or attempts used), it returns the failure marked `do_not_retry`, and
   `call_with_retries` never retries such a failure. Even if a team later
   adds retries to the gateway again, an overloaded backend still gets one
   call per request (tested with 3 retries at every layer and an empty
   budget).

Same simulation (100 requests/s, 40 s, blip at seconds 10-12):

| Setup | Peak DB calls/s | Largest queue | Requests OK at 40 s | Total DB calls |
|---|---|---|---|---|
| 3 retries at every layer | 6,400 | 184,650 | 0 / 100 | 189,850 |
| 3 retries at one layer, no budget | 400 | 7,650 | 0 / 100 | 12,850 |
| One layer + 10% budget + do-not-retry | 191 | 241 | 100 / 100 | 4,091 |

With the fix, requests still fail during the blip and for a few seconds
after it while the queue drains, but by second 17 (five seconds after the
blip ends) every request succeeds again and the queue is empty.

## Trade-offs
- **Fewer retries means more visible errors during short blips.** A request
  that a deep retry chain would eventually have saved now fails fast. That is
  the right trade under overload, but callers must handle the error
  (show "try again", queue the work).
- **Choosing the layer.** Retrying closest to the failing dependency is
  usually best, because that layer knows whether the call is idempotent and
  what the error means. Retrying non-idempotent writes needs idempotency
  keys whichever layer does it.
- **Budget tuning.** 10% is a common default. Too low and real transient
  errors are not retried; too high and overload is amplified again. A
  minimum allowance (a few retries per second) helps low-traffic clients,
  which this model does not include.
- **Budgets are per client.** Thousands of clients each adding 10% still add
  10% in total, which is fine; but clients that ignore the budget (old
  versions, scripts) are not limited by it. Server-side load shedding
  (problem 016 in this repo) is still needed.
- **Related but different:** devops-cloud-solutions 001 fixes synchronized
  retries with backoff and jitter. Jitter spreads retries out in time; it does
  not stop 64× multiplication. Use both, plus circuit breakers.
- The model is simple: fixed capacity, FIFO queue, no backoff, and every
  abandoned call still runs. Real databases cancel some abandoned queries,
  which helps, but connection pools and lock queues often behave like this.

## Run
```bash
python -m unittest -v test_solution.py
```
