# 013 · Products showed "out of stock" while the warehouse shelves were full, and some customers were charged for orders that never shipped

## Problem
A retailer split its monolith into services that each own their data:
inventory, payments and shipping. Placing an order became three calls from
the order service: reserve stock, charge the card, create the shipment. When
any call failed, the order service returned an error to the customer.

Within weeks, popular products showed "out of stock" online while the
warehouse had units on the shelves; an inventory audit found thousands of
units reserved by orders that did not exist. Support also received complaints
from customers who were charged although their order "failed" (the shipping
service rejected their address after the payment went through). Finance
refunded them by hand.

## Root cause
**A multi-service operation without a transaction or compensation.** Each
service committed its own step. When a later step failed, nothing undid the
earlier ones:

- a declined card left the stock reservation in place forever;
- an unserviceable address left both the reservation and the charge;
- a crash of the order service between calls left whatever had already
  happened, with no record of it.

In the simulation (1,000 orders, 8% card declines, 2% unserviceable
addresses), the original code placed 884 orders and leaked **116 stock
reservations** and **19 charges without a shipment**.

## Solution
An orchestrated **saga** (`solution.py`):

1. **Steps with compensations.** The order is a list of `Step(action,
   compensate)`: reserve/release stock, charge/refund card,
   create/cancel shipment.
2. **Durable saga log.** The orchestrator records each saga's status,
   completed steps and compensated steps in its own database
   (`SagaLog`). Progress is written per step, so a crash loses nothing.
3. **Compensate in reverse on business failure.** If a step fails
   permanently, completed steps are undone newest first (refund, then
   release stock). A step that timed out after all retries is also
   compensated, because it may have succeeded remotely (the test covers a
   payment that charged and then timed out).
4. **Idempotent participants.** Every action and compensation is keyed by
   the saga id, so retries and recovery can safely repeat them.
5. **Retries for transient errors**, bounded per call.
6. **Recovery.** On start-up, `recover()` resumes every saga still
   `running` or `compensating` from its log. Each saga is driven on its own:
   if one saga's compensation still fails after its retries, that saga stays
   `compensating` with the error recorded for the next pass, and the other
   sagas are still recovered.

Results from the tests: on the same 1,000 orders, every order ends either
fully placed (stock reserved, charged, shipped) or fully undone, and stock
equals the starting stock minus placed orders. Crashing the orchestrator at
each of 9 points (before/after each step, during compensation), with and
without a failing shipment, and then running `recover()` always ends in a
consistent final state.

## Trade-offs
- A saga is **not isolated**: other requests can see intermediate states
  (stock reserved for an order that will be compensated a second later).
  Design for that with semantic states such as "reserved" and "pending
  payment" rather than final ones.
- Some actions cannot be undone (an email sent, goods handed to a courier).
  Put them last, after the "pivot" step, or make their compensation a new
  action (a cancellation email).
- **A compensation can overtake its action.** A request that timed out may
  still be in flight and reach the participant after the refund or release.
  So every compensation leaves a tombstone for the saga id, and the action
  refuses to run for a cancelled saga (tests: late charge, reserve and
  shipment after compensation are rejected). Tombstones must be kept at
  least as long as a request can be delayed.
- Compensations must eventually succeed. Retry with back-off forever and
  alert a human after a while; never drop them.
- **Orchestration vs choreography**: here one orchestrator drives the flow,
  which keeps it visible and testable. Choreography (each service reacts to
  events) avoids a central component but makes the flow harder to follow and
  debug. Workflow engines (Temporal, AWS Step Functions) provide the durable
  log, retries and recovery as a product.
- Publishing step commands reliably needs the outbox pattern (see
  backend-api-solutions 006) when the orchestrator talks to services over a
  queue.
- Two-phase commit avoids intermediate states but needs all participants to
  support it and blocks while the coordinator is down; it is rarely available
  across independent services.

## Run
```bash
python -m unittest -v test_solution.py
```
