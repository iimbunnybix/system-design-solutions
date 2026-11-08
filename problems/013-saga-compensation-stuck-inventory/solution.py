"""Orchestrated saga: multi-service order placement without leaked state.

Placing an order touches three services that each own their database:
reserve stock (inventory), charge the card (payments), create the shipment
(shipping). There is no distributed transaction across them. The original
code called them in sequence and returned an error if a later step failed,
leaving earlier steps applied: stock reserved for orders that never existed,
and occasionally a charge with no shipment.

``SagaOrchestrator`` runs the steps from a definition, writes every state
transition to a durable ``SagaLog`` *before* acting, and on a business
failure runs the compensations of completed steps in reverse order. Because
the log is durable and every action and compensation is idempotent (keyed by
saga id), a crash at any point is repaired by ``recover()``, which resumes each
unfinished saga from its log.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


class BusinessError(Exception):
    """Permanent failure (card declined, out of stock): compensate."""


class TransientError(Exception):
    """Timeout / 503: retry the same idempotent call."""


class Crash(BaseException):
    """Simulated orchestrator process death (not caught by ``except Exception``)."""


# --- Participating services (each idempotent per saga id) ---------------------

class Inventory:
    def __init__(self, stock: dict[str, int]):
        self.stock = dict(stock)
        self.reservations: dict[str, tuple[str, int]] = {}
        self.cancelled: set[str] = set()

    def reserve(self, saga_id: str, sku: str, qty: int) -> None:
        if saga_id in self.cancelled:
            # A delayed request that arrives after its compensation.
            raise BusinessError("saga already cancelled")
        if saga_id in self.reservations:
            return                                   # retry of a done call
        if self.stock.get(sku, 0) < qty:
            raise BusinessError("out of stock")
        self.stock[sku] -= qty
        self.reservations[saga_id] = (sku, qty)

    def release(self, saga_id: str) -> None:
        # Tombstone first: the reserve may still be in flight and land later.
        self.cancelled.add(saga_id)
        held = self.reservations.pop(saga_id, None)
        if held:
            self.stock[held[0]] += held[1]


class Payments:
    def __init__(self, declines: set[str] = frozenset()):
        self.declines = declines
        self.charges: dict[str, int] = {}
        self.refunds: set[str] = set()
        self.cancelled: set[str] = set()

    def charge(self, saga_id: str, amount: int) -> None:
        if saga_id in self.cancelled:
            raise BusinessError("saga already cancelled")
        if saga_id in self.charges:
            return
        if saga_id in self.declines:
            raise BusinessError("card declined")
        self.charges[saga_id] = amount

    def refund(self, saga_id: str) -> None:
        self.cancelled.add(saga_id)            # remember even if nothing to undo yet
        if saga_id in self.charges:
            self.refunds.add(saga_id)

    def net_charged(self, saga_id: str) -> int:
        return 0 if saga_id in self.refunds else self.charges.get(saga_id, 0)


class Shipping:
    def __init__(self, unserviceable: set[str] = frozenset()):
        self.unserviceable = unserviceable
        self.shipments: set[str] = set()
        self.cancelled: set[str] = set()

    def create(self, saga_id: str) -> None:
        if saga_id in self.cancelled:
            raise BusinessError("saga already cancelled")
        if saga_id in self.unserviceable:
            raise BusinessError("address not serviceable")
        self.shipments.add(saga_id)

    def cancel(self, saga_id: str) -> None:
        self.cancelled.add(saga_id)
        self.shipments.discard(saga_id)


# --- Saga machinery -----------------------------------------------------------

@dataclass(frozen=True)
class Step:
    name: str
    action: Callable[[str], None]
    compensate: Callable[[str], None] | None


@dataclass
class SagaRecord:
    saga_id: str
    status: str = "running"     # running | compensating | completed | compensated
    completed: list[str] = field(default_factory=list)
    compensated: list[str] = field(default_factory=list)
    error: str | None = None


class SagaLog:
    """Durable state per saga. In production: a table written in the
    orchestrator's own database (one row per saga, updated transactionally)."""

    def __init__(self) -> None:
        self.records: dict[str, SagaRecord] = {}

    def unfinished(self) -> list[SagaRecord]:
        return [r for r in self.records.values() if r.status in ("running", "compensating")]


class SagaOrchestrator:
    def __init__(self, log: SagaLog, max_attempts: int = 5,
                 crash_hook: Callable[[str, str, str], None] | None = None):
        self.log = log
        self.max_attempts = max_attempts
        self.crash_hook = crash_hook or (lambda *a: None)   # (saga, phase, step)

    def _call(self, fn: Callable[[str], None], saga_id: str) -> None:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return fn(saga_id)
            except TransientError:
                if attempt == self.max_attempts:
                    raise

    def run(self, saga_id: str, steps: list[Step]) -> SagaRecord:
        rec = self.log.records.setdefault(saga_id, SagaRecord(saga_id))
        return self._drive(rec, steps)

    def recover(self, steps_for: Callable[[str], list[Step]]) -> list[SagaRecord]:
        return [self._drive(r, steps_for(r.saga_id)) for r in self.log.unfinished()]

    def _drive(self, rec: SagaRecord, steps: list[Step]) -> SagaRecord:
        if rec.status == "running":
            for step in steps:
                if step.name in rec.completed:
                    continue
                self.crash_hook(rec.saga_id, "before", step.name)
                try:
                    self._call(step.action, rec.saga_id)
                except (BusinessError, TransientError) as e:
                    # A transient error that outlived the retries is treated
                    # as failure; the step's own compensation is idempotent so
                    # it is safe to run even if the call actually succeeded.
                    rec.error = f"{step.name}: {e}"
                    rec.status = "compensating"
                    if isinstance(e, TransientError):
                        rec.completed.append(step.name)
                    break
                self.crash_hook(rec.saga_id, "after", step.name)
                rec.completed.append(step.name)
            else:
                rec.status = "completed"
                return rec
        by_name = {s.name: s for s in steps}
        for name in reversed(rec.completed):
            if name in rec.compensated:
                continue
            step = by_name[name]
            self.crash_hook(rec.saga_id, "compensate", name)
            if step.compensate:
                # Compensations must eventually succeed; retry forever in
                # production (with back-off and alerting), bounded here.
                self._call(step.compensate, rec.saga_id)
            rec.compensated.append(name)
        rec.status = "compensated"
        return rec


def order_steps(inv: Inventory, pay: Payments, ship: Shipping, sku: str, qty: int,
                amount: int) -> list[Step]:
    return [
        Step("reserve_stock", lambda s: inv.reserve(s, sku, qty), inv.release),
        Step("charge_card", lambda s: pay.charge(s, amount), pay.refund),
        Step("create_shipment", ship.create, ship.cancel),
    ]


def naive_place_order(inv: Inventory, pay: Payments, ship: Shipping, saga_id: str,
                      sku: str, qty: int, amount: int) -> bool:
    """The original code: sequential calls, no compensation."""
    try:
        inv.reserve(saga_id, sku, qty)
        pay.charge(saga_id, amount)
        ship.create(saga_id)
        return True
    except BusinessError:
        return False
