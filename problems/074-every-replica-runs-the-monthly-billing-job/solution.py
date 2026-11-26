"""Scaling the billing service to three replicas billed every customer three times.

The monthly billing job was an in-process scheduler inside the service. With
one instance that was fine; when the service was scaled out, every replica's
scheduler fired at midnight and each one charged every customer. The fix
makes each scheduled run claim a unique run key in a shared store before it
starts (only one replica wins), lets another replica take over if the winner
dies mid-run, and charges with an idempotency key per run and customer, so
a takeover finishes the run without charging anyone twice.
"""
from __future__ import annotations

from dataclasses import dataclass, field

REPLICAS = ["billing-1", "billing-2", "billing-3"]
CUSTOMERS = 200
CLAIM_TTL_SECONDS = 300               # a run that has not finished by then is presumed dead
RUN_KEY = "monthly-billing@2026-12-01"


def customer_ids() -> list[str]:
    return [f"cust-{number:04d}" for number in range(1, CUSTOMERS + 1)]


class PaymentProvider:
    """Records charges; dedupes on an idempotency key when one is given."""

    def __init__(self) -> None:
        self.charges: list[str] = []                   # customer id per charge
        self.seen_keys: set[str] = set()

    def charge(self, customer: str, idempotency_key: str | None = None) -> None:
        if idempotency_key is not None:
            if idempotency_key in self.seen_keys:
                return                                 # same request again: no new charge
            self.seen_keys.add(idempotency_key)
        self.charges.append(customer)


# ---------------------------------------------------------------- original
def naive_run_on_every_replica(provider: PaymentProvider) -> None:
    """Original: each replica's scheduler fires and bills everyone."""
    for _replica in REPLICAS:
        for customer in customer_ids():
            provider.charge(customer)


# ---------------------------------------------------------------- the fix
@dataclass
class Claim:
    owner: str
    expires_at: int
    done: bool = False


@dataclass
class RunRegistry:
    """Shared store of scheduled runs (a table with a unique key in practice)."""

    claims: dict[str, Claim] = field(default_factory=dict)

    def try_claim(self, run_key: str, owner: str, now: int) -> bool:
        """Atomically claim a run unless someone else holds it or it is done.

        In a database this is one conditional INSERT ... ON CONFLICT or an
        UPDATE guarded by ``done = false AND expires_at <= now``.
        """
        claim = self.claims.get(run_key)
        if claim is not None and (claim.done or claim.expires_at > now):
            return False
        self.claims[run_key] = Claim(owner, now + CLAIM_TTL_SECONDS)
        return True

    def mark_done(self, run_key: str, owner: str) -> None:
        claim = self.claims[run_key]
        if claim.owner != owner:
            raise RuntimeError("only the current owner can finish a run")
        claim.done = True


def run_billing(replica: str, registry: RunRegistry, provider: PaymentProvider,
                now: int, crash_after: int | None = None) -> bool:
    """One replica's scheduler fires. Returns True if it finished the run.

    ``crash_after`` simulates the replica dying after that many charges.
    """
    if not registry.try_claim(RUN_KEY, replica, now):
        return False
    for charged, customer in enumerate(customer_ids()):
        if crash_after is not None and charged == crash_after:
            return False                               # died; claim expires later
        # Same key on a takeover, so customers charged before the crash are skipped.
        provider.charge(customer, idempotency_key=f"{RUN_KEY}:{customer}")
    registry.mark_done(RUN_KEY, replica)
    return True


def run_all_replicas(registry: RunRegistry, provider: PaymentProvider, now: int) -> list[str]:
    """Every replica's scheduler fires at the same moment; return who ran it."""
    finished = []
    for replica in REPLICAS:
        if run_billing(replica, registry, provider, now):
            finished.append(replica)
    return finished
