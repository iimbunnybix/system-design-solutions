"""Priority-aware load shedding at an API gateway.

When the gateway ran out of capacity it rejected whichever requests happened
to arrive after the slots filled up. Arrival order is effectively random, so
checkout requests were shed exactly as often as background analytics pings.

The fix is admission control by priority: every priority may only use the
gateway up to its own utilisation ceiling. Background traffic is cut off
first, normal traffic next, and the top slice of capacity is reserved for
critical traffic such as checkout and payments.
"""
from __future__ import annotations

import random
from collections import Counter
from enum import IntEnum


class Priority(IntEnum):
    """Lower number = more important."""

    CRITICAL = 0     # checkout, payment confirmation
    DEFAULT = 1      # browsing, search
    BACKGROUND = 2   # analytics pings, prefetch, telemetry


# Share of total capacity each priority may fill, in whole percent. Above 60%
# utilisation no new background request is admitted; above 70% no default
# request is admitted. The last 30% is therefore reserved for critical traffic.
# Integers (not 0.7) keep the slot maths exact: 90 * 0.7 is 62.999... in floats.
UTILISATION_CEILING_PERCENT = {
    Priority.CRITICAL: 100,
    Priority.DEFAULT: 70,
    Priority.BACKGROUND: 60,
}
PERCENT = 100
# Every class with a non-zero ceiling may use at least one slot, so a tiny
# gateway (capacity 1-3) does not reject browsing on an idle box.
MIN_SLOTS_PER_CLASS = 1


class NaiveGateway:
    """Original behaviour: admit anything while a slot is free."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.in_flight = 0

    def try_admit(self, priority: Priority) -> bool:
        """Admit regardless of priority; reject only when completely full."""
        if self.in_flight >= self.capacity:
            return False
        self.in_flight += 1
        return True

    def release(self) -> None:
        """A request finished and gave its slot back."""
        self.in_flight -= 1


class PriorityGateway:
    """Admit a request only if utilisation is below its priority's ceiling."""

    def __init__(self, capacity: int, ceilings_percent: dict[Priority, int] | None = None):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.ceilings_percent = dict(ceilings_percent or UTILISATION_CEILING_PERCENT)
        self.in_flight = 0

    def slot_limit(self, priority: Priority) -> int:
        """How many in-flight requests may exist when this priority is admitted."""
        percent = self.ceilings_percent[priority]
        if percent <= 0:
            return 0
        # Integer floor division rounds down exactly, so a class never exceeds
        # its share on larger gateways...
        share = self.capacity * percent // PERCENT
        # ...but on a tiny gateway the floor can be 0, which would shed the
        # class even while the gateway is idle.
        return max(MIN_SLOTS_PER_CLASS, share)

    def try_admit(self, priority: Priority) -> bool:
        """Admit if there is room below this priority's ceiling."""
        if self.in_flight >= self.slot_limit(priority):
            return False
        self.in_flight += 1
        return True

    def release(self) -> None:
        """A request finished and gave its slot back."""
        if self.in_flight == 0:
            raise RuntimeError("release() called with no request in flight")
        self.in_flight -= 1


def build_arrivals(mix: dict[Priority, int], rng: random.Random) -> list[Priority]:
    """One tick of traffic, shuffled because real arrival order is random."""
    arrivals: list[Priority] = []
    for priority, count in mix.items():
        arrivals.extend([priority] * count)
    rng.shuffle(arrivals)
    return arrivals


def simulate(gateway: NaiveGateway | PriorityGateway, mix: dict[Priority, int],
             ticks: int, seed: int) -> dict[Priority, float]:
    """Run ``ticks`` rounds of traffic and return the success rate per priority.

    Each admitted request holds its slot for exactly one tick, so a gateway
    with capacity N serves at most N requests per tick.
    """
    rng = random.Random(seed)
    offered: Counter = Counter()
    admitted: Counter = Counter()
    for _ in range(ticks):
        admitted_this_tick = 0
        for priority in build_arrivals(mix, rng):
            offered[priority] += 1
            if gateway.try_admit(priority):
                admitted[priority] += 1
                admitted_this_tick += 1
        # Requests finish at the end of the tick and free their slots.
        for _ in range(admitted_this_tick):
            gateway.release()
    return success_rates(offered, admitted)


def success_rates(offered: Counter, admitted: Counter) -> dict[Priority, float]:
    """Fraction of offered requests that were admitted, per priority."""
    rates: dict[Priority, float] = {}
    for priority, count in offered.items():
        rates[priority] = admitted[priority] / count
    return rates
