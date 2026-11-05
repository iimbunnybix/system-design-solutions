"""Retry amplification across layers, and a retry budget that stops it.

A request passes through three layers: API gateway -> order service ->
data-access client -> database. Each layer was written by a different team
and each "safely" retried a failed call up to 3 times. The retries multiply:
when the database fails, one user request becomes up to 4 * 4 * 4 = 64
database calls. A three-second database slowdown therefore turned into 64x
the load, and the queue of abandoned work never drained: the outage became
permanent until traffic was cut.

The fix retries at exactly one layer (data access), only while a per-client
retry budget allows it (retries at most 10% of recent first attempts), and marks a
give-up as "overloaded, do not retry" so the layers above never retry it.
"""
from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

NAIVE_RETRIES_PER_LAYER = 3      # what every team picked independently
SINGLE_LAYER_RETRIES = 3         # fixed design: only the data-access layer retries
RETRY_BUDGET_RATIO = 0.10        # retries may be at most 10% of first attempts
BUDGET_WINDOW_CALLS = 1000       # how many recent DB calls the budget looks at

REQUESTS_PER_TICK = 100          # user requests per second
NORMAL_CAPACITY = 150            # DB calls the database finishes per second
BLIP_CAPACITY = 50               # capacity during the short slowdown
BLIP_TICKS = range(10, 13)       # seconds 10, 11 and 12
SIMULATED_TICKS = 40


@dataclass
class Outcome:
    """Result of one call at any layer."""
    ok: bool
    do_not_retry: bool = False   # set when the callee says "overloaded, back off"


class Database:
    """A database that works through a FIFO queue at a fixed rate per tick.

    A call that cannot start in the tick it was sent waits in the queue, and
    its client times out. The database still executes it later, so abandoned
    calls keep using capacity: that is what makes overload self-sustaining.
    """

    def __init__(self) -> None:
        self.backlog = 0          # abandoned calls still waiting to execute
        self.free_this_tick = 0
        self.calls_received = 0

    def start_tick(self, capacity: int) -> None:
        """Spend this tick's capacity on old abandoned work first (FIFO)."""
        drained = min(self.backlog, capacity)
        self.backlog -= drained
        self.free_this_tick = capacity - drained

    def execute(self) -> Outcome:
        """One DB call: succeeds if capacity is left this tick, else times out."""
        self.calls_received += 1
        if self.free_this_tick > 0:
            self.free_this_tick -= 1
            return Outcome(ok=True)
        # Behind the queue: the client times out, but the work stays queued.
        self.backlog += 1
        return Outcome(ok=False)


class RetryBudget:
    """Per-client budget: a retry is allowed only if recent retries stay at
    or below RETRY_BUDGET_RATIO of recent first attempts (Finagle style)."""

    def __init__(self, ratio: float = RETRY_BUDGET_RATIO,
                 window: int = BUDGET_WINDOW_CALLS) -> None:
        self.ratio = ratio
        self.recent_calls: deque[bool] = deque(maxlen=window)   # True = retry

    def record_first_attempt(self) -> None:
        self.recent_calls.append(False)

    def try_spend_retry(self) -> bool:
        retries = sum(self.recent_calls)
        first_attempts = len(self.recent_calls) - retries
        if retries + 1 > self.ratio * first_attempts:
            return False
        self.recent_calls.append(True)
        return True


def call_with_retries(call_once: Callable[[], Outcome], max_retries: int,
                      budget: RetryBudget | None = None) -> Outcome:
    """Run ``call_once`` and retry failures up to ``max_retries`` times.

    Never retries an error marked do_not_retry. With a budget, each retry
    must be paid for; when the budget says no, the failure is returned as
    do_not_retry so callers further up do not retry it either.
    """
    if budget is not None:
        budget.record_first_attempt()
    outcome = call_once()
    retries_done = 0
    while not outcome.ok and not outcome.do_not_retry and retries_done < max_retries:
        if budget is not None and not budget.try_spend_retry():
            return Outcome(ok=False, do_not_retry=True)
        retries_done += 1
        outcome = call_once()
    if not outcome.ok and budget is not None:
        # The one retrying layer gave up: tell everyone above to stop too.
        return Outcome(ok=False, do_not_retry=True)
    return outcome


@dataclass
class RequestStack:
    """Gateway -> service -> data access -> database, with per-layer retries."""
    database: Database
    gateway_retries: int
    service_retries: int
    data_access_retries: int
    budget: RetryBudget | None = None

    def data_access_call(self) -> Outcome:
        return call_with_retries(self.database.execute, self.data_access_retries, self.budget)

    def service_call(self) -> Outcome:
        return call_with_retries(self.data_access_call, self.service_retries)

    def handle_user_request(self) -> Outcome:
        return call_with_retries(self.service_call, self.gateway_retries)


def naive_stack(database: Database) -> RequestStack:
    """The original setup: every layer retries 3 times, no budget."""
    return RequestStack(database, NAIVE_RETRIES_PER_LAYER, NAIVE_RETRIES_PER_LAYER,
                        NAIVE_RETRIES_PER_LAYER)


def single_layer_stack(database: Database) -> RequestStack:
    """Retries only in data access, but without a budget (still not enough)."""
    return RequestStack(database, 0, 0, SINGLE_LAYER_RETRIES)


def budgeted_stack(database: Database) -> RequestStack:
    """The fix: one retrying layer, a 10% retry budget, do-not-retry signalling."""
    return RequestStack(database, 0, 0, SINGLE_LAYER_RETRIES, RetryBudget())


@dataclass
class TickStats:
    db_calls: int
    succeeded: int
    backlog: int


@dataclass
class SimulationResult:
    ticks: list[TickStats] = field(default_factory=list)

    def total_db_calls(self) -> int:
        return sum(tick.db_calls for tick in self.ticks)

    def last_tick(self) -> TickStats:
        return self.ticks[-1]


def capacity_at(tick: int) -> int:
    """Database capacity for a tick: reduced during the blip."""
    return BLIP_CAPACITY if tick in BLIP_TICKS else NORMAL_CAPACITY


def simulate(make_stack: Callable[[Database], RequestStack], ticks: int = SIMULATED_TICKS) -> SimulationResult:
    """Send REQUESTS_PER_TICK user requests per tick through a fresh stack."""
    database = Database()
    stack = make_stack(database)
    result = SimulationResult()
    for tick in range(ticks):
        database.start_tick(capacity_at(tick))
        calls_before = database.calls_received
        succeeded = 0
        for _ in range(REQUESTS_PER_TICK):
            if stack.handle_user_request().ok:
                succeeded += 1
        result.ticks.append(TickStats(database.calls_received - calls_before,
                                      succeeded, database.backlog))
    return result
