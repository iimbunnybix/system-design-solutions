"""Client-side load balancing that does not herd onto a freshly added node.

Every API client picked the backend with the lowest load in a shared load
report that was refreshed only every few seconds. When a new node joined it
reported zero load, so every client sent every request to it until the next
refresh; the node overloaded while the old ones went idle, and the herd then
moved to whichever node looked emptiest in the next report.

The fix is "power of two random choices": each request samples two backends
at random and goes to the one with fewer requests in flight *from this
client*. Randomness spreads clients apart, and local counts are never stale.
"""
from __future__ import annotations

import random

CLIENT_COUNT = 100
EXISTING_BACKENDS = 10
REQUEST_DURATION_TICKS = 5
REPORT_REFRESH_TICKS = 10
SIMULATION_TICKS = 60
# A backend that holds more than this many requests at once starts timing out.
BACKEND_SAFE_IN_FLIGHT = 80
NEW_NODE = "backend-new"


class Cluster:
    """Backends and the in-flight requests on each, by finishing tick."""

    def __init__(self, names: list[str]) -> None:
        self.finishing_ticks: dict[str, list[int]] = {}
        for name in names:
            self.finishing_ticks[name] = []

    def in_flight(self, name: str) -> int:
        """Requests currently running on a backend."""
        return len(self.finishing_ticks[name])

    def send(self, name: str, now: int) -> None:
        """Start a request that ends REQUEST_DURATION_TICKS later."""
        self.finishing_ticks[name].append(now + REQUEST_DURATION_TICKS)

    def finish_requests(self, now: int) -> None:
        """Drop requests whose finishing tick has arrived."""
        for name, ticks in self.finishing_ticks.items():
            self.finishing_ticks[name] = [tick for tick in ticks if tick > now]

    def load_report(self) -> dict[str, int]:
        """Snapshot of every backend's load, as a metrics service would publish."""
        report = {}
        for name in self.finishing_ticks:
            report[name] = self.in_flight(name)
        return report


def build_cluster_with_new_node() -> Cluster:
    """Ten warm backends at their fair share of load, plus one empty new node.

    Warm backends' requests finish spread over the next few ticks, as they
    would in steady state.
    """
    names = [f"backend-{index}" for index in range(EXISTING_BACKENDS)]
    cluster = Cluster(names + [NEW_NODE])
    requests_per_tick = CLIENT_COUNT
    for index, name in enumerate(names):
        for finishing_tick in range(1, REQUEST_DURATION_TICKS + 1):
            # Each warm backend got 1/10 of each earlier tick's requests.
            for _ in range(requests_per_tick // EXISTING_BACKENDS):
                cluster.finishing_ticks[name].append(finishing_tick)
    return cluster


def naive_pick(report: dict[str, int]) -> str:
    """Original rule: the least-loaded backend in the shared (stale) report."""
    best_name = ""
    best_load = -1
    for name, load in report.items():
        if best_load < 0 or load < best_load:
            best_name, best_load = name, load
    return best_name


class TwoChoicesClient:
    """One client that balances using only its own in-flight counts."""

    def __init__(self, backends: list[str], rng: random.Random) -> None:
        if len(backends) < 2:
            raise ValueError("two random choices need at least two backends")
        self.backends = backends
        self.rng = rng
        self.my_finishing_ticks: dict[str, list[int]] = {name: [] for name in backends}

    def pick(self, now: int) -> str:
        """Sample two different backends, choose the one this client loads less."""
        self.forget_finished(now)
        first, second = self.rng.sample(self.backends, 2)
        if len(self.my_finishing_ticks[second]) < len(self.my_finishing_ticks[first]):
            first = second
        self.my_finishing_ticks[first].append(now + REQUEST_DURATION_TICKS)
        return first

    def forget_finished(self, now: int) -> None:
        """Drop this client's requests that have completed."""
        for name, ticks in self.my_finishing_ticks.items():
            self.my_finishing_ticks[name] = [tick for tick in ticks if tick > now]


class LoadStats:
    """What the on-call engineer would see on the per-backend dashboard."""

    def __init__(self) -> None:
        self.peak_in_flight = 0
        self.peak_on_new_node = 0
        self.overloaded_backend_ticks = 0

    def record(self, cluster: Cluster) -> None:
        """Sample every backend's load once per tick."""
        for name in cluster.finishing_ticks:
            load = cluster.in_flight(name)
            self.peak_in_flight = max(self.peak_in_flight, load)
            if name == NEW_NODE:
                self.peak_on_new_node = max(self.peak_on_new_node, load)
            if load > BACKEND_SAFE_IN_FLIGHT:
                self.overloaded_backend_ticks += 1


def simulate_naive(ticks: int = SIMULATION_TICKS) -> LoadStats:
    """Every client sends one request per tick to the report's least-loaded node."""
    cluster = build_cluster_with_new_node()
    stats = LoadStats()
    report = cluster.load_report()
    for now in range(ticks):
        cluster.finish_requests(now)
        # The shared report is only refreshed every few ticks.
        if now % REPORT_REFRESH_TICKS == 0:
            report = cluster.load_report()
        for _ in range(CLIENT_COUNT):
            cluster.send(naive_pick(report), now)
        stats.record(cluster)
    return stats


def simulate_two_choices(seed: int, ticks: int = SIMULATION_TICKS) -> LoadStats:
    """Same traffic, but each client uses two random choices on local counts."""
    cluster = build_cluster_with_new_node()
    stats = LoadStats()
    backends = list(cluster.finishing_ticks)
    rng = random.Random(seed)
    clients = []
    for _ in range(CLIENT_COUNT):
        # Each client gets its own seeded generator, as separate processes would.
        clients.append(TwoChoicesClient(backends, random.Random(rng.random())))
    for now in range(ticks):
        cluster.finish_requests(now)
        for client in clients:
            cluster.send(client.pick(now), now)
        stats.record(cluster)
    return stats


def fair_share() -> int:
    """In-flight requests per backend if load were spread perfectly evenly."""
    total_in_flight = CLIENT_COUNT * REQUEST_DURATION_TICKS
    return total_in_flight // (EXISTING_BACKENDS + 1)
