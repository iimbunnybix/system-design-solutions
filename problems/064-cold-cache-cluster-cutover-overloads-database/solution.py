"""Move reads to a new cache cluster without sending its misses to the database all at once.

The team replaced the cache cluster and flipped 100% of reads to the new,
empty cluster in one step. Its hit rate went from about 95% to 0, so nearly
every read went to the database, which received about 20 times its normal
load and fell over.

The fix warms the new cluster before it serves traffic. In a shadow phase
reads are still answered by the old cluster, but every key read is also
written into the new one. Then traffic moves over in steps; a step is taken
only when the new cluster's hit rate is close to the old one's, and the ramp
steps back automatically if database load goes over its budget.
"""
from __future__ import annotations

import random
from collections import OrderedDict
from dataclasses import dataclass, field
from itertools import accumulate

KEY_COUNT = 50_000
ZIPF_EXPONENT = 1.25           # a few products get most of the reads
CACHE_CAPACITY = 10_000        # keys per cluster
READS_PER_SECOND = 1_000
WARMUP_SECONDS = 120           # old cluster runs this long before we measure
RUN_SECONDS = 300
CUTOVER_SECOND = 30            # naive: flip 100% at this second
DB_BUDGET_READS_PER_SECOND = 150   # about 3x normal miss traffic
DB_CAPACITY_READS_PER_SECOND = 300  # beyond this, reads time out
RAMP_PERCENTS = [0, 5, 25, 50, 100]  # 0 = shadow only
DECISION_SECONDS = 10          # how often the controller looks at the metrics
HIT_RATE_TOLERANCE = 0.02      # new may trail old by at most 2 points
ROUTING_BUCKETS = 100
HASH_MULTIPLIER = 2_654_435_761   # Knuth multiplicative hash for key buckets


class LruCache:
    """A cache cluster modelled as one LRU of keys."""

    def __init__(self, capacity: int = CACHE_CAPACITY) -> None:
        self.capacity = capacity
        self.keys: OrderedDict[int, None] = OrderedDict()

    def contains(self, key: int) -> bool:
        """Look up a key and mark it recently used."""
        if key not in self.keys:
            return False
        self.keys.move_to_end(key)
        return True

    def put(self, key: int) -> None:
        """Store a key, evicting the least recently used one if full."""
        self.keys[key] = None
        self.keys.move_to_end(key)
        if len(self.keys) > self.capacity:
            self.keys.popitem(last=False)


class Traffic:
    """Seeded Zipf-like read traffic, one batch of keys per second."""

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.keys = list(range(KEY_COUNT))
        weights = [1 / (rank + 1) ** ZIPF_EXPONENT for rank in self.keys]
        self.cumulative = list(accumulate(weights))

    def next_second(self) -> list[int]:
        """The keys read during one second."""
        return self.rng.choices(self.keys, cum_weights=self.cumulative, k=READS_PER_SECOND)


@dataclass
class SecondStats:
    """Counters for one second of traffic."""
    db_reads: int = 0              # reads that reached the database
    failed_reads: int = 0          # reads beyond DB capacity: user saw an error
    old_lookups: int = 0
    old_hits: int = 0
    new_lookups: int = 0
    new_hits: int = 0


@dataclass
class RunResult:
    """Database load per second and where the ramp was each second."""
    db_reads_per_second: list[int] = field(default_factory=list)
    percent_on_new: list[int] = field(default_factory=list)
    failed_reads: int = 0

    def peak_db_load(self) -> int:
        """Highest database reads in any one second."""
        return max(self.db_reads_per_second)

    def seconds_over_budget(self) -> int:
        """Seconds in which the database got more reads than its budget."""
        return sum(1 for load in self.db_reads_per_second if load > DB_BUDGET_READS_PER_SECOND)


def routes_to_new(key: int, percent: int) -> bool:
    """Stable per-key routing, so a key's traffic keeps warming the same cluster."""
    bucket = (key * HASH_MULTIPLIER) % 2**32 % ROUTING_BUCKETS
    return bucket < percent


def serve_second(keys: list[int], old: LruCache, new: LruCache, percent: int,
                 shadow_fill: bool) -> SecondStats:
    """Serve one second of reads, then answer all of that second's misses from the DB."""
    stats = SecondStats()
    misses: list[tuple[int, LruCache]] = []
    for key in keys:
        if routes_to_new(key, percent):
            if not lookup_new(key, new, stats):
                misses.append((key, new))
            continue
        stats.old_lookups += 1
        old_hit = old.contains(key)
        if old_hit:
            stats.old_hits += 1
        else:
            misses.append((key, old))
        # WHY: the shadow read measures the new cluster's would-be hit rate,
        # then copies the value we already have so it warms at no DB cost.
        if shadow_fill:
            lookup_new(key, new, stats)
            if old_hit:
                new.put(key)
    answer_from_database(misses, stats, new if shadow_fill else None)
    return stats


def lookup_new(key: int, new: LruCache, stats: SecondStats) -> bool:
    """Look a key up in the new cluster and count the result."""
    stats.new_lookups += 1
    hit = new.contains(key)
    if hit:
        stats.new_hits += 1
    return hit


def answer_from_database(misses: list[tuple[int, LruCache]], stats: SecondStats,
                         shadow: LruCache | None) -> None:
    """Send this second's misses to the DB; reads past its capacity fail and fill nothing."""
    # WHY: fills happen after the DB answers, so concurrent misses for one hot
    # key in the same second all reach the database.
    stats.db_reads = len(misses)
    answered = misses[:DB_CAPACITY_READS_PER_SECOND]
    stats.failed_reads = len(misses) - len(answered)
    for key, cache in answered:
        cache.put(key)
        if shadow is not None:
            shadow.put(key)


def warm_old_cluster(traffic: Traffic) -> LruCache:
    """The old cluster as it was before the migration: running for a while."""
    old = LruCache()
    unused = LruCache(capacity=0)
    for _ in range(WARMUP_SECONDS):
        serve_second(traffic.next_second(), old, unused, percent=0, shadow_fill=False)
    return old


def naive_cutover(seed: int) -> RunResult:
    """The incident: flip 100% of reads to the empty new cluster at one instant."""
    traffic = Traffic(seed)
    old, new = warm_old_cluster(traffic), LruCache()
    result = RunResult()
    for second in range(RUN_SECONDS):
        percent = 100 if second >= CUTOVER_SECOND else 0
        stats = serve_second(traffic.next_second(), old, new, percent, shadow_fill=False)
        result.db_reads_per_second.append(stats.db_reads)
        result.failed_reads += stats.failed_reads
        result.percent_on_new.append(percent)
    return result


@dataclass
class RampController:
    """Moves traffic to the new cluster one step at a time, guarded by metrics."""
    step: int = 0
    window: list[SecondStats] = field(default_factory=list)

    def percent(self) -> int:
        """Share of keys currently served by the new cluster."""
        return RAMP_PERCENTS[self.step]

    def observe(self, stats: SecondStats) -> None:
        """Record one second; step back at once on overload, else decide each window."""
        if stats.db_reads > DB_BUDGET_READS_PER_SECOND:
            self.step = max(0, self.step - 1)
            self.window = []
            return
        self.window.append(stats)
        if len(self.window) >= DECISION_SECONDS:
            if self._new_is_warm() and self.step < len(RAMP_PERCENTS) - 1:
                self.step += 1
            self.window = []

    def _new_is_warm(self) -> bool:
        """True when the new cluster's hit rate is within tolerance of the old one's."""
        if self.percent() == 100:
            return True
        old_hit_rate = hit_rate(sum(s.old_hits for s in self.window),
                                sum(s.old_lookups for s in self.window))
        new_hit_rate = hit_rate(sum(s.new_hits for s in self.window),
                                sum(s.new_lookups for s in self.window))
        return new_hit_rate >= old_hit_rate - HIT_RATE_TOLERANCE


def hit_rate(hits: int, lookups: int) -> float:
    """Hits divided by lookups; an unused cluster counts as fully warm."""
    if lookups == 0:
        return 1.0
    return hits / lookups


def warmed_cutover(seed: int, shadow_fill: bool = True,
                   controller: RampController | None = None) -> RunResult:
    """The fix: shadow-fill the new cluster, then ramp only while it is warm and the DB is safe."""
    traffic = Traffic(seed)
    old, new = warm_old_cluster(traffic), LruCache()
    controller = controller or RampController()
    result = RunResult()
    for _ in range(RUN_SECONDS):
        percent = controller.percent()
        stats = serve_second(traffic.next_second(), old, new, percent, shadow_fill)
        controller.observe(stats)
        result.db_reads_per_second.append(stats.db_reads)
        result.failed_reads += stats.failed_reads
        result.percent_on_new.append(percent)
    return result


def normal_db_load(result: RunResult, seconds: int = CUTOVER_SECOND) -> float:
    """Average database reads per second before anything changed."""
    return sum(result.db_reads_per_second[:seconds]) / seconds


def first_second_fully_on_new(result: RunResult) -> int | None:
    """When all reads first went to the new cluster, or None if never."""
    for second, percent in enumerate(result.percent_on_new):
        if percent == 100:
            return second
    return None
