"""One viral product's cache key overloads a single cache node.

The incident: product pages are cached in a 10-node cache cluster, and each
key lives on exactly one node (chosen by hashing the key). When one product
went viral, 40% of all reads were for that one key, so its node received four
times its capacity and timed out, while the other nine nodes were mostly idle.
Adding nodes did not help: the hot key still lives on one node.

The fix: count recent requests in a sliding window to detect hot keys, and
serve a hot key from several copies ("key#1", "key#2", ...) placed on
different nodes, picking a copy at random per read. A write updates the home
copy and deletes every other copy, so no copy can serve a stale value.
"""
from __future__ import annotations

import hashlib
import random
from collections import Counter, deque
from typing import Callable

NODE_COUNT = 10
NODE_CAPACITY_PER_S = 20_000         # reads one cache node can serve per second
TOTAL_READS_PER_S = 100_000          # the whole site during the spike
HOT_TRAFFIC_SHARE = 0.4              # share of reads for the viral product
CATALOG_SIZE = 10_000                # other products, read uniformly
VIRAL_KEY = "product:viral"

DETECTOR_WINDOW = 1_000              # look at the last 1,000 reads
HOT_THRESHOLD = 20                   # >= 2% of recent reads makes a key hot
COPY_COUNT = 5                       # home copy + 4 replicas for a hot key

Loader = Callable[[str], str]


class CacheNode:
    """One cache server: a key/value store that counts the reads it serves."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.reads = 0

    def get(self, key: str) -> str | None:
        """Serve one read (hit or miss) and count it as load."""
        self.reads += 1
        return self.store.get(key)


def home_node_index(key: str) -> int:
    """Stable hash placement: the node that owns ``key``."""
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % NODE_COUNT


# ---------------------------------------------------------------- original
class NaiveCacheClient:
    """Original client: every key, however hot, is read from its home node."""

    def __init__(self, nodes: list[CacheNode], load_from_db: Loader) -> None:
        self.nodes = nodes
        self.load_from_db = load_from_db
        self.db_reads = 0

    def get(self, key: str) -> str:
        """Cache-aside read from the key's single home node."""
        node = self.nodes[home_node_index(key)]
        value = node.get(key)
        if value is None:
            self.db_reads += 1
            value = self.load_from_db(key)
            node.store[key] = value
        return value


# ---------------------------------------------------------------- the fix
class HotKeyDetector:
    """Counts each key's reads among the last ``window`` reads."""

    def __init__(self, window: int = DETECTOR_WINDOW, threshold: int = HOT_THRESHOLD) -> None:
        self.window = window
        self.threshold = threshold
        self.recent: deque[str] = deque()
        self.counts: Counter[str] = Counter()

    def record(self, key: str) -> None:
        """Add one read; forget the oldest once the window is full."""
        self.recent.append(key)
        self.counts[key] += 1
        if len(self.recent) > self.window:
            oldest = self.recent.popleft()
            self.counts[oldest] -= 1
            if self.counts[oldest] == 0:
                del self.counts[oldest]

    def is_hot(self, key: str) -> bool:
        """True while the key has at least ``threshold`` recent reads."""
        return self.counts[key] >= self.threshold


def copy_key(key: str, copy_number: int) -> str:
    """Name of a copy in the cache; copy 0 is the key itself."""
    if copy_number == 0:
        return key
    return f"{key}#{copy_number}"


def copy_node_index(key: str, copy_number: int) -> int:
    """Place copy i on the i-th node after home.

    WHY not hash "key#i": independent hashes often put two copies on the same
    node, which would leave that node hot. Consecutive nodes are always distinct.
    """
    return (home_node_index(key) + copy_number) % NODE_COUNT


class ReplicatingCacheClient:
    """Cache client that spreads reads of hot keys over COPY_COUNT nodes."""

    def __init__(self, nodes: list[CacheNode], load_from_db: Loader,
                 detector: HotKeyDetector, seed: int = 0) -> None:
        if COPY_COUNT > len(nodes):
            raise ValueError("cannot place more copies than there are nodes")
        self.nodes = nodes
        self.load_from_db = load_from_db
        self.detector = detector
        self.rng = random.Random(seed)
        self.db_reads = 0

    def get(self, key: str) -> str:
        """Read from a random copy if the key is hot, else from its home node."""
        self.detector.record(key)
        copy_number = 0
        if self.detector.is_hot(key):
            copy_number = self.rng.randrange(COPY_COUNT)
        node = self.nodes[copy_node_index(key, copy_number)]
        value = node.get(copy_key(key, copy_number))
        if value is None:
            value = self.fill_copy(key, copy_number)
        return value

    def fill_copy(self, key: str, copy_number: int) -> str:
        """Fill a missing copy from the home copy, or the database if that misses too.

        WHY from home first: five empty replicas would otherwise cost five
        database reads every time a hot key is updated.
        """
        home = self.nodes[home_node_index(key)]
        value = home.get(key) if copy_number != 0 else None
        if value is None:
            self.db_reads += 1
            value = self.load_from_db(key)
            home.store[key] = value
        self.nodes[copy_node_index(key, copy_number)].store[copy_key(key, copy_number)] = value
        return value

    def set(self, key: str, value: str) -> None:
        """Write the home copy and drop all replicas so none can serve old data."""
        self.nodes[home_node_index(key)].store[key] = value
        self.delete_replicas(key)

    def invalidate(self, key: str) -> None:
        """Delete every copy of a key."""
        self.nodes[home_node_index(key)].store.pop(key, None)
        self.delete_replicas(key)

    def delete_replicas(self, key: str) -> None:
        """Delete copies 1..COPY_COUNT-1, whether or not the key is hot right now.

        WHY always: a key that has cooled down may still have copies left from
        when it was hot; they would be served again the next time it gets hot.
        """
        for copy_number in range(1, COPY_COUNT):
            node = self.nodes[copy_node_index(key, copy_number)]
            node.store.pop(copy_key(key, copy_number), None)


# ---------------------------------------------------------------- simulation
def spike_workload(reads: int = TOTAL_READS_PER_S, seed: int = 29) -> list[str]:
    """One second of reads: 40% for the viral product, the rest spread out."""
    rng = random.Random(seed)
    keys = []
    for _ in range(reads):
        if rng.random() < HOT_TRAFFIC_SHARE:
            keys.append(VIRAL_KEY)
        else:
            keys.append(f"product:{rng.randrange(CATALOG_SIZE)}")
    return keys


def fake_database(key: str) -> str:
    """Stands in for the product database."""
    return f"page for {key}"


def node_loads(nodes: list[CacheNode]) -> list[int]:
    """Reads each node served."""
    return [node.reads for node in nodes]


def rejected_reads(loads: list[int], capacity: int = NODE_CAPACITY_PER_S) -> int:
    """Reads above a node's capacity time out (the model's view of overload)."""
    total = 0
    for load in loads:
        total += max(0, load - capacity)
    return total
