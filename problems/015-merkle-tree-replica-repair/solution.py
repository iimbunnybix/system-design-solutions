"""Merkle-tree anti-entropy for repairing diverged replicas.

Two replicas of a key-value store silently drift apart when one of them misses
writes (a dropped message, a node that was down). The original repair job
compared the replicas key by key, which means shipping every key and version
over the network even when only a handful differ.

This module splits the key space into a fixed number of buckets, hashes each
bucket, and builds a binary Merkle tree over the bucket hashes. Two replicas
compare roots first and only walk down into subtrees whose hashes differ, so
the keys actually exchanged are limited to the few buckets that really
diverged.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

# Number of leaf buckets. Must be a power of two so the tree is a full binary
# tree. 1024 keeps buckets small for millions of keys while the tree itself
# stays tiny (2047 hashes).
DEFAULT_LEAF_COUNT = 1024

# Hash of an empty bucket, so empty buckets compare equal on both replicas.
EMPTY_BUCKET_HASH = hashlib.sha256(b"").digest()

# Field widths of the record encoding used for bucket hashes.
LENGTH_PREFIX_BYTES = 4
VERSION_BYTES = 8


@dataclass(frozen=True)
class Record:
    """A stored value with a version; the higher version is the newer write."""

    version: int
    value: str


@dataclass
class SyncStats:
    """What a repair run had to send over the network."""

    hashes_compared: int = 0
    keys_exchanged: int = 0
    keys_repaired: int = 0


@dataclass
class Replica:
    """One copy of the data: key -> Record."""

    name: str
    data: dict[str, Record] = field(default_factory=dict)

    def put(self, key: str, version: int, value: str) -> None:
        """Store a write if it is newer than what this replica already has."""
        current = self.data.get(key)
        if current is None or version > current.version:
            self.data[key] = Record(version, value)


def bucket_of(key: str, leaf_count: int) -> int:
    """Map a key to a leaf bucket using a stable hash (not Python's hash())."""
    # Python's built-in hash() is randomised per process, so two replicas
    # would disagree about bucket numbers. SHA-256 is the same everywhere.
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:8], "big") % leaf_count


def length_prefixed(text: str) -> bytes:
    """Encode a string as its byte length followed by its bytes."""
    encoded = text.encode()
    return len(encoded).to_bytes(LENGTH_PREFIX_BYTES, "big") + encoded


def encode_record(key: str, record: Record) -> bytes:
    """Serialize one record so that no two different records share bytes.

    Separator characters are not enough: keys and values are arbitrary
    strings and may contain the separator themselves. A length prefix on every
    field makes the encoding unambiguous whatever the field contains.
    """
    version_bytes = record.version.to_bytes(VERSION_BYTES, "big", signed=True)
    return length_prefixed(key) + version_bytes + length_prefixed(record.value)


def hash_bucket(replica: Replica, keys: list[str]) -> bytes:
    """Hash every key, version and value in a bucket into one digest."""
    if not keys:
        return EMPTY_BUCKET_HASH
    hasher = hashlib.sha256()
    for key in keys:
        hasher.update(encode_record(key, replica.data[key]))
    return hasher.digest()


class MerkleTree:
    """Binary hash tree over a replica's buckets.

    ``levels[0]`` is the root level (one hash); the last level holds one hash
    per leaf bucket. Node ``i`` on a level has children ``2i`` and ``2i+1``.
    """

    def __init__(self, replica: Replica, leaf_count: int = DEFAULT_LEAF_COUNT):
        if leaf_count < 1 or leaf_count & (leaf_count - 1) != 0:
            raise ValueError("leaf_count must be a power of two")
        self.leaf_count = leaf_count
        self.bucket_keys = self._group_keys(replica)
        leaves = [hash_bucket(replica, keys) for keys in self.bucket_keys]
        self.levels = self._build_levels(leaves)

    def _group_keys(self, replica: Replica) -> list[list[str]]:
        """Put each key into its bucket in one pass over the data."""
        buckets: list[list[str]] = [[] for _ in range(self.leaf_count)]
        for key in replica.data:
            buckets[bucket_of(key, self.leaf_count)].append(key)
        for keys in buckets:
            keys.sort()
        return buckets

    @staticmethod
    def _build_levels(leaves: list[bytes]) -> list[list[bytes]]:
        """Hash pairs of children upwards until a single root remains."""
        levels = [leaves]
        while len(levels[0]) > 1:
            children = levels[0]
            parents = []
            for index in range(0, len(children), 2):
                parents.append(hashlib.sha256(children[index] + children[index + 1]).digest())
            levels.insert(0, parents)
        return levels

    @property
    def root(self) -> bytes:
        """Hash that summarises the whole replica."""
        return self.levels[0][0]


def differing_buckets(left: MerkleTree, right: MerkleTree, stats: SyncStats) -> list[int]:
    """Walk both trees from the root and return leaf buckets whose hashes differ."""
    if left.leaf_count != right.leaf_count:
        raise ValueError("both trees must use the same leaf_count")
    # Start at the root; each step only keeps the nodes that differ.
    suspects = [0]
    for depth in range(len(left.levels)):
        next_suspects = []
        for node in suspects:
            stats.hashes_compared += 1
            if left.levels[depth][node] == right.levels[depth][node]:
                continue
            next_suspects.append(node)
        is_leaf_level = depth == len(left.levels) - 1
        if is_leaf_level:
            return next_suspects
        suspects = []
        for node in next_suspects:
            suspects.extend([2 * node, 2 * node + 1])
    return []


def choose_winner(left_record: Record | None, right_record: Record | None) -> Record:
    """Pick the record both replicas should keep: the higher version wins."""
    if left_record is None:
        return right_record
    if right_record is None:
        return left_record
    # Same version but different values means corruption or a buggy writer;
    # break the tie on the value so both replicas still end up identical.
    left_rank = (left_record.version, left_record.value)
    right_rank = (right_record.version, right_record.value)
    return left_record if left_rank >= right_rank else right_record


def reconcile_keys(left: Replica, right: Replica, keys: set[str], stats: SyncStats) -> None:
    """Make both replicas agree on ``keys`` by copying the winning record."""
    for key in sorted(keys):
        stats.keys_exchanged += 1
        left_record = left.data.get(key)
        right_record = right.data.get(key)
        if left_record == right_record:
            continue
        stats.keys_repaired += 1
        winner = choose_winner(left_record, right_record)
        left.data[key] = winner
        right.data[key] = winner


def merkle_repair(left: Replica, right: Replica, leaf_count: int = DEFAULT_LEAF_COUNT) -> SyncStats:
    """Repair two replicas by exchanging only the keys of differing buckets."""
    stats = SyncStats()
    left_tree = MerkleTree(left, leaf_count)
    right_tree = MerkleTree(right, leaf_count)
    for bucket in differing_buckets(left_tree, right_tree, stats):
        keys = set(left_tree.bucket_keys[bucket]) | set(right_tree.bucket_keys[bucket])
        reconcile_keys(left, right, keys, stats)
    return stats


def naive_full_compare_repair(left: Replica, right: Replica) -> SyncStats:
    """Original repair job: ship and compare every single key."""
    stats = SyncStats()
    all_keys = set(left.data) | set(right.data)
    reconcile_keys(left, right, all_keys, stats)
    return stats
