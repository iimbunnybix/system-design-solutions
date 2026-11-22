"""Keep an empty replacement replica out of read quorums until it has the data.

A key-value store keeps 3 copies of every key and uses quorums: a write
waits for 2 acks (W=2), a read asks 2 replicas (R=2) and returns the newest
version. Because R + W > N, every read set overlaps every acknowledged write.
When replica C's disk died, operators started a fresh node with C's identity
and an empty disk, and it served reads at once. An empty disk answers fastest,
so it was in almost every read set. Writes that had been acknowledged by A
and C now existed only on A, and reads that went to {C, B} returned old
values or "not found" for data that was never deleted.

The fix gives each replica a state. A replacement starts BOOTSTRAPPING: it
receives new writes, but it is never asked during reads and its acks do not
count toward the write quorum. It streams every key from the serving peers,
keeping the newest version of each, and only then becomes SERVING. If too few
replicas are serving, reads fail loudly instead of returning wrong data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

READ_QUORUM = 2
WRITE_QUORUM = 2


class ReplicaState(Enum):
    BOOTSTRAPPING = "bootstrapping"   # receives writes, not trusted for reads
    SERVING = "serving"


class QuorumUnavailable(Exception):
    """Not enough serving replicas: refuse rather than answer from too few copies."""


@dataclass(frozen=True)
class Versioned:
    version: int
    value: str


@dataclass
class Replica:
    name: str
    state: ReplicaState = ReplicaState.SERVING
    data: dict[str, Versioned] = field(default_factory=dict)

    def put(self, key: str, item: Versioned) -> None:
        """Store ``item`` unless a newer version is already here."""
        # A bootstrap stream can deliver an old copy after a newer live write.
        current = self.data.get(key)
        if current is None or item.version > current.version:
            self.data[key] = item

    def get(self, key: str) -> Versioned | None:
        return self.data.get(key)


def newest(answers: list[Versioned | None]) -> Versioned | None:
    """The highest version among the replies (None means a replica had nothing)."""
    best = None
    for answer in answers:
        if answer is not None and (best is None or answer.version > best.version):
            best = answer
    return best


class Cluster:
    """Three replicas listed in response-time order (fastest first)."""

    def __init__(self, replica_names: list[str]) -> None:
        self.replicas = [Replica(name) for name in replica_names]

    def replica(self, name: str) -> Replica:
        for replica in self.replicas:
            if replica.name == name:
                return replica
        raise KeyError(name)

    def write(self, key: str, item: Versioned, unreachable: tuple[str, ...] = ()) -> None:
        """Send to every reachable replica; succeed once enough serving ones ack."""
        serving_acks = 0
        for replica in self.replicas:
            if replica.name in unreachable:
                continue
            replica.put(key, item)
            # A bootstrapping copy may still be missing older data, so it
            # cannot be one of the W copies a later read relies on.
            if replica.state is ReplicaState.SERVING:
                serving_acks += 1
        if serving_acks < WRITE_QUORUM:
            raise QuorumUnavailable(f"write {key!r}: only {serving_acks} serving acks")

    def naive_read(self, key: str) -> Versioned | None:
        """Original read: ask the R fastest replicas, whatever their state."""
        chosen = self.replicas[:READ_QUORUM]
        return newest([replica.get(key) for replica in chosen])

    def read(self, key: str) -> Versioned | None:
        """Fixed read: ask the R fastest replicas that are SERVING."""
        serving = [replica for replica in self.replicas if replica.state is ReplicaState.SERVING]
        if len(serving) < READ_QUORUM:
            raise QuorumUnavailable(f"read {key!r}: only {len(serving)} serving replicas")
        chosen = serving[:READ_QUORUM]
        return newest([replica.get(key) for replica in chosen])

    def naive_replace(self, name: str) -> None:
        """Original procedure: new empty disk, same identity, serving at once."""
        self._swap_in(Replica(name, ReplicaState.SERVING))

    def replace(self, name: str) -> None:
        """Fixed procedure: the new empty node starts as BOOTSTRAPPING."""
        self._swap_in(Replica(name, ReplicaState.BOOTSTRAPPING))

    def _swap_in(self, new_replica: Replica) -> None:
        # An empty disk answers fastest, so the new node moves to the front.
        self.replicas = [new_replica] + [r for r in self.replicas if r.name != new_replica.name]

    def bootstrap(self, name: str) -> int:
        """Stream every key from all serving peers, then mark the replica SERVING.

        Copying from all peers (not just one) matters: an acknowledged write
        may now live on only one surviving replica. Returns keys streamed.
        """
        target = self.replica(name)
        streamed = 0
        for peer in self.replicas:
            if peer is target or peer.state is not ReplicaState.SERVING:
                continue
            for key, item in list(peer.data.items()):
                target.put(key, item)
                streamed += 1
        target.state = ReplicaState.SERVING
        return streamed


# --- the incident, as a reproducible scenario --------------------------------
KEY_COUNT = 1_000             # keys written while all three replicas were up
UPDATED_WHILE_B_DOWN = 100    # keys updated during B's restart (acked by A and C)
CREATED_WHILE_B_DOWN = 50     # keys created during B's restart (acked by A and C)


def build_incident_cluster() -> tuple[Cluster, dict[str, Versioned]]:
    """Replay the writes before C's disk failed; return the cluster and the truth.

    The returned dict is the latest acknowledged version of every key.
    """
    cluster = Cluster(["B", "A", "C"])
    latest: dict[str, Versioned] = {}
    for index in range(KEY_COUNT):
        item = Versioned(1, f"profile-{index}-v1")
        cluster.write(f"user:{index}", item)
        latest[f"user:{index}"] = item
    # B restarted for a kernel patch; W=2 writes still succeeded via A and C.
    for index in range(UPDATED_WHILE_B_DOWN):
        item = Versioned(2, f"profile-{index}-v2")
        cluster.write(f"user:{index}", item, unreachable=("B",))
        latest[f"user:{index}"] = item
    for index in range(KEY_COUNT, KEY_COUNT + CREATED_WHILE_B_DOWN):
        item = Versioned(1, f"profile-{index}-v1")
        cluster.write(f"user:{index}", item, unreachable=("B",))
        latest[f"user:{index}"] = item
    return cluster, latest


def wrong_reads(cluster: Cluster, latest: dict[str, Versioned], use_naive: bool) -> dict[str, int]:
    """Count reads that returned an old version or nothing at all."""
    counts = {"stale": 0, "missing": 0}
    for key, expected in latest.items():
        answer = cluster.naive_read(key) if use_naive else cluster.read(key)
        if answer is None:
            counts["missing"] += 1
        elif answer != expected:
            counts["stale"] += 1
    return counts
