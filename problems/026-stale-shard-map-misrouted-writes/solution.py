"""Stop clients with an outdated shard map from writing to the wrong shard.

User profiles are range-sharded by user id. Each API server caches the shard
map and routes reads and writes itself. When shard "s1" was split and the
upper half of its keys moved to a new shard "s3", API servers that had not
refreshed their cached map kept sending those users' writes to s1. s1
accepted them, because it never checked whether it still owned the key, so
the writes landed in a copy nobody reads any more and were silently lost.

The fix: every shard knows which key ranges it owns (and the map version
that told it so) and rejects requests for keys it does not own with a
``WrongShard`` reply that carries the current map version. The client then
refreshes its map and retries, a bounded number of times.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import Callable

MAX_ROUTING_ATTEMPTS = 3


@dataclass(frozen=True)
class ShardMap:
    """Sorted range starts -> shard; a key belongs to the last start <= key."""
    version: int
    range_starts: tuple[str, ...]
    shards: tuple[str, ...]

    def shard_for(self, key: str) -> str:
        index = bisect.bisect_right(self.range_starts, key) - 1
        return self.shards[index]


class WrongShard(Exception):
    """This shard does not own the key; refresh the map (newer version given)."""

    def __init__(self, shard: str, key: str, map_version: int) -> None:
        super().__init__(f"{shard} does not own {key!r} (map v{map_version})")
        self.map_version = map_version


@dataclass
class Shard:
    name: str
    shard_map: ShardMap
    check_ownership: bool = True
    data: dict[str, str] = field(default_factory=dict)

    def _require_owner(self, key: str) -> None:
        # The shard, not the client, is the authority on what it owns.
        if self.check_ownership and self.shard_map.shard_for(key) != self.name:
            raise WrongShard(self.name, key, self.shard_map.version)

    def put(self, key: str, value: str) -> None:
        self._require_owner(key)
        self.data[key] = value

    def get(self, key: str) -> str | None:
        self._require_owner(key)
        return self.data.get(key)


class Cluster:
    """The config service plus all shards."""

    def __init__(self, shard_map: ShardMap, check_ownership: bool = True) -> None:
        self.current_map = shard_map
        self.shards: dict[str, Shard] = {}
        for name in shard_map.shards:
            self.shards[name] = Shard(name, shard_map, check_ownership)

    def split(self, old_shard: str, split_key: str, new_shard: str) -> None:
        """Move keys >= split_key from ``old_shard``'s range to ``new_shard``."""
        old_map = self.current_map
        position = bisect.bisect_right(old_map.range_starts, split_key)
        starts = list(old_map.range_starts)
        names = list(old_map.shards)
        starts.insert(position, split_key)
        names.insert(position, new_shard)
        new_map = ShardMap(old_map.version + 1, tuple(starts), tuple(names))
        source = self.shards[old_shard]
        target = Shard(new_shard, new_map, source.check_ownership)
        for key, value in list(source.data.items()):
            if new_map.shard_for(key) == new_shard:
                target.data[key] = source.data.pop(key)
        self.shards[new_shard] = target
        # Publish: every shard learns the new map, then the config service.
        for shard in self.shards.values():
            shard.shard_map = new_map
        self.current_map = new_map


class Client:
    """An API server with its own cached copy of the shard map."""

    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster
        self.cached_map = cluster.current_map
        self.refreshes = 0

    def _refresh(self) -> None:
        self.cached_map = self.cluster.current_map
        self.refreshes += 1

    def put(self, key: str, value: str) -> None:
        self._with_routing(key, lambda shard: shard.put(key, value))

    def get(self, key: str) -> str | None:
        return self._with_routing(key, lambda shard: shard.get(key))

    def _with_routing(self, key: str, operation: Callable[[Shard], str | None]) -> str | None:
        """Send to the cached owner; on WrongShard refresh the map and retry."""
        for _ in range(MAX_ROUTING_ATTEMPTS):
            shard = self.cluster.shards[self.cached_map.shard_for(key)]
            try:
                return operation(shard)
            except WrongShard:
                self._refresh()
        raise RuntimeError(f"could not route {key!r} after {MAX_ROUTING_ATTEMPTS} attempts")
