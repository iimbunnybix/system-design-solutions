"""Flash-deal stock sold from several regions without overselling.

The incident: a deal of 1,000 units was sold from three regions. Each region
checked a locally replicated "units sold" count before selling. Replication
between regions lags by a few seconds, so near the end every region still
saw stock that the others had already sold, and the deal oversold by
hundreds of units.

The fix is escrow (allocation): the global quota is split into per-region
allocations. A region sells only from its own allocation, with an atomic local
decrement, so no cross-region read is needed on the hot path. When a region
runs out it asks a coordinator for more; the coordinator hands out unused
units it holds or reclaims from other regions. Units are only ever moved, never
copied, so the regions together can never sell more than the global total.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

DEAL_UNITS = 1000
REGIONS = ("eu", "us", "ap")
TICK_MS = 100                    # simulation step
# Buy attempts per region per 100 ms tick (1,000/s in total; US is heaviest).
DEMAND_PER_TICK = {"eu": 30, "us": 60, "ap": 10}
REPLICATION_LAG_TICKS = 5        # 500 ms before a sale is visible in other regions
SIMULATED_TICKS = 30
REFILL_CHUNK = 50                # units a region asks for when it runs out


# ---------------------------------------------------------------- original
@dataclass
class ReplicatedStockRegion:
    """Original design: sell while the locally replicated sold count < total."""
    name: str
    sold_here: int = 0
    # sold counts of other regions as last replicated here
    seen_from_others: dict[str, int] = field(default_factory=dict)

    def visible_sold(self) -> int:
        """Units sold as this region currently believes (others are stale)."""
        return self.sold_here + sum(self.seen_from_others.values())

    def try_sell(self) -> bool:
        """Sell one unit if the (possibly stale) local view shows stock left."""
        if self.visible_sold() >= DEAL_UNITS:
            return False
        self.sold_here += 1
        return True


def simulate_replicated_check(lag_ticks: int = REPLICATION_LAG_TICKS) -> dict[str, int]:
    """Run the deal with the original check; return units sold per region.

    With ``lag_ticks`` = N, a region sees the others' counts as they were at
    the end of tick ``t - N``. N must be at least 1 (ticks are processed in
    whole-second batches).
    """
    if lag_ticks < 1:
        raise ValueError("lag must be at least one tick")
    regions = {name: ReplicatedStockRegion(name) for name in REGIONS}
    history: list[dict[str, int]] = []      # sold_here per region after each tick
    for tick in range(SIMULATED_TICKS):
        apply_replication(regions, history, tick - lag_ticks)
        for name, region in regions.items():
            for _ in range(DEMAND_PER_TICK[name]):
                region.try_sell()
        history.append({name: region.sold_here for name, region in regions.items()})
    return {name: region.sold_here for name, region in regions.items()}


def apply_replication(regions: dict[str, ReplicatedStockRegion],
                      history: list[dict[str, int]], visible_tick: int) -> None:
    """Make each region see the others' sold counts as of ``visible_tick``."""
    if visible_tick < 0:
        return
    snapshot = history[visible_tick]
    for name, region in regions.items():
        for other, sold in snapshot.items():
            if other != name:
                region.seen_from_others[other] = sold


# ---------------------------------------------------------------- the fix
class RegionAllocation:
    """Units this region may sell. Only this object changes its counter."""

    def __init__(self, name: str, units: int) -> None:
        self.name = name
        self.remaining = units
        self.sold = 0
        self.lock = threading.Lock()

    def try_take_local(self) -> bool:
        """Atomic local decrement: the only check on the hot path."""
        with self.lock:
            if self.remaining == 0:
                return False
            self.remaining -= 1
            self.sold += 1
            return True

    def release_half(self) -> int:
        """Give back half of the unused units (rounded up) to the coordinator."""
        with self.lock:
            released = (self.remaining + 1) // 2
            self.remaining -= released
            return released

    def unsold(self) -> int:
        """Units still allocated here and not yet sold."""
        with self.lock:
            return self.remaining

    def add(self, units: int) -> None:
        """Receive units granted by the coordinator."""
        with self.lock:
            self.remaining += units


class EscrowCoordinator:
    """Owns the global quota and moves unused allocation between regions."""

    def __init__(self, total_units: int, region_names: tuple[str, ...],
                 rebalance: bool = True) -> None:
        self.lock = threading.Lock()
        self.rebalance = rebalance           # False = static split, for comparison
        self.pool = 0                        # units not allocated to any region
        self.regions: dict[str, RegionAllocation] = {}
        share, leftover = divmod(total_units, len(region_names))
        for index, name in enumerate(region_names):
            # Hand the remainder to the first regions so nothing is lost.
            extra = 1 if index < leftover else 0
            self.regions[name] = RegionAllocation(name, share + extra)
        self.refill_requests = 0
        self.sold_out = False                # set once no unit is left anywhere

    def sell(self, region_name: str) -> bool:
        """Sell one unit in a region; refill from the coordinator if it is empty."""
        region = self.regions[region_name]
        if region.try_take_local():
            return True
        # After a global sell-out, answer locally instead of calling the
        # coordinator for every one of the many buyers still clicking.
        if self.sold_out or not self.rebalance:
            return False
        # Never hold the region lock while asking the coordinator (lock order:
        # coordinator first, then region), so refills cannot deadlock.
        self.request_refill(region_name)
        return region.try_take_local()

    def request_refill(self, region_name: str) -> int:
        """Move up to REFILL_CHUNK unused units to the asking region."""
        with self.lock:
            self.refill_requests += 1
            region = self.regions[region_name]
            # Another buyer in this region may have refilled it already.
            if region.unsold() > 0:
                return 0
            if self.pool == 0:
                self.reclaim_from_others(region_name)
            if self.pool == 0:
                # release_half gives back at least 1 of any unsold units, and
                # units only arrive under this lock, so every region is at 0.
                self.sold_out = True
                return 0
            granted = min(REFILL_CHUNK, self.pool)
            self.pool -= granted
            region.add(granted)
            return granted

    def reclaim_from_others(self, region_name: str) -> None:
        """Pull half of every other region's unused units into the pool."""
        for name, region in self.regions.items():
            if name != region_name:
                self.pool += region.release_half()

    def total_sold(self) -> int:
        """Units sold across all regions."""
        return sum(region.sold for region in self.regions.values())

    def units_accounted_for(self) -> int:
        """sold + unsold allocations + pool; must always equal the deal size."""
        unsold = sum(region.remaining for region in self.regions.values())
        return self.total_sold() + unsold + self.pool


def simulate_escrow(rebalance: bool = True) -> EscrowCoordinator:
    """Run the same demand through the escrow design."""
    coordinator = EscrowCoordinator(DEAL_UNITS, REGIONS, rebalance)
    for _ in range(SIMULATED_TICKS):
        for name in REGIONS:
            for _ in range(DEMAND_PER_TICK[name]):
                coordinator.sell(name)
    return coordinator
