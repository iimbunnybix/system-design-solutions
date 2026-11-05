"""Find nearby drivers without scanning every driver: a geohash grid index.

``naive_nearby`` is the original query: compute the great-circle distance to
every online driver and keep those within the radius. Cost grows linearly with
the number of drivers in the whole city, for every rider request.

``GeohashIndex`` is the fix:

* each driver's position is encoded as a geohash (an interleaving of longitude
  and latitude bits, base32-encoded); drivers are bucketed by the geohash
  prefix (cell) at a precision chosen for the search radius;
* a query reads only the cells that cover the search circle's bounding box
  (not just the rider's own cell: a rider near a cell edge can have the
  closest drivers in the next cell), then filters candidates by exact
  haversine distance;
* location updates move a driver between cells in O(1).

Longitude wrap-around at +/-180 is handled. The bounding box is computed for
latitudes up to 85 degrees; polar service areas would need a different
projection.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Set, Tuple

BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
EARTH_RADIUS_M = 6_371_000
M_PER_DEG_LAT = math.pi * EARTH_RADIUS_M / 180


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def encode(lat: float, lon: float, precision: int) -> str:
    """Standard geohash encoding."""
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("invalid coordinates")
    lat_rng, lon_rng = [-90.0, 90.0], [-180.0, 180.0]
    out, bits, ch, even = [], 0, 0, True
    while len(out) < precision:
        rng, val = (lon_rng, lon) if even else (lat_rng, lat)
        mid = (rng[0] + rng[1]) / 2
        if val >= mid:
            ch = (ch << 1) | 1
            rng[0] = mid
        else:
            ch <<= 1
            rng[1] = mid
        even = not even
        bits += 1
        if bits == 5:
            out.append(BASE32[ch])
            bits, ch = 0, 0
    return "".join(out)


def cell_size_deg(precision: int) -> Tuple[float, float]:
    """(height in degrees latitude, width in degrees longitude) of a cell."""
    total = 5 * precision
    lon_bits = (total + 1) // 2
    lat_bits = total // 2
    return 180 / 2 ** lat_bits, 360 / 2 ** lon_bits


def covering_cells(lat: float, lon: float, radius_m: float, precision: int) -> Set[str]:
    """All cells at ``precision`` that intersect the circle's bounding box.

    The box is ``radius_m`` north/south and, in longitude, wide enough at the
    latitude furthest from the equator that the circle reaches (where degrees
    of longitude are shortest). Cells are enumerated on the geohash grid, so
    boundaries and the +/-180 meridian are handled exactly.
    """
    if radius_m < 0:
        raise ValueError("radius must be non-negative")
    h, w = cell_size_deg(precision)
    dlat = radius_m / M_PER_DEG_LAT
    reach = min(85.0, abs(lat) + dlat)
    dlon = min(180.0, radius_m / (M_PER_DEG_LAT * math.cos(math.radians(reach))))
    rows = round(180 / h)
    cols = round(360 / w)
    i0 = max(0, math.floor((max(-90.0, lat - dlat) + 90) / h))
    i1 = min(rows - 1, math.floor((min(90.0, lat + dlat) + 90) / h))
    j0 = math.floor((lon - dlon + 180) / w)
    j1 = math.floor((lon + dlon + 180) / w)
    if j1 - j0 + 1 >= cols:
        j0, j1 = 0, cols - 1
    cells = set()
    for i in range(i0, i1 + 1):
        clat = -90 + (i + 0.5) * h
        for j in range(j0, j1 + 1):
            clon = -180 + ((j % cols) + 0.5) * w
            cells.add(encode(clat, clon, precision))
    return cells


def naive_nearby(drivers: Dict[str, Tuple[float, float]], lat: float, lon: float,
                 radius_m: float) -> List[Tuple[float, str]]:
    """Scan every driver. Returns (distance_m, driver_id) sorted by distance."""
    out = []
    for d, (dlat, dlon) in drivers.items():
        dist = haversine_m(lat, lon, dlat, dlon)
        if dist <= radius_m:
            out.append((dist, d))
    return sorted(out)


@dataclass
class QueryStats:
    cells: int = 0
    candidates: int = 0


class GeohashIndex:
    """Drivers bucketed by geohash cell at a fixed precision."""

    def __init__(self, precision: int) -> None:
        self.precision = precision
        self.cells: DefaultDict[str, Set[str]] = defaultdict(set)
        self.position: Dict[str, Tuple[float, float, str]] = {}
        self.last = QueryStats()

    def upsert(self, driver: str, lat: float, lon: float) -> None:
        cell = encode(lat, lon, self.precision)
        old = self.position.get(driver)
        if old is not None and old[2] != cell:
            self.cells[old[2]].discard(driver)
            if not self.cells[old[2]]:
                del self.cells[old[2]]
        self.cells[cell].add(driver)
        self.position[driver] = (lat, lon, cell)

    def remove(self, driver: str) -> None:
        old = self.position.pop(driver, None)
        if old is not None:
            self.cells[old[2]].discard(driver)
            if not self.cells[old[2]]:
                del self.cells[old[2]]

    def nearby(self, lat: float, lon: float, radius_m: float,
               limit: int | None = None) -> List[Tuple[float, str]]:
        """Drivers within ``radius_m``, nearest first.

        Only the cells covering the circle's bounding box are read; candidates
        are then filtered by exact distance, so results equal a full scan.
        """
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")
        stats = QueryStats()
        out = []
        for cell in covering_cells(lat, lon, radius_m, self.precision):
            stats.cells += 1
            for d in self.cells.get(cell, ()):
                stats.candidates += 1
                dlat, dlon, _ = self.position[d]
                dist = haversine_m(lat, lon, dlat, dlon)
                if dist <= radius_m:
                    out.append((dist, d))
        self.last = stats
        out.sort()
        # Compare with None explicitly: limit=0 ("I need no more drivers")
        # must return nothing, not everyone in the radius.
        if limit is None:
            return out
        return out[:limit]
