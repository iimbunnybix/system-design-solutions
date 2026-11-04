# 012 · "Finding drivers near you" took longer every month as the fleet grew, and the matching service fell over on Friday nights

## Problem
A ride-hailing app's matching service answers "which online drivers are
within 2 km of this rider?" for every ride request and every few seconds
while the rider watches the map. Drivers send their location every 4 seconds.
The query computed the distance from the rider to every online driver in the
city. As the fleet grew, the service's CPU grew with it; on Friday nights
with peak riders *and* peak drivers, request latency went from tens of
milliseconds to seconds, matching timed out, and riders saw "no drivers
available" while drivers sat idle nearby.

## Root cause
**Every query was a full scan of the fleet.** Cost per query is proportional
to the number of online drivers in the city, and the number of queries grows
with riders, so total work grows with *riders × drivers*. Nothing in the data
layout let the service skip drivers on the other side of the city. In the
reproduction, one 2 km query over 20,000 drivers computes 20,000 distances.

## Solution
A geohash grid index (`GeohashIndex` in `solution.py`):

1. **Encode each driver's position as a geohash** at precision 6. A geohash
   interleaves longitude and latitude bits, so a prefix is a rectangular cell
   (precision 6 is about 0.61 km × 1.22 km at the equator, narrower in
   longitude away from it). Drivers are stored in a map
   `cell → set(driver)`; a location update moves a driver between cells in O(1).
2. **Query only the cells that cover the search circle.**
   `covering_cells` enumerates every grid cell intersecting the circle's
   bounding box, not just the rider's own cell: a rider near a cell edge has
   drivers metres away in the next cell (tested), and near ±180° longitude the
   neighbouring cell has a completely different geohash (tested).
3. **Filter candidates by exact haversine distance**, so results are
   identical to the full scan (checked on 40 random riders × 3 radii).

Measured on 50,000 drivers spread over a 44 km × 28 km city, 100 random
2 km queries: each query reads about **32 cells** and examines about
**900 drivers (1.8% of the fleet)** to return about 500 matches, and runs in
roughly 1.3 ms instead of 29 ms for the scan on the test machine. The gap
grows with fleet size, since the index's work depends on local density, not
city-wide totals.

## Trade-offs
- **Precision is a tuning knob.** Coarser cells mean fewer cells but more
  wasted candidates (precision 5: ~3 cells but ~2,900 candidates for the same
  queries); finer cells waste fewer candidates but cost many more lookups
  (precision 7: ~780 cells). Pick it for your typical radius, or keep two
  precisions for short and long radii.
- **Hot cells** (an airport queue) still hold many drivers; add a per-cell
  limit or sort by distance within cells.
- Geohash cells are rectangles whose width shrinks towards the poles; the
  covering box accounts for this up to 85° latitude, which is fine for cities.
- In production this usually lives in a store with geo support: Redis `GEO`
  commands (geohash-based sorted sets), PostGIS with a GiST index, or Uber's
  H3 hexagonal grid (uniform neighbour distances). The idea is the same.
- Locations are seconds stale by design; matching must re-check the chosen
  driver's availability atomically when assigning the ride.

## Run
```bash
python -m unittest -v test_solution.py
```
