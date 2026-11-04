import random
import unittest

from solution import (GeohashIndex, cell_size_deg, covering_cells, encode, haversine_m,
                      naive_nearby)

# A city-sized box (about 44 km x 28 km).
LAT0, LAT1, LON0, LON1 = 18.90, 19.30, 72.78, 73.05


def city(n, seed=1):
    rng = random.Random(seed)
    return {f"d{i}": (rng.uniform(LAT0, LAT1), rng.uniform(LON0, LON1)) for i in range(n)}


def build(drivers, precision=6):
    idx = GeohashIndex(precision)
    for d, (lat, lon) in drivers.items():
        idx.upsert(d, lat, lon)
    return idx


def queries(n, seed=2):
    rng = random.Random(seed)
    return [(rng.uniform(LAT0 + 0.02, LAT1 - 0.02), rng.uniform(LON0 + 0.02, LON1 - 0.02))
            for _ in range(n)]


class ReproduceFullScan(unittest.TestCase):
    def test_naive_query_touches_every_driver(self):
        drivers = city(20_000)
        calls = 0
        original = haversine_m

        import solution
        def counting(*a):
            nonlocal calls
            calls += 1
            return original(*a)
        solution.haversine_m = counting
        try:
            naive_nearby(drivers, 19.1, 72.9, 2_000)
        finally:
            solution.haversine_m = original
        self.assertEqual(calls, 20_000)        # cost grows with the whole fleet


class GeohashFix(unittest.TestCase):
    def test_known_geohash(self):
        self.assertEqual(encode(57.64911, 10.40744, 11), "u4pruydqqvj")
        self.assertEqual(encode(0, 0, 1), "s")

    def test_cell_sizes(self):
        h, w = cell_size_deg(6)
        self.assertAlmostEqual(h * 111_195, 611, delta=1)    # ~0.61 km x 1.22 km
        self.assertAlmostEqual(w * 111_195, 1_222, delta=1)

    def test_results_equal_full_scan(self):
        drivers = city(20_000)
        idx = build(drivers)
        for lat, lon in queries(40):
            for radius in (300, 2_000, 5_000):
                self.assertEqual(idx.nearby(lat, lon, radius),
                                 naive_nearby(drivers, lat, lon, radius))

    def test_reads_a_small_fraction_of_drivers(self):
        drivers = city(50_000)
        idx = build(drivers)
        cand = cells = 0
        qs = queries(100)
        for lat, lon in qs:
            idx.nearby(lat, lon, 2_000)
            cand += idx.last.candidates
            cells += idx.last.cells
        self.assertLess(cand / len(qs), 0.025 * 50_000)   # ~1.8% of drivers examined
        self.assertLess(cells / len(qs), 40)

    def test_driver_just_across_a_cell_edge_is_found(self):
        idx = GeohashIndex(6)
        h, w = cell_size_deg(6)
        # cell boundaries are multiples of h/w from -90/-180
        edge_lon = -180 + round((72.9 + 180) / w) * w
        idx.upsert("near", 19.1, edge_lon + 1e-6)
        rider = (19.1, edge_lon - 1e-6)
        self.assertNotEqual(encode(*rider, 6), encode(19.1, edge_lon + 1e-6, 6))
        self.assertEqual([d for _, d in idx.nearby(*rider, 50)], ["near"])

    def test_antimeridian(self):
        idx = GeohashIndex(6)
        idx.upsert("east", -16.5, 179.995)
        idx.upsert("west", -16.5, -179.995)
        found = [d for _, d in idx.nearby(-16.5, 179.999, 2_000)]
        self.assertEqual(sorted(found), ["east", "west"])

    def test_moving_and_removing_drivers(self):
        idx = GeohashIndex(6)
        idx.upsert("d1", 19.10, 72.90)
        idx.upsert("d1", 19.20, 72.95)          # moved ~12 km
        self.assertEqual(idx.nearby(19.10, 72.90, 1_000), [])
        self.assertEqual([d for _, d in idx.nearby(19.20, 72.95, 1_000)], ["d1"])
        self.assertEqual(sum(len(s) for s in idx.cells.values()), 1)
        idx.remove("d1")
        idx.remove("d1")                        # idempotent
        self.assertEqual(idx.nearby(19.20, 72.95, 1_000), [])
        self.assertEqual(len(idx.cells), 0)

    def test_limit_returns_nearest_first(self):
        idx = GeohashIndex(6)
        for i, off in enumerate([0.003, 0.001, 0.002]):
            idx.upsert(f"d{i}", 19.1 + off, 72.9)
        self.assertEqual([d for _, d in idx.nearby(19.1, 72.9, 1_000, limit=2)], ["d1", "d2"])

    def test_edge_inputs(self):
        with self.assertRaises(ValueError):
            encode(91, 0, 6)
        with self.assertRaises(ValueError):
            covering_cells(0, 0, -1, 6)
        idx = GeohashIndex(6)
        idx.upsert("d", 0.0, 0.0)
        self.assertEqual([d for _, d in idx.nearby(0.0, 0.0, 0)], ["d"])


if __name__ == "__main__":
    unittest.main()
