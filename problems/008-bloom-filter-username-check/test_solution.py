import random
import string
import unittest

from solution import BloomFilter, UsernameService, normalize

N = 50_000


def names(n, seed, prefix):
    rng = random.Random(seed)
    return [prefix + "".join(rng.choices(string.ascii_lowercase + string.digits, k=10)) for _ in range(n)]


TAKEN = names(N, 1, "u_")
FREE = names(N, 2, "f_")            # disjoint by prefix


def checks(seed=3, total=20_000, taken_share=0.1):
    rng = random.Random(seed)
    return [rng.choice(TAKEN) if rng.random() < taken_share else rng.choice(FREE) for _ in range(total)]


class ReproduceDbLoad(unittest.TestCase):
    def test_every_check_hits_the_database(self):
        svc = UsernameService(set(TAKEN), bloom=None)
        qs = checks()
        for q in qs:
            svc.is_available(q)
        self.assertEqual(svc.db_queries, len(qs))


class WithBloom(unittest.TestCase):
    def test_sizing_matches_formula(self):
        bf = BloomFilter(N, 0.01)
        self.assertEqual(bf.m, 479_253)          # ~9.6 bits per item
        self.assertEqual(bf.k, 7)
        self.assertEqual(len(bf.bits), 59_907)   # ~59 KB for 50k names

    def test_no_false_negatives_and_low_fp_rate(self):
        bf = BloomFilter(N, 0.01)
        for n in TAKEN:
            bf.add(n)
        self.assertTrue(all(n in bf for n in TAKEN))
        fp = sum(n in bf for n in FREE) / len(FREE)
        self.assertLess(fp, 0.0125)
        self.assertAlmostEqual(bf.expected_fp_rate(), 0.01, delta=0.001)

    def test_database_queries_drop_and_answers_unchanged(self):
        plain = UsernameService(set(TAKEN), None)
        svc = UsernameService(set(TAKEN), BloomFilter(N, 0.01))
        qs = checks()
        self.assertEqual([svc.is_available(q) for q in qs], [plain.is_available(q) for q in qs])
        self.assertLess(svc.db_queries, len(qs) * 0.12)   # ~10% taken + ~1% of the free ones

    def test_register_updates_filter_immediately(self):
        svc = UsernameService(set(TAKEN[:100]), BloomFilter(1000, 0.01))
        self.assertTrue(svc.is_available("NewUser"))
        self.assertTrue(svc.register("NewUser"))
        self.assertFalse(svc.is_available("newuser"))
        self.assertFalse(svc.register(" NEWUSER "))        # case/space-insensitive duplicate

    def test_overfilled_filter_degrades(self):
        bf = BloomFilter(1000, 0.01)
        for n in TAKEN[:5000]:
            bf.add(n)
        self.assertTrue(bf.needs_rebuild())
        fp = sum(n in bf for n in FREE[:5000]) / 5000
        self.assertGreater(fp, 0.3)                          # far above the 1% design point

    def test_serialization_round_trip(self):
        bf = BloomFilter(1000, 0.01)
        for n in TAKEN[:500]:
            bf.add(n)
        clone = BloomFilter.from_bytes(bf.to_bytes())
        self.assertEqual((clone.m, clone.k, clone.count), (bf.m, bf.k, 500))
        self.assertTrue(all(n in clone for n in TAKEN[:500]))
        with self.assertRaises(ValueError):
            BloomFilter.from_bytes(bf.to_bytes()[:-1])

    def test_invalid_parameters(self):
        for cap, p in ((0, 0.01), (10, 0), (10, 1)):
            with self.assertRaises(ValueError):
                BloomFilter(cap, p)

    def test_normalize(self):
        self.assertEqual(normalize("  Ravi_K "), "ravi_k")
        self.assertIn("é", normalize("José"))


if __name__ == "__main__":
    unittest.main()
