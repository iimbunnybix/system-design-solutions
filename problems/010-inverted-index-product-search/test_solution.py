import random
import unittest

from solution import InvertedIndex, ScanStats, like_search, normalize

BRANDS = ["Acme", "Nordic", "Zenith", "Café Bleu", "Orion", "Pioneer", "Summit", "Vertex"]
ADJ = ["running", "trail", "waterproof", "lightweight", "leather", "wireless", "ergonomic",
       "organic", "stainless", "kids", "classic", "pro", "compact", "insulated"]
NOUNS = ["shoes", "jacket", "backpack", "headphones", "kettle", "chair", "bottle", "tent",
         "socks", "keyboard", "mug", "lamp", "gloves", "speaker", "blender", "watch"]
COLORS = ["black", "red", "navy", "olive", "white", "grey"]
N_PRODUCTS = 20_000
FALSE_PRO_HITS = 1_985   # "waterproof" products returned for the query "pro"


def catalog(seed=1):
    rng = random.Random(seed)
    out = {}
    for pid in range(N_PRODUCTS):
        words = [rng.choice(BRANDS), rng.choice(ADJ)]
        if rng.random() < 0.5:
            words.append(rng.choice(ADJ))
        words += [rng.choice(NOUNS), "-", rng.choice(COLORS)]
        out[pid] = " ".join(words).title()
    return out


def oracle(products, query, prefix_last=True):
    """Brute-force reference: every query term appears as a token (last one as a prefix)."""
    terms = normalize(query)
    out = set()
    for pid, name in products.items():
        toks = normalize(name)
        ok = all(t in toks for t in terms[:-1])
        last = terms[-1]
        ok = ok and (any(x.startswith(last) for x in toks) if prefix_last else last in toks)
        if ok:
            out.add(pid)
    return out


class Fixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.products = catalog()
        cls.idx = InvertedIndex()
        for pid, name in cls.products.items():
            cls.idx.add(pid, name)


class ReproduceLikeSearch(Fixture):
    def test_every_query_scans_the_whole_table(self):
        stats = ScanStats()
        like_search(self.products, "waterproof jacket", stats)
        self.assertEqual(stats.rows_examined, N_PRODUCTS)

    def test_word_order_and_accents_break_matching(self):
        self.assertGreater(len(like_search(self.products, "running shoes")), 0)
        self.assertEqual(like_search(self.products, "shoes running"), [])
        self.assertEqual(like_search(self.products, "cafe bleu kettle"), [])
        self.assertGreater(len(self.idx.match_all("shoes running")), 0)
        self.assertGreater(len(self.idx.match_all("cafe bleu kettle")), 0)

    def test_substring_matches_unrelated_words(self):
        # "pro" as a substring also hits every "waterproof" product.
        like = set(like_search(self.products, "pro"))
        exact = self.idx.match_all("pro", prefix_last=False)
        false_hits = like - exact
        self.assertEqual(len(false_hits), FALSE_PRO_HITS)
        self.assertTrue(all("waterproof" in self.products[p].lower() for p in false_hits))


class IndexCorrectness(Fixture):
    QUERIES = ["waterproof jacket", "shoes running", "cafe bleu kettle", "nordic wireless head",
               "trail tent olive", "summit stainless bott", "kids socks red", "zz nothing"]

    def test_matches_brute_force_oracle(self):
        for q in self.QUERIES:
            self.assertEqual(self.idx.match_all(q), oracle(self.products, q), q)

    def test_work_is_far_below_a_full_scan(self):
        for q in ("waterproof jacket", "kids socks red", "trail tent olive"):
            self.idx.postings_examined = 0
            self.idx.match_all(q, prefix_last=False)
            self.assertLess(self.idx.postings_examined, N_PRODUCTS * 0.14, q)  # ~13% vs 100%

    def test_ranking_prefers_repeated_and_rarer_terms(self):
        idx = InvertedIndex()
        idx.add(1, "Trail Running Shoes")
        idx.add(2, "Trail Trail Running Shoes")          # term frequency
        idx.add(3, "Running Socks")
        self.assertEqual(idx.search("trail running", prefix_last=False), [2, 1])

    def test_prefix_search_as_you_type(self):
        self.assertEqual(self.idx.match_all("waterproof jack"), self.idx.match_all("waterproof jacket"))
        self.assertEqual(len(self.idx.search("headph", limit=5)), 5)

    def test_prefix_expansion_is_capped(self):
        idx = InvertedIndex(max_prefix_expansions=3)
        for i, w in enumerate(["sa", "sb", "sc", "sd", "se"]):
            idx.add(i, w)
        self.assertEqual(idx.expand_prefix("s"), ["sa", "sb", "sc"])

    def test_update_and_remove(self):
        idx = InvertedIndex()
        idx.add(1, "Red Kettle")
        idx.add(1, "Blue Kettle")                       # rename re-indexes
        self.assertEqual(idx.search("red", prefix_last=False), [])
        self.assertEqual(idx.search("blue kettle"), [1])
        idx.remove(1)
        self.assertEqual(idx.search("kettle"), [])
        self.assertNotIn("kettle", idx.postings)

    def test_empty_and_punctuation_queries(self):
        self.assertEqual(self.idx.search(""), [])
        self.assertEqual(self.idx.search("  --  "), [])
        self.assertEqual(InvertedIndex().search("anything"), [])

    def test_normalize(self):
        self.assertEqual(normalize("Café BLEU – Pro-Max 2"), ["cafe", "bleu", "pro", "max", "2"])


if __name__ == "__main__":
    unittest.main()
