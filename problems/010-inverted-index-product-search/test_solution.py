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


class CountingLengths(dict):
    """doc_len stand-in that counts how many document lengths are read in bulk."""

    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.bulk_reads = 0

    def values(self):
        self.bulk_reads += len(self)
        return super().values()


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

    def test_ranking_does_not_rescan_every_length_per_candidate(self):
        idx = InvertedIndex()
        for pid, name in self.products.items():
            idx.add(pid, name)
        idx.doc_len = CountingLengths(idx.doc_len)
        matches = len(idx.match_all("black"))           # 3,345 of 20,000 products
        self.assertGreater(matches, 3_000)
        idx.search("black")
        # The original summed all 20,000 lengths for each of the 3,345 matches.
        self.assertLess(idx.doc_len.bulk_reads, N_PRODUCTS)

    def test_total_length_tracks_add_rename_and_remove(self):
        idx = InvertedIndex()
        idx.add(1, "Red Kettle")
        idx.add(2, "Trail Running Shoes")
        idx.add(1, "Blue Steel Kettle")                 # rename replaces the old length
        self.assertEqual(idx.total_len, 6)
        idx.remove(2)
        self.assertEqual(idx.total_len, 3)
        idx.remove(1)
        self.assertEqual(idx.total_len, 0)

    def test_empty_and_punctuation_queries(self):
        self.assertEqual(self.idx.search(""), [])
        self.assertEqual(self.idx.search("  --  "), [])
        self.assertEqual(InvertedIndex().search("anything"), [])

    def test_normalize(self):
        self.assertEqual(normalize("Café BLEU – Pro-Max 2"), ["cafe", "bleu", "pro", "max", "2"])


class NonAsciiNames(unittest.TestCase):
    """Issue #13: letters outside ASCII must be kept, not treated as separators."""

    def test_cyrillic_name_is_found_by_its_own_word(self):
        idx = InvertedIndex()
        idx.add(1, "Кофе Арабика 1 кг")
        self.assertEqual(idx.search("кофе"), [1])

    def test_sharp_s_matches_its_ascii_spelling(self):
        idx = InvertedIndex()
        idx.add(2, "Straße Map")
        self.assertEqual(idx.search("strasse"), [2])
        self.assertEqual(idx.search("STRASSE map"), [2])

    def test_letters_without_ascii_decomposition_are_kept(self):
        self.assertEqual(normalize("Straße Œuvre ØL"), ["strasse", "œuvre", "øl"])

    def test_underscore_still_separates_words(self):
        self.assertEqual(normalize("snake_case"), ["snake", "case"])


if __name__ == "__main__":
    unittest.main()
