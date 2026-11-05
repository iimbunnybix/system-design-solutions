"""Product search with an inverted index instead of ``LIKE '%term%'`` scans.

The original search ran ``WHERE lower(name) LIKE '%<query>%'``. A leading
wildcard cannot use a B-tree index, so every search read every product row,
and the substring semantics were wrong for users anyway: "shoes running" did
not find "Running Shoes", "cafe" did not find "Café", and results had no
relevance order.

``InvertedIndex`` maps each normalised token to the set of products containing
it. A query intersects the posting lists of its terms (smallest first), treats
the last term as a prefix for search-as-you-type, and ranks with BM25.
"""
from __future__ import annotations

import bisect
import math
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass

# Letters and digits in ANY script. "[^\W_]" means "a word character that is
# not an underscore". An ASCII-only class like [a-z0-9] silently dropped
# Cyrillic, Greek and CJK names and split "Straße" into "stra" + "e".
TOKEN_RE = re.compile(r"[^\W_]+")


def normalize(text: str) -> list[str]:
    """Case-fold, strip accents (é -> e), split on anything that is not a letter or digit.

    ``casefold()`` is used instead of ``lower()`` because it also folds
    letters such as "ß" to "ss", so "Straße" and "strasse" match.
    """
    decomposed = unicodedata.normalize("NFKD", text.casefold())
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return TOKEN_RE.findall(stripped)


# ------------------------------------------------------------- the old way
@dataclass
class ScanStats:
    rows_examined: int = 0


def like_search(products: dict[int, str], query: str, stats: ScanStats | None = None) -> list[int]:
    """Equivalent of ``WHERE lower(name) LIKE lower('%query%')``: full scan, substring match."""
    q = query.lower()
    out = []
    for pid, name in products.items():
        if stats:
            stats.rows_examined += 1
        if q in name.lower():
            out.append(pid)
    return out


# ------------------------------------------------------------------ the fix
class InvertedIndex:
    """In-memory inverted index with BM25 ranking and prefix matching on the last term.

    Args:
        k1, b: BM25 parameters (standard defaults 1.2 and 0.75).
        max_prefix_expansions: cap on vocabulary terms a prefix can expand to,
            so a one-letter prefix cannot turn into a huge union.
    """

    def __init__(self, k1: float = 1.2, b: float = 0.75, max_prefix_expansions: int = 50) -> None:
        self.k1 = k1
        self.b = b
        self.max_prefix_expansions = max_prefix_expansions
        self.postings: dict[str, set[int]] = {}
        self.term_freqs: dict[int, Counter] = {}
        self.doc_len: dict[int, int] = {}
        # Running sum of all document lengths, so BM25's average length is
        # O(1) per query instead of a pass over the whole catalog per candidate.
        self.total_len = 0
        self._vocab: list[str] = []      # sorted, for prefix lookup
        self._vocab_dirty = False
        self.postings_examined = 0

    # ---------------------------------------------------------- maintenance
    def add(self, doc_id: int, text: str) -> None:
        if doc_id in self.term_freqs:
            self.remove(doc_id)
        tokens = normalize(text)
        tf = Counter(tokens)
        self.term_freqs[doc_id] = tf
        self.doc_len[doc_id] = len(tokens)
        self.total_len += len(tokens)
        for term in tf:
            if term not in self.postings:
                self.postings[term] = set()
                self._vocab_dirty = True
            self.postings[term].add(doc_id)

    def remove(self, doc_id: int) -> None:
        tf = self.term_freqs.pop(doc_id, None)
        if tf is None:
            return
        self.total_len -= self.doc_len.pop(doc_id)
        for term in tf:
            ids = self.postings[term]
            ids.discard(doc_id)
            if not ids:
                del self.postings[term]
                self._vocab_dirty = True

    def _vocabulary(self) -> list[str]:
        if self._vocab_dirty:
            self._vocab = sorted(self.postings)
            self._vocab_dirty = False
        return self._vocab

    def expand_prefix(self, prefix: str) -> list[str]:
        vocab = self._vocabulary()
        i = bisect.bisect_left(vocab, prefix)
        out = []
        while i < len(vocab) and vocab[i].startswith(prefix) and len(out) < self.max_prefix_expansions:
            out.append(vocab[i])
            i += 1
        return out

    # --------------------------------------------------------------- search
    def _candidates(self, terms: list[str], prefix_last: bool) -> tuple[set[int], list[list[str]]]:
        groups: list[list[str]] = []
        for i, term in enumerate(terms):
            if prefix_last and i == len(terms) - 1:
                groups.append(self.expand_prefix(term))
            else:
                groups.append([term] if term in self.postings else [])
        if any(not g for g in groups):
            return set(), groups
        sets = []
        for g in groups:
            if len(g) == 1:
                sets.append(self.postings[g[0]])
            else:
                union: set[int] = set()
                for t in g:
                    union |= self.postings[t]
                sets.append(union)
        sets.sort(key=len)                         # intersect smallest first
        result = set(sets[0])
        self.postings_examined += len(sets[0])
        for s in sets[1:]:
            if not result:
                break
            self.postings_examined += len(result)  # each lookup is an O(1) membership test
            result &= s
        return result, groups

    def _bm25(self, doc_id: int, groups: list[list[str]], n: int, avgdl: float) -> float:
        """BM25 score of one candidate. ``n`` and ``avgdl`` are computed once per query."""
        dl = self.doc_len[doc_id]
        tf_doc = self.term_freqs[doc_id]
        score = 0.0
        for g in groups:
            best = 0.0
            for term in g:
                tf = tf_doc.get(term, 0)
                if not tf:
                    continue
                df = len(self.postings[term])
                idf = math.log(1 + (n - df + 0.5) / (df + 0.5))
                best = max(best, idf * tf * (self.k1 + 1) /
                           (tf + self.k1 * (1 - self.b + self.b * dl / avgdl)))
            score += best
        return score

    def search(self, query: str, limit: int = 10, prefix_last: bool = True) -> list[int]:
        """All terms must match (AND). Results ranked by BM25, ties by id."""
        terms = normalize(query)
        if not terms or not self.doc_len:
            return []
        ids, groups = self._candidates(terms, prefix_last)
        # Catalog-wide statistics once per query, not once per candidate:
        # recomputing the average length inside _bm25 made ranking
        # O(matches x catalog size).
        n = len(self.doc_len)
        avgdl = self.total_len / n
        ranked = sorted(ids, key=lambda d: (-self._bm25(d, groups, n, avgdl), d))
        return ranked[:limit]

    def match_all(self, query: str, prefix_last: bool = True) -> set[int]:
        """Unranked, unlimited match set (used for correctness checks)."""
        terms = normalize(query)
        if not terms:
            return set()
        return self._candidates(terms, prefix_last)[0]
