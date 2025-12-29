from __future__ import annotations

import math
import re
from datetime import datetime
from typing import List, Optional, Tuple

SCORE_TERMS = {
    "jewish": 6, "judaism": 6, "jews": 5, "hebrew": 4, "yiddish": 4,
    "talmud": 4, "torah": 4, "rabbi": 3, "synagogue": 3, "hasidic": 3,
    "hasidism": 3, "kosher": 2, "kabbalah": 3, "sephardic": 3, "ashkenazi": 3,
    "chassidic": 3, "chabad": 3,

    "israel": 5, "jerusalem": 4, "tel aviv": 3, "zionism": 3, "kibbutz": 2,
    "aliyah": 3, "palestine": 2,

    "holocaust": 7, "shoah": 7, "antisemitism": 7, "anti-semitism": 7, "anti semitism": 7,
    "pogrom": 5, "ghetto": 3, "concentration camp": 5,

    "hanukkah": 3, "passover": 3, "rosh hashanah": 3, "yom kippur": 3, "purim": 2,
    "sukkot": 2, "shabbat": 2, "sabbath": 2, "bar mitzvah": 2, "bat mitzvah": 2,

    "midrash": 3, "halacha": 3, "tikkun": 2, "gemara": 3, "siddur": 3,

    "yad vashem": 4, "balfour": 2, "knesset": 2, "idf": 2,
}

NEGATIVE_TERMS = {
    "christmas": -2,
    "easter": -2,
    "church": -2,
    "bible study": -1,
    "new testament": -2,
    "jesus": -3,
}

FICTION_HINTS = (
    "fiction", "novel", "short stories", "mystery", "thriller",
    "fantasy", "romance", "literary fiction",
)
NON_FICTION_HINTS = ("nonfiction", "non-fiction")

_WORDISH = re.compile(r"^[a-z0-9]+$")


_SYNONYM_REPLACEMENTS = {
    "anti-semitism": "antisemitism",
    "anti semitism": "antisemitism",
    "antisemitism": "antisemitism",
    "chassidic": "hasidic",
    "chasidic": "hasidic",
    "shabbos": "shabbat",
}

_SYNONYM_PATTERNS = [
    (re.compile(rf"\\b{re.escape(src)}\\b"), dst) for src, dst in _SYNONYM_REPLACEMENTS.items()
]


def _apply_synonyms(hay: str) -> str:
    for pattern, dst in _SYNONYM_PATTERNS:
        hay = pattern.sub(dst, hay)
    return hay


def _normalize_haystack(*texts: str) -> str:
    hay = " ".join([t or "" for t in texts])
    hay = hay.lower()
    hay = _apply_synonyms(hay)
    hay = re.sub(r"[-–—]", " ", hay)
    hay = re.sub(r"\s+", " ", hay).strip()
    return hay


def _term_in_hay(term: str, hay: str) -> bool:
    t = (term or "").strip().lower()
    if not t:
        return False
    if " " in t:
        return t in hay
    if _WORDISH.match(t):
        return re.search(rf"\b{re.escape(t)}\b", hay) is not None
    return t in hay


def jewish_relevance_score(*texts: str, field_weights: Optional[List[float]] = None) -> Tuple[int, List[str]]:
    weights = field_weights or []
    if weights and len(weights) < len(texts):
        weights = weights + [1.0] * (len(texts) - len(weights))
    elif not weights:
        weights = [1.0] * len(texts)

    score = 0.0
    matched: List[str] = []

    for text, wgt in zip(texts, weights):
        hay = _normalize_haystack(text)
        for term, w in SCORE_TERMS.items():
            if _term_in_hay(term, hay):
                score += w * float(wgt)
                matched.append(term)
        for term, w in NEGATIVE_TERMS.items():
            if _term_in_hay(term, hay):
                score += w * float(wgt)
                matched.append(term)
        if re.search(r"\bjew\w*\b", hay):
            score += 2 * float(wgt)
            matched.append("contains:jew*")

    return int(round(score)), sorted(set(matched))


def fiction_flag(subjects: str, synopsis: str, title: str = "") -> int:
    hay = _normalize_haystack(subjects, synopsis, title)
    if any(_term_in_hay(h, hay) for h in NON_FICTION_HINTS):
        return 0
    return 1 if any(_term_in_hay(h, hay) for h in FICTION_HINTS) else 0


def popularity_proxy(
    pages: str,
    date_published: str,
    language: str,
    has_synopsis: bool,
    average_rating: float | None = None,
    ratings_count: int | None = None,
) -> float:
    p = 0.0
    try:
        n = int(re.sub(r"\D+", "", pages or "") or "0")
        p += min(1.0, n / 600.0) * 0.35
    except Exception:
        pass

    year = None
    try:
        m = re.match(r"^(\d{4})", (date_published or "").strip())
        if m:
            year = int(m.group(1))
    except Exception:
        year = None

    if year:
        age = max(0, datetime.now().year - year)
        p += max(0.0, 1.0 - min(30.0, age) / 30.0) * 0.35
    else:
        p += 0.10

    if (language or "").lower() == "en":
        p += 0.15
    if has_synopsis:
        p += 0.15

    if average_rating is not None and ratings_count is not None:
        try:
            rc = max(0, int(ratings_count))
            ar = max(0.0, min(5.0, float(average_rating)))
            if rc >= 5:
                p += min(0.2, (ar / 5.0) * 0.2)
            if rc >= 50:
                p += 0.05
        except Exception:
            pass

    return max(0.0, min(1.0, p))


def rank_score(jewish_score: int, pop: float, is_fiction: int, fiction_only: bool, seen_count: int) -> float:
    base = (max(-5, jewish_score) / 20.0)
    base = max(-0.25, min(1.5, base))

    score = (base * 0.90) + (pop * 0.10)
    score += (0.10 if is_fiction else 0.0) if not fiction_only else (0.10 if is_fiction else -0.35)

    boost = min(0.15, math.log1p(max(1, int(seen_count))) / 8.0)
    score += boost
    return score
