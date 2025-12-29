from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Iterable, List, Optional, Tuple

import requests

from isbn_harvester.core.models import BookRow
from isbn_harvester.core.scoring import popularity_proxy, rank_score
from isbn_harvester.core.normalize import normalize_subject_term
from isbn_harvester.integrations.http_client import TokenBucket

logger = logging.getLogger(__name__)

_HTML_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


class RateLimitError(RuntimeError):
    def __init__(self, label: str, status_code: int, body_preview: str) -> None:
        super().__init__(f"{label} rate limited (status={status_code})")
        self.label = label
        self.status_code = status_code
        self.body_preview = body_preview


def _clean_text(text: str, max_len: int = 600) -> str:
    if not text:
        return ""
    t = _HTML_RE.sub(" ", str(text))
    t = _WS_RE.sub(" ", t).strip()
    if max_len and len(t) > max_len:
        return t[: max_len - 1].rstrip() + "…"
    return t


def _get_json_with_retries(
    session: requests.Session,
    url: str,
    params: dict,
    timeout_s: int,
    retries: int = 3,
    *,
    debug: bool = False,
    label: str = "",
    max_body_preview: int = 500,
) -> dict:
    backoff = 1.0
    for attempt in range(1, retries + 2):
        try:
            r = session.get(url, params=params, timeout=timeout_s)
            if debug:
                text = r.text or ""
                preview = text.replace("\r", " ").replace("\n", " ").strip()
                if len(preview) > max_body_preview:
                    preview = preview[:max_body_preview].rstrip() + "..."
                logger.info(
                    "%s response status=%s len=%s body=%s",
                    label or "HTTP",
                    r.status_code,
                    len(text),
                    preview,
                )
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt <= retries:
                    time.sleep(min(30.0, backoff))
                    backoff = min(30.0, backoff * 2)
                    continue
                if r.status_code == 429:
                    preview = (r.text or "")[:max_body_preview].replace("\r", " ").replace("\n", " ").strip()
                    raise RateLimitError(label or url, r.status_code, preview)
            r.raise_for_status()
            return r.json() or {}
        except Exception:
            if attempt <= retries:
                time.sleep(min(30.0, backoff))
                backoff = min(30.0, backoff * 2)
                continue
            raise


def _merge_text(base: str, extra: str, max_len: int = 600) -> str:
    base = (base or "").strip()
    if base:
        return base
    extra = _clean_text(extra or "", max_len=max_len)
    return extra


def _merge_subjects(base: str, extra: Iterable[str]) -> str:
    base_parts = [normalize_subject_term(s) for s in (base or "").split(",") if s.strip()]
    seen = {s.lower() for s in base_parts}
    out = [s for s in base_parts if s]
    for s in extra:
        val = normalize_subject_term(s)
        if not val:
            continue
        key = val.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(val)
    return ", ".join(out)


def _payload_has_data(payloads: List[dict]) -> bool:
    for payload in payloads:
        for key, val in payload.items():
            if isinstance(val, list) and val:
                return True
            if isinstance(val, str) and val.strip():
                return True
    return False


def _is_sufficient(subjects: List[str], desc_parts: List[str], *, min_subjects: int, min_desc_len: int) -> bool:
    if len(subjects) >= min_subjects and len(" ".join(desc_parts).strip()) >= min_desc_len:
        return True
    return False


def _combine_payloads(payloads: List[dict]) -> dict:
    combined = {
        "title": "",
        "subtitle": "",
        "authors": "",
        "publisher": "",
        "date_published": "",
        "language": "",
        "pages": "",
        "cover_url": "",
        "cover_url_original": "",
        "google_main_category": "",
        "google_categories": [],
        "google_average_rating": None,
        "google_ratings_count": None,
    }
    for payload in payloads:
        if not combined["title"] and payload.get("title"):
            combined["title"] = payload["title"]
        if not combined["subtitle"] and payload.get("subtitle"):
            combined["subtitle"] = payload["subtitle"]
        if not combined["authors"] and payload.get("authors"):
            combined["authors"] = ", ".join([str(a) for a in payload.get("authors") if str(a).strip()])
        if not combined["publisher"] and payload.get("publisher"):
            combined["publisher"] = payload["publisher"]
        if not combined["date_published"] and payload.get("publish_date"):
            combined["date_published"] = payload["publish_date"]
        if not combined["language"] and payload.get("language"):
            combined["language"] = payload["language"]
        if not combined["pages"] and payload.get("pages"):
            combined["pages"] = payload["pages"]
        if not combined["cover_url"] and payload.get("cover_url"):
            combined["cover_url"] = payload["cover_url"]
        if not combined["cover_url_original"] and payload.get("cover_url"):
            combined["cover_url_original"] = payload["cover_url"]
        if not combined["google_main_category"] and payload.get("google_main_category"):
            combined["google_main_category"] = payload.get("google_main_category") or ""
        if not combined["google_categories"] and payload.get("google_categories"):
            combined["google_categories"] = [str(c) for c in payload.get("google_categories") if str(c).strip()]
        if combined["google_average_rating"] is None and payload.get("google_average_rating") is not None:
            combined["google_average_rating"] = payload.get("google_average_rating")
        if combined["google_ratings_count"] is None and payload.get("google_ratings_count") is not None:
            combined["google_ratings_count"] = payload.get("google_ratings_count")
    return combined


def _apply_external_enrichment(
    row: BookRow,
    combined: dict,
    subjects: List[str],
    desc: str,
    ol_subjects: List[str],
    loc_subjects: List[str],
    fiction_only: bool,
) -> BookRow:
    merged_subjects = _merge_subjects(row.subjects, subjects)
    merged_ol_subjects = _merge_subjects(row.ol_subjects, ol_subjects)
    merged_loc_subjects = _merge_subjects(row.loc_subjects, loc_subjects)
    synopsis = _merge_text(row.synopsis, desc, max_len=320)
    overview = _merge_text(row.overview, desc, max_len=320)

    merged = {
        "title": row.title,
        "subtitle": row.subtitle,
        "authors": row.authors,
        "publisher": row.publisher,
        "date_published": row.date_published,
        "language": row.language,
        "pages": row.pages,
        "cover_url": row.cover_url,
        "cover_url_original": row.cover_url_original,
        "google_main_category": row.google_main_category,
        "google_categories": row.google_categories,
        "google_average_rating": row.google_average_rating,
        "google_ratings_count": row.google_ratings_count,
    }

    for key in ("title", "subtitle", "authors", "publisher", "date_published", "language", "pages"):
        if not merged[key] and combined.get(key):
            merged[key] = combined[key]
    if not merged["cover_url"] and combined.get("cover_url"):
        merged["cover_url"] = combined["cover_url"]
    if not merged["cover_url_original"] and combined.get("cover_url_original"):
        merged["cover_url_original"] = combined["cover_url_original"]
    if not merged["google_main_category"] and combined.get("google_main_category"):
        merged["google_main_category"] = combined.get("google_main_category") or ""
    if not merged["google_categories"] and combined.get("google_categories"):
        merged["google_categories"] = json.dumps(
            [str(c) for c in combined.get("google_categories") if str(c).strip()],
            ensure_ascii=False,
        )
    if merged["google_average_rating"] in (None, 0.0) and combined.get("google_average_rating") is not None:
        try:
            merged["google_average_rating"] = float(combined.get("google_average_rating") or 0.0)
        except Exception:
            pass
    if merged["google_ratings_count"] in (None, 0) and combined.get("google_ratings_count") is not None:
        try:
            merged["google_ratings_count"] = int(combined.get("google_ratings_count") or 0)
        except Exception:
            pass

    title_long = row.title_long or merged["title"]

    updated = replace(
        row,
        title=merged["title"],
        title_long=title_long,
        subtitle=merged["subtitle"],
        authors=merged["authors"],
        publisher=merged["publisher"],
        date_published=merged["date_published"],
        language=merged["language"],
        pages=merged["pages"],
        cover_url=merged["cover_url"],
        cover_url_original=merged["cover_url_original"],
        google_main_category=merged["google_main_category"],
        google_categories=merged["google_categories"],
        google_average_rating=float(merged.get("google_average_rating") or 0.0),
        google_ratings_count=int(merged.get("google_ratings_count") or 0),
        subjects=merged_subjects,
        ol_subjects=merged_ol_subjects,
        loc_subjects=merged_loc_subjects,
        synopsis=synopsis,
        overview=overview,
    )
    updated_pop = popularity_proxy(
        updated.pages,
        updated.date_published,
        updated.language,
        bool(updated.synopsis),
        updated.google_average_rating,
        updated.google_ratings_count,
    )
    updated_rank = rank_score(
        updated.jewish_score,
        updated_pop,
        updated.fiction_flag,
        fiction_only,
        updated.seen_count,
    )
    return replace(updated, popularity_proxy=updated_pop, rank_score=updated_rank)


def _get_openlibrary(
    isbn13: str,
    session: requests.Session,
    timeout_s: int,
    *,
    debug: bool = False,
) -> Tuple[dict, List[str], str]:
    url = "https://openlibrary.org/api/books"
    params = {"bibkeys": f"ISBN:{isbn13}", "format": "json", "jscmd": "data"}
    data = _get_json_with_retries(session, url, params, timeout_s, debug=debug, label="OpenLibrary")
    book = data.get(f"ISBN:{isbn13}", {}) if isinstance(data, dict) else {}
    subjects = []
    for sub in book.get("subjects") or []:
        name = sub.get("name") if isinstance(sub, dict) else str(sub)
        if name:
            subjects.append(name)
    desc = book.get("description") or ""
    if isinstance(desc, dict):
        desc = desc.get("value") or ""
    payload = {
        "title": book.get("title") or "",
        "subtitle": book.get("subtitle") or "",
        "publish_date": book.get("publish_date") or "",
        "publisher": "",
        "authors": [],
        "pages": "",
        "language": "",
        "cover_url": "",
    }
    publishers = book.get("publishers") or []
    if publishers:
        p = publishers[0]
        payload["publisher"] = p.get("name") if isinstance(p, dict) else str(p)
    authors = book.get("authors") or []
    for a in authors:
        name = a.get("name") if isinstance(a, dict) else str(a)
        if name:
            payload["authors"].append(name)
    if book.get("number_of_pages"):
        payload["pages"] = str(book.get("number_of_pages"))
    languages = book.get("languages") or []
    if languages:
        lang = languages[0]
        if isinstance(lang, dict):
            key = lang.get("key") or ""
            payload["language"] = key.rsplit("/", 1)[-1] if "/" in key else key
        else:
            payload["language"] = str(lang)
    cover = book.get("cover") or {}
    if isinstance(cover, dict):
        payload["cover_url"] = cover.get("medium") or cover.get("large") or cover.get("small") or ""
    return payload, subjects, str(desc or "")


def _get_openlibrary_search(
    title: str,
    authors: str,
    session: requests.Session,
    timeout_s: int,
    *,
    debug: bool = False,
) -> Tuple[dict, List[str], str]:
    url = "https://openlibrary.org/search.json"
    params = {"title": title or "", "author": authors or "", "limit": "1"}
    data = _get_json_with_retries(session, url, params, timeout_s, debug=debug, label="OpenLibrarySearch")
    docs = data.get("docs") or []
    if not docs:
        return {}, [], ""
    doc = docs[0] if isinstance(docs[0], dict) else {}
    subjects = doc.get("subject") or []
    desc = doc.get("first_sentence") or ""
    if isinstance(desc, list):
        desc = " ".join(str(x) for x in desc if str(x).strip())
    payload = {
        "title": doc.get("title") or "",
        "subtitle": doc.get("subtitle") or "",
        "publish_date": str(doc.get("first_publish_year") or ""),
        "publisher": (doc.get("publisher") or [""])[0] if isinstance(doc.get("publisher"), list) else "",
        "authors": doc.get("author_name") or [],
        "pages": "",
        "language": (doc.get("language") or [""])[0] if isinstance(doc.get("language"), list) else "",
        "cover_url": "",
    }
    return payload, [str(s) for s in subjects if str(s).strip()], str(desc or "")


def _get_google_books(
    isbn13: str,
    api_key: str,
    session: requests.Session,
    timeout_s: int,
    *,
    debug: bool = False,
) -> Tuple[dict, List[str], str]:
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": f"isbn:{isbn13}",
        "key": api_key,
        "maxResults": "5",
        "printType": "books",
        "projection": "full",
    }
    data = _get_json_with_retries(session, url, params, timeout_s, debug=debug, label="GoogleBooks")
    items = data.get("items") or []
    if not items:
        return {}, [], ""
    info = _pick_google_item(items)
    subjects = info.get("categories") or []
    desc = info.get("description") or ""
    payload = {
        "title": info.get("title") or "",
        "subtitle": info.get("subtitle") or "",
        "publish_date": info.get("publishedDate") or "",
        "publisher": info.get("publisher") or "",
        "authors": info.get("authors") or [],
        "pages": str(info.get("pageCount") or ""),
        "language": info.get("language") or "",
        "cover_url": "",
        "google_main_category": info.get("mainCategory") or "",
        "google_categories": info.get("categories") or [],
        "google_average_rating": info.get("averageRating"),
        "google_ratings_count": info.get("ratingsCount"),
    }
    image_links = info.get("imageLinks") or {}
    if isinstance(image_links, dict):
        payload["cover_url"] = image_links.get("thumbnail") or image_links.get("smallThumbnail") or ""
    return payload, [str(s) for s in subjects if str(s).strip()], str(desc or "")


def _get_google_books_search(
    title: str,
    authors: str,
    api_key: str,
    session: requests.Session,
    timeout_s: int,
    *,
    debug: bool = False,
) -> Tuple[dict, List[str], str]:
    url = "https://www.googleapis.com/books/v1/volumes"
    q_parts = []
    if title:
        q_parts.append(f'intitle:"{title}"')
    author = ""
    if authors:
        author = (authors.split(",")[0] or "").strip()
    if author:
        q_parts.append(f'inauthor:"{author}"')
    if not q_parts:
        return {}, [], ""
    params = {
        "q": " ".join(q_parts),
        "key": api_key,
        "maxResults": "5",
        "printType": "books",
        "projection": "full",
    }
    data = _get_json_with_retries(session, url, params, timeout_s, debug=debug, label="GoogleBooksSearch")
    items = data.get("items") or []
    if not items:
        return {}, [], ""
    info = _pick_google_item(items)
    subjects = info.get("categories") or []
    desc = info.get("description") or ""
    payload = {
        "title": info.get("title") or "",
        "subtitle": info.get("subtitle") or "",
        "publish_date": info.get("publishedDate") or "",
        "publisher": info.get("publisher") or "",
        "authors": info.get("authors") or [],
        "pages": str(info.get("pageCount") or ""),
        "language": info.get("language") or "",
        "cover_url": "",
        "google_main_category": info.get("mainCategory") or "",
        "google_categories": info.get("categories") or [],
        "google_average_rating": info.get("averageRating"),
        "google_ratings_count": info.get("ratingsCount"),
    }
    image_links = info.get("imageLinks") or {}
    if isinstance(image_links, dict):
        payload["cover_url"] = image_links.get("thumbnail") or image_links.get("smallThumbnail") or ""
    return payload, [str(s) for s in subjects if str(s).strip()], str(desc or "")


def _pick_google_item(items: list) -> dict:
    best = None
    best_score = -1
    for item in items:
        info = item.get("volumeInfo") or {}
        score = 0
        if info.get("categories"):
            score += 2
        if info.get("description"):
            score += 1
        if info.get("mainCategory"):
            score += 1
        if score > best_score:
            best_score = score
            best = info
    return best or (items[0].get("volumeInfo") or {})


def _find_loc_record(results: List[dict], isbn13: str) -> Optional[dict]:
    if not results:
        return None
    for rec in results:
        isbns = rec.get("isbn") or []
        if isinstance(isbns, str):
            isbns = [isbns]
        if any(isbn13 == str(i).replace("-", "") for i in isbns):
            return rec
    return results[0]


def _get_loc(
    isbn13: str,
    session: requests.Session,
    timeout_s: int,
    *,
    debug: bool = False,
) -> Tuple[dict, List[str], str]:
    url = "https://www.loc.gov/books/"
    params = {"q": f"isbn:{isbn13}", "fo": "json"}
    data = _get_json_with_retries(session, url, params, timeout_s, debug=debug, label="LOC")
    results = data.get("results") or []
    rec = _find_loc_record(results, isbn13)
    if not rec:
        return {}, [], ""
    subjects = []
    for sub in rec.get("subject") or rec.get("subjects") or []:
        subjects.append(str(sub))
    desc = rec.get("description") or ""
    if isinstance(desc, list):
        desc = " ".join(str(x) for x in desc if str(x).strip())
    payload = {
        "title": rec.get("title") or "",
        "subtitle": "",
        "publish_date": rec.get("date") or "",
        "publisher": "",
        "authors": [],
        "pages": "",
        "language": "",
        "cover_url": "",
    }
    publisher = rec.get("publisher") or []
    if isinstance(publisher, list) and publisher:
        payload["publisher"] = str(publisher[0])
    elif isinstance(publisher, str):
        payload["publisher"] = publisher
    contributors = rec.get("contributor") or rec.get("contributor_name") or []
    if isinstance(contributors, list):
        payload["authors"] = [str(c) for c in contributors if str(c).strip()]
    elif isinstance(contributors, str):
        payload["authors"] = [contributors]
    languages = rec.get("language") or []
    if isinstance(languages, list) and languages:
        payload["language"] = str(languages[0])
    elif isinstance(languages, str):
        payload["language"] = languages
    return payload, [s for s in subjects if s.strip()], str(desc or "")


def _should_enrich(row: BookRow, enrich_all: bool) -> bool:
    if enrich_all:
        return True
    if row.subjects and (row.synopsis or row.overview):
        return False
    return True


def enrich_rows(
    rows: Iterable[BookRow],
    *,
    google_api_key: Optional[str],
    enable_openlibrary: bool = True,
    enable_google_books: bool = True,
    enable_loc: bool = True,
    max_rows: int = 0,
    concurrency: int = 6,
    timeout_s: int = 12,
    rate_per_sec: float = 2.0,
    burst: int = 4,
    enrich_all: bool = False,
    cache_path: Optional[str] = None,
    cache_stats: Optional[dict] = None,
    shortcircuit: bool = True,
    min_subjects: int = 3,
    min_desc_len: int = 200,
    debug: bool = False,
    openlibrary_fallback: bool = True,
    loc_disable_after: int = 3,
    google_fallback: bool = True,
    fiction_only: bool = False,
) -> List[BookRow]:
    rows = list(rows)
    if not rows:
        return []

    rows.sort(key=lambda r: r.rank_score, reverse=True)
    targets = [r for r in rows if _should_enrich(r, enrich_all)]
    if max_rows and max_rows > 0:
        targets = targets[:max_rows]

    if not targets:
        return rows

    limiter_openlibrary = TokenBucket(rate_per_sec, burst)
    limiter_google = TokenBucket(rate_per_sec, burst)
    limiter_loc = TokenBucket(rate_per_sec, burst)
    updated = {r.isbn13: r for r in rows if r.isbn13}
    local = threading.local()
    cache_lock = threading.Lock()
    cache_data = {}
    cache_fh = None
    stats = {"hits": 0, "misses": 0, "writes": 0}
    loc_lock = threading.Lock()
    loc_state = {"disabled": False, "failures": 0}

    if cache_path:
        try:
            os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
            with open(cache_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    isbn = rec.get("isbn13") or ""
                    if not isbn:
                        continue
                    cache_data[isbn] = rec
            cache_fh = open(cache_path, "a", encoding="utf-8")
        except Exception as e:
            logger.warning("External enrich cache unavailable: %r", e)
            cache_data = {}
            cache_fh = None

    def _enrich_one(row: BookRow) -> BookRow:
        session = getattr(local, "session", None)
        if session is None:
            session = requests.Session()
            local.session = session
        if debug:
            logger.info("Enrich start %s", row.isbn13)
        if row.isbn13 and row.isbn13 in cache_data:
            rec = cache_data[row.isbn13]
            combined = rec.get("combined") or {}
            subjects = rec.get("subjects") or []
            desc = rec.get("desc") or ""
            ol_subjects = rec.get("ol_subjects") or []
            loc_subjects = rec.get("loc_subjects") or []
            with cache_lock:
                stats["hits"] += 1
            if debug:
                logger.info("Enrich cache hit %s subjects=%s desc_len=%s", row.isbn13, len(subjects), len(desc))
            return _apply_external_enrichment(row, combined, subjects, desc, ol_subjects, loc_subjects, fiction_only)
        if cache_path:
            with cache_lock:
                stats["misses"] += 1
        subjects: List[str] = []
        desc_parts: List[str] = []
        payloads: List[dict] = []
        ol_subjects: List[str] = []
        loc_subjects: List[str] = []

        if enable_openlibrary:
            limiter_openlibrary.take(1.0)
            try:
                if debug:
                    logger.info(
                        "OpenLibrary request %s params=%s",
                        "https://openlibrary.org/api/books",
                        {"bibkeys": f"ISBN:{row.isbn13}", "format": "json", "jscmd": "data"},
                    )
                payload, s, d = _get_openlibrary(row.isbn13, session, timeout_s, debug=debug)
                payloads.append(payload)
                subjects.extend(s)
                ol_subjects.extend(s)
                if d:
                    desc_parts.append(d)
                if debug:
                    logger.info("OpenLibrary %s subjects=%s desc_len=%s", row.isbn13, len(s), len(d or ""))
                has_ol = bool(s or d or _payload_has_data([payload]))
                if openlibrary_fallback and not has_ol and (row.title or row.authors):
                    if debug:
                        logger.info("OpenLibrary fallback search %s", row.isbn13)
                    spayload, ss, sd = _get_openlibrary_search(row.title, row.authors, session, timeout_s, debug=debug)
                    if spayload or ss or sd:
                        payloads.append(spayload)
                        subjects.extend(ss)
                        ol_subjects.extend(ss)
                        if sd:
                            desc_parts.append(sd)
            except Exception as e:
                logger.debug("OpenLibrary enrich failed %s: %r", row.isbn13, e)

        if (
            enable_google_books
            and google_api_key
            and (
                not shortcircuit
                or not _is_sufficient(
                    subjects, desc_parts, min_subjects=min_subjects, min_desc_len=min_desc_len
                )
            )
        ):
            limiter_google.take(1.0)
            try:
                if debug:
                    logger.info(
                        "GoogleBooks request %s params=%s",
                        "https://www.googleapis.com/books/v1/volumes",
                        {"q": f"isbn:{row.isbn13}", "key": "***"},
                    )
                payload, s, d = _get_google_books(row.isbn13, google_api_key, session, timeout_s, debug=debug)
                payloads.append(payload)
                subjects.extend(s)
                if d:
                    desc_parts.append(d)
                if debug:
                    logger.info("GoogleBooks %s subjects=%s desc_len=%s", row.isbn13, len(s), len(d or ""))
                has_google = bool(s or d or _payload_has_data([payload]))
                if google_fallback and not has_google and (row.title or row.authors):
                    if debug:
                        logger.info("GoogleBooks fallback search %s", row.isbn13)
                    spayload, ss, sd = _get_google_books_search(
                        row.title,
                        row.authors,
                        google_api_key,
                        session,
                        timeout_s,
                        debug=debug,
                    )
                    if spayload or ss or sd:
                        payloads.append(spayload)
                        subjects.extend(ss)
                        if sd:
                            desc_parts.append(sd)
            except Exception as e:
                logger.debug("Google Books enrich failed %s: %r", row.isbn13, e)

        if enable_loc and (
            not shortcircuit
            or not _is_sufficient(
                subjects, desc_parts, min_subjects=min_subjects, min_desc_len=min_desc_len
            )
        ):
            with loc_lock:
                if loc_state["disabled"]:
                    if debug:
                        logger.info("LOC disabled; skipping %s", row.isbn13)
                    enable_loc_local = False
                else:
                    enable_loc_local = True
            if enable_loc_local:
                limiter_loc.take(1.0)
                try:
                    if debug:
                        logger.info(
                            "LOC request %s params=%s",
                            "https://www.loc.gov/books/",
                            {"q": f"isbn:{row.isbn13}", "fo": "json"},
                        )
                    payload, s, d = _get_loc(row.isbn13, session, timeout_s, debug=debug)
                    payloads.append(payload)
                    subjects.extend(s)
                    loc_subjects.extend(s)
                    if d:
                        desc_parts.append(d)
                    if debug:
                        logger.info("LOC %s subjects=%s desc_len=%s", row.isbn13, len(s), len(d or ""))
                except RateLimitError as e:
                    with loc_lock:
                        loc_state["failures"] += 1
                        if loc_state["failures"] >= max(1, int(loc_disable_after)):
                            loc_state["disabled"] = True
                            logger.warning("LOC disabled after %s rate limits", loc_state["failures"])
                except Exception as e:
                    logger.debug("LOC enrich failed %s: %r", row.isbn13, e)

        if not subjects and not desc_parts and not _payload_has_data(payloads):
            if debug:
                logger.info("Enrich no data %s", row.isbn13)
            return row

        combined = _combine_payloads(payloads)
        desc = " ".join(desc_parts)
        updated_row = _apply_external_enrichment(row, combined, subjects, desc, ol_subjects, loc_subjects, fiction_only)

        if cache_fh and row.isbn13:
            rec = {
                "isbn13": row.isbn13,
                "combined": combined,
                "subjects": subjects,
                "desc": desc,
                "ol_subjects": ol_subjects,
                "loc_subjects": loc_subjects,
            }
            with cache_lock:
                cache_data[row.isbn13] = rec
                cache_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                cache_fh.flush()
                stats["writes"] += 1
        if debug:
            logger.info(
                "Enrich done %s subjects=%s desc_len=%s",
                row.isbn13,
                len(subjects),
                len(desc),
            )
        return updated_row

    logger.info(
        "External enrich start: targets=%s openlibrary=%s google_books=%s loc=%s",
        len(targets),
        enable_openlibrary,
        bool(google_api_key) if enable_google_books else False,
        enable_loc,
    )

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        future_map = {ex.submit(_enrich_one, row): row.isbn13 for row in targets}
        for fut in as_completed(future_map):
            isbn = future_map[fut]
            try:
                updated_row = fut.result()
                if updated_row.isbn13:
                    updated[updated_row.isbn13] = updated_row
            except Exception as e:
                logger.debug("External enrich failed %s: %r", isbn, e)

    if cache_fh:
        cache_fh.close()
    if cache_stats is not None:
        cache_stats.update(stats)

    return [updated.get(r.isbn13, r) if r.isbn13 else r for r in rows]
