from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import asdict
from typing import Iterable, List

from isbn_harvester.core.models import BookRow
from isbn_harvester.io.utils import atomic_write_csv

logger = logging.getLogger(__name__)

MAX_ROWS_WARN = 100000
MAX_ROW_CHARS_WARN = 50000

FULL_CSV_FIELDS = [
        "isbn13", "isbn10",
        "title", "title_long",
        "subtitle",
        "edition",
        "dimensions",
        "authors",
        "date_published",
        "publisher",
        "language",
        "subjects",
        "ol_subjects",
        "loc_subjects",
        "pages",
        "format",
        "synopsis",
        "overview",
        "cover_url",
        "cover_url_original",
        "cover_expires_at",
        "s3_cover_key",
        "cloudfront_cover_url",
        "bookshop_url",
        "bookshop_affiliate_url",
        "jewish_score",
        "fiction_flag",
        "popularity_proxy",
        "rank_score",
        "matched_terms",
        "seen_count",
        "sources",
        "shopify_tags",
        "taxonomy_content_type",
        "taxonomy_primary_genre",
        "taxonomy_jewish_themes",
        "taxonomy_geography",
        "taxonomy_historical_era",
        "taxonomy_religious_orientation",
        "taxonomy_cultural_tradition",
        "taxonomy_language",
        "taxonomy_character_focus",
        "taxonomy_narrative_style",
        "taxonomy_emotional_tone",
        "taxonomy_high_level_categories",
        "taxonomy_confidence",
        "taxonomy_tags",
        "google_main_category",
        "google_categories",
        "google_average_rating",
        "google_ratings_count",
        "task_endpoint",
        "task_group",
        "task_query",
        "page",
    ]


def _write_schema(out_path: str) -> None:
    meta_path = f"{out_path}.schema.json"
    payload = {
        "version": 2,
        "generated_at": int(time.time()),
        "fields": FULL_CSV_FIELDS,
    }

    def _write(path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)

    atomic_write_csv(_write, meta_path)
    logger.debug("Wrote schema: %s", meta_path)


def write_full_csv(rows: Iterable[BookRow], out_path: str) -> None:
    rows = list(rows)
    fieldnames = list(FULL_CSV_FIELDS)
    max_row_len = [0]
    max_row_isbn = [""]

    def _write(path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                row_dict = asdict(r)
                row_len = sum(len(str(v)) for v in row_dict.values() if v is not None)
                if row_len > max_row_len[0]:
                    max_row_len[0] = row_len
                    max_row_isbn[0] = r.isbn13
                w.writerow(row_dict)

    atomic_write_csv(_write, out_path)
    _write_schema(out_path)
    logger.info("Wrote full CSV: %s rows=%s", out_path, len(rows))
    if len(rows) > MAX_ROWS_WARN:
        logger.warning("Large CSV: rows=%s (warn threshold=%s)", len(rows), MAX_ROWS_WARN)
    if max_row_len[0] > MAX_ROW_CHARS_WARN:
        logger.warning(
            "Large row detected: isbn13=%s size=%s chars (warn threshold=%s)",
            max_row_isbn[0] or "(unknown)",
            max_row_len[0],
            MAX_ROW_CHARS_WARN,
        )


def _to_int(val: str) -> int:
    try:
        return int(val)
    except Exception:
        return 0


def _to_float(val: str) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def read_full_csv(path: str) -> List[BookRow]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows: List[BookRow] = []
        for rec in reader:
            rows.append(
                BookRow(
                    isbn13=rec.get("isbn13", ""),
                    isbn10=rec.get("isbn10", ""),
                    title=rec.get("title", ""),
                    title_long=rec.get("title_long", ""),
                    subtitle=rec.get("subtitle", ""),
                    edition=rec.get("edition", ""),
                    dimensions=rec.get("dimensions", ""),
                    authors=rec.get("authors", ""),
                    date_published=rec.get("date_published", ""),
                    publisher=rec.get("publisher", ""),
                    language=rec.get("language", ""),
                    subjects=rec.get("subjects", ""),
                    ol_subjects=rec.get("ol_subjects", ""),
                    loc_subjects=rec.get("loc_subjects", ""),
                    pages=rec.get("pages", ""),
                    format=rec.get("format", ""),
                    synopsis=rec.get("synopsis", ""),
                    overview=rec.get("overview", ""),
                    cover_url=rec.get("cover_url", ""),
                    cover_url_original=rec.get("cover_url_original", ""),
                    cover_expires_at=_to_int(rec.get("cover_expires_at", "0")),
                    s3_cover_key=rec.get("s3_cover_key", ""),
                    cloudfront_cover_url=rec.get("cloudfront_cover_url", ""),
                    bookshop_url=rec.get("bookshop_url", ""),
                    bookshop_affiliate_url=rec.get("bookshop_affiliate_url", ""),
                    jewish_score=_to_int(rec.get("jewish_score", "0")),
                    fiction_flag=_to_int(rec.get("fiction_flag", "0")),
                    popularity_proxy=_to_float(rec.get("popularity_proxy", "0")),
                    rank_score=_to_float(rec.get("rank_score", "0")),
                    matched_terms=rec.get("matched_terms", ""),
                    seen_count=_to_int(rec.get("seen_count", "0")),
                    sources=rec.get("sources", ""),
                    shopify_tags=rec.get("shopify_tags", ""),
                    taxonomy_content_type=rec.get("taxonomy_content_type", ""),
                    taxonomy_primary_genre=rec.get("taxonomy_primary_genre", ""),
                    taxonomy_jewish_themes=rec.get("taxonomy_jewish_themes", ""),
                    taxonomy_geography=rec.get("taxonomy_geography", ""),
                    taxonomy_historical_era=rec.get("taxonomy_historical_era", ""),
                    taxonomy_religious_orientation=rec.get("taxonomy_religious_orientation", ""),
                    taxonomy_cultural_tradition=rec.get("taxonomy_cultural_tradition", ""),
                    taxonomy_language=rec.get("taxonomy_language", ""),
                    taxonomy_character_focus=rec.get("taxonomy_character_focus", ""),
                    taxonomy_narrative_style=rec.get("taxonomy_narrative_style", ""),
                    taxonomy_emotional_tone=rec.get("taxonomy_emotional_tone", ""),
                    taxonomy_high_level_categories=rec.get("taxonomy_high_level_categories", ""),
                    taxonomy_confidence=rec.get("taxonomy_confidence", ""),
                    taxonomy_tags=rec.get("taxonomy_tags", ""),
                    google_main_category=rec.get("google_main_category", ""),
                    google_categories=rec.get("google_categories", ""),
                    google_average_rating=_to_float(rec.get("google_average_rating", "0")),
                    google_ratings_count=_to_int(rec.get("google_ratings_count", "0")),
                    task_endpoint=rec.get("task_endpoint", ""),
                    task_group=rec.get("task_group", ""),
                    task_query=rec.get("task_query", ""),
                    page=_to_int(rec.get("page", "0")),
                )
            )
        return rows
