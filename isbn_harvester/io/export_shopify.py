from __future__ import annotations

import csv
import json
import logging
import re
from typing import Iterable, List, Set

from isbn_harvester.core.models import BookRow
from isbn_harvester.core.normalize import html_escape_text
from isbn_harvester.io.utils import atomic_write_csv

logger = logging.getLogger(__name__)

MAX_ROWS_WARN = 100000
MAX_ROW_CHARS_WARN = 50000


def _shopify_fieldnames() -> List[str]:
    mf = lambda key, typ: f"Metafield: custom.{key} [{typ}]"
    return [
        "Handle",
        "Title",
        "Body (HTML)",
        "Vendor",
        "Product Category",
        "Type",
        "Tags",
        "Published",
        "Option1 Name",
        "Option1 Value",
        "Variant SKU",
        "Variant Inventory Tracker",
        "Variant Inventory Qty",
        "Variant Inventory Policy",
        "Variant Fulfillment Service",
        "Variant Price",
        "Variant Requires Shipping",
        "Variant Taxable",
        "Variant Barcode",
        "Image Src",
        "Image Position",
        "SEO Title",
        "SEO Description",
        "Status",

        mf("isbn_13", "single_line_text_field"),
        mf("isbn_10", "single_line_text_field"),
        mf("authors", "single_line_text_field"),
        mf("subtitle", "single_line_text_field"),
        mf("edition", "single_line_text_field"),
        mf("dimensions", "single_line_text_field"),
        mf("publisher", "single_line_text_field"),
        mf("publish_date", "single_line_text_field"),
        mf("language", "single_line_text_field"),
        mf("pages", "number_integer"),
        mf("binding", "single_line_text_field"),
        mf("subjects_text", "multi_line_text_field"),
        mf("subjects_list", "list.single_line_text_field"),
        mf("synopsis", "multi_line_text_field"),
        mf("overview", "multi_line_text_field"),
        mf("jewish_score", "number_integer"),
        mf("rank_score", "number_decimal"),
        mf("fiction_flag", "number_integer"),
        mf("matched_terms", "single_line_text_field"),
        mf("sources", "single_line_text_field"),
        mf("cover_url", "url"),
        mf("cover_url_original", "url"),
        mf("cloudfront_cover_url", "url"),
        mf("bookshop_url", "url"),
        mf("bookshop_affiliate_url", "url"),
        mf("google_main_category", "single_line_text_field"),
        mf("google_categories", "list.single_line_text_field"),
        mf("average_rating", "number_decimal"),
        mf("ratings_count", "number_integer"),

        mf("content_type", "single_line_text_field"),
        mf("primary_genre", "single_line_text_field"),
        mf("jewish_themes", "list.single_line_text_field"),
        mf("geography", "list.single_line_text_field"),
        mf("historical_era", "list.single_line_text_field"),
        mf("religious_orientation", "list.single_line_text_field"),
        mf("cultural_tradition", "list.single_line_text_field"),
        mf("language_taxonomy", "list.single_line_text_field"),
        mf("character_focus", "list.single_line_text_field"),
        mf("narrative_style", "list.single_line_text_field"),
        mf("emotional_tone", "list.single_line_text_field"),
        mf("high_level_categories", "list.single_line_text_field"),
        mf("taxonomy_confidence", "json"),
    ]


SHOPIFY_FIELDNAMES = _shopify_fieldnames()

def subjects_to_list_json(subjects: str, max_items: int = 100, max_item_len: int = 255) -> str:
    parts = [p.strip() for p in (subjects or "").split(",") if p.strip()]
    seen: Set[str] = set()
    out: List[str] = []
    for p in parts:
        v = p.strip()
        if not v:
            continue
        k = v.lower()
        if k in seen:
            continue
        seen.add(k)
        if len(v) > max_item_len:
            v = v[:max_item_len].strip()
        out.append(v)
        if len(out) >= max_items:
            break
    return json.dumps(out, ensure_ascii=False)


def categories_to_list_json(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "[]"
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return json.dumps([str(v) for v in parsed if str(v).strip()], ensure_ascii=False)
        except Exception:
            pass
    return subjects_to_list_json(raw)


def categories_to_tag_string(value: str, prefix: str = "") -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                tags = [str(v) for v in parsed if str(v).strip()]
                if prefix:
                    tags = [f"{prefix}: {v}" for v in tags]
                return ", ".join(tags)
        except Exception:
            pass
    return f"{prefix}: {raw}" if prefix else raw

def slugify_handle(title: str, isbn13: str) -> str:
    base = (title or "").lower().strip()
    base = re.sub(r"[-–—]", " ", base)
    base = re.sub(r"[^a-z0-9\s]+", "", base)
    base = re.sub(r"\s+", "-", base).strip("-")
    if not base:
        return isbn13
    base = base[:70].strip("-")
    return f"{base}-{isbn13}"


def build_body_html(r: BookRow) -> str:
    parts: List[str] = []
    if r.subtitle:
        parts.append(f"<p><em>{html_escape_text(r.subtitle)}</em></p>")
    if r.synopsis:
        parts.append(f"<p>{html_escape_text(r.synopsis)}</p>")
    if r.overview and r.overview != r.synopsis:
        parts.append(f"<p>{html_escape_text(r.overview)}</p>")
    return "\n".join(parts)


def choose_image_url(r: BookRow) -> str:
    return (r.cloudfront_cover_url or r.cover_url_original or r.cover_url or "").strip()


def _merge_tags(*values: str) -> str:
    parts: List[str] = []
    seen = set()
    for v in values:
        for raw in (v or "").split(","):
            t = raw.strip()
            if not t:
                continue
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)
            parts.append(t)
    return ", ".join(parts)


def _limit_tags(tags: str, max_total_len: int = 5000) -> str:
    parts = [p.strip() for p in (tags or "").split(",") if p.strip()]
    out: List[str] = []
    for p in parts:
        candidate = (", ".join(out + [p])) if out else p
        if len(candidate) > max_total_len:
            break
        out.append(p)
    return ", ".join(out)


def write_shopify_products_csv(rows: Iterable[BookRow], out_path: str, *, publish: bool = False) -> None:
    rows = list(rows)
    max_row_len = [0]
    max_row_handle = [""]

    mf = lambda key, typ: f"Metafield: custom.{key} [{typ}]"
    fieldnames = list(SHOPIFY_FIELDNAMES)

    def _write(path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

            for r in rows:
                handle = slugify_handle(r.title, r.isbn13)
                body_html = build_body_html(r)
                vendor = r.publisher or "Unknown"
                tags = _merge_tags(
                    r.shopify_tags or "",
                    r.taxonomy_tags or "",
                    (f"Google: {r.google_main_category}" if r.google_main_category else ""),
                    categories_to_tag_string(r.google_categories, prefix="Google"),
                )
                tags = _limit_tags(tags)
                img = choose_image_url(r)
                status = "active" if publish else "draft"

                pages_int = ""
                try:
                    pages_int = str(int(re.sub(r"\D+", "", r.pages or "") or ""))
                except Exception:
                    pages_int = ""

                row_dict = {
                    "Handle": handle,
                    "Title": r.title or r.title_long or r.isbn13,
                    "Body (HTML)": body_html,
                    "Vendor": vendor,
                    "Product Category": "",
                    "Type": "Book",
                    "Tags": tags,
                    "Published": "TRUE" if publish else "FALSE",
                    "Option1 Name": "Format",
                    "Option1 Value": (r.format or "Book"),
                    "Variant SKU": r.isbn13,
                    "Variant Inventory Tracker": "shopify",
                    "Variant Inventory Qty": 0,
                    "Variant Inventory Policy": "deny",
                    "Variant Fulfillment Service": "manual",
                    "Variant Price": "",
                    "Variant Requires Shipping": "TRUE",
                    "Variant Taxable": "TRUE",
                    "Variant Barcode": r.isbn13,
                    "Image Src": img,
                    "Image Position": 1 if img else "",
                    "SEO Title": r.title or "",
                    "SEO Description": (r.synopsis or r.overview or "")[:320],
                    "Status": status,

                    mf("isbn_13", "single_line_text_field"): r.isbn13 or "",
                    mf("isbn_10", "single_line_text_field"): r.isbn10 or "",
                    mf("authors", "single_line_text_field"): r.authors or "",
                    mf("subtitle", "single_line_text_field"): r.subtitle or "",
                    mf("edition", "single_line_text_field"): r.edition or "",
                    mf("dimensions", "single_line_text_field"): r.dimensions or "",
                    mf("publisher", "single_line_text_field"): r.publisher or "",
                    mf("publish_date", "single_line_text_field"): r.date_published or "",
                    mf("language", "single_line_text_field"): r.language or "",
                    mf("pages", "number_integer"): pages_int,
                    mf("binding", "single_line_text_field"): r.format or "",

                    mf("subjects_text", "multi_line_text_field"): r.subjects or "",
                    mf("subjects_list", "list.single_line_text_field"): subjects_to_list_json(r.subjects),

                    mf("synopsis", "multi_line_text_field"): r.synopsis or "",
                    mf("overview", "multi_line_text_field"): r.overview or "",

                    mf("jewish_score", "number_integer"): str(int(r.jewish_score)),
                    mf("rank_score", "number_decimal"): f"{float(r.rank_score):.6f}",
                    mf("fiction_flag", "number_integer"): str(int(r.fiction_flag)),

                    mf("matched_terms", "single_line_text_field"): r.matched_terms or "",
                    mf("sources", "single_line_text_field"): r.sources or "",

                    mf("cover_url", "url"): r.cover_url or "",
                    mf("cover_url_original", "url"): r.cover_url_original or "",
                    mf("cloudfront_cover_url", "url"): r.cloudfront_cover_url or "",

                    mf("bookshop_url", "url"): r.bookshop_url or "",
                    mf("bookshop_affiliate_url", "url"): r.bookshop_affiliate_url or "",
                    mf("google_main_category", "single_line_text_field"): r.google_main_category or "",
                    mf("google_categories", "list.single_line_text_field"): categories_to_list_json(r.google_categories),
                    mf("average_rating", "number_decimal"): f"{float(r.google_average_rating):.2f}" if r.google_average_rating else "",
                    mf("ratings_count", "number_integer"): str(int(r.google_ratings_count)) if r.google_ratings_count else "",

                    mf("content_type", "single_line_text_field"): r.taxonomy_content_type or "",
                    mf("primary_genre", "single_line_text_field"): r.taxonomy_primary_genre or "",
                    mf("jewish_themes", "list.single_line_text_field"): r.taxonomy_jewish_themes or "[]",
                    mf("geography", "list.single_line_text_field"): r.taxonomy_geography or "[]",
                    mf("historical_era", "list.single_line_text_field"): r.taxonomy_historical_era or "[]",
                    mf("religious_orientation", "list.single_line_text_field"): r.taxonomy_religious_orientation or "[]",
                    mf("cultural_tradition", "list.single_line_text_field"): r.taxonomy_cultural_tradition or "[]",
                    mf("language_taxonomy", "list.single_line_text_field"): r.taxonomy_language or "[]",
                    mf("character_focus", "list.single_line_text_field"): r.taxonomy_character_focus or "[]",
                    mf("narrative_style", "list.single_line_text_field"): r.taxonomy_narrative_style or "[]",
                    mf("emotional_tone", "list.single_line_text_field"): r.taxonomy_emotional_tone or "[]",
                    mf("high_level_categories", "list.single_line_text_field"): r.taxonomy_high_level_categories or "[]",
                    mf("taxonomy_confidence", "json"): r.taxonomy_confidence or "{}",
                }
                row_len = sum(len(str(v)) for v in row_dict.values() if v is not None)
                if row_len > max_row_len[0]:
                    max_row_len[0] = row_len
                    max_row_handle[0] = handle
                w.writerow(row_dict)

    atomic_write_csv(_write, out_path)
    logger.info("Wrote Shopify CSV: %s rows=%s", out_path, len(rows))
    if len(rows) > MAX_ROWS_WARN:
        logger.warning("Large Shopify CSV: rows=%s (warn threshold=%s)", len(rows), MAX_ROWS_WARN)
    if max_row_len[0] > MAX_ROW_CHARS_WARN:
        logger.warning(
            "Large Shopify row detected: handle=%s size=%s chars (warn threshold=%s)",
            max_row_handle[0] or "(unknown)",
            max_row_len[0],
            MAX_ROW_CHARS_WARN,
        )
