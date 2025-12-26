from __future__ import annotations

import csv
import json
import os
import re
import tempfile
from typing import Iterable, List, Set

from .models import BookRow
from .normalize import html_escape_text


def subjects_to_list_json(subjects: str, max_items: int = 50, max_item_len: int = 200) -> str:
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


def slugify_handle(title: str, isbn13: str) -> str:
    base = (title or "").lower().strip()
    base = re.sub(r"[-–—]", " ", base)
    base = re.sub(r"[^a-z0-9\s]+", "", base)
    base = re.sub(r"\s+", "-", base).strip("-")
    if not base:
        base = isbn13
    base = base[:70].strip("-")
    return f"{base}-{isbn13}"


def build_body_html(r: BookRow) -> str:
    parts: List[str] = []
    if r.synopsis:
        parts.append(f"<p>{html_escape_text(r.synopsis)}</p>")
    if r.overview and r.overview != r.synopsis:
        parts.append(f"<p>{html_escape_text(r.overview)}</p>")
    return "\n".join(parts)


def choose_image_url(r: BookRow) -> str:
    return (r.cloudfront_cover_url or r.cover_url_original or r.cover_url or "").strip()


def atomic_write_csv(write_fn, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    d = os.path.dirname(out_path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, newline="", encoding="utf-8") as tf:
        tmp_path = tf.name
    try:
        write_fn(tmp_path)
        os.replace(tmp_path, out_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def write_shopify_products_csv(rows: Iterable[BookRow], out_path: str, *, publish: bool = False) -> None:
    rows = list(rows)

    mf = lambda key, typ: f"Metafield: custom.{key} [{typ}]"

    fieldnames = [
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
    ]

    def _write(path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

            for r in rows:
                handle = slugify_handle(r.title, r.isbn13)
                body_html = build_body_html(r)
                vendor = r.publisher or "Unknown"
                tags = r.shopify_tags or ""
                img = choose_image_url(r)
                status = "active" if publish else "draft"

                pages_int = ""
                try:
                    pages_int = str(int(re.sub(r"\D+", "", r.pages or "") or ""))
                except Exception:
                    pages_int = ""

                w.writerow({
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
                })

    atomic_write_csv(_write, out_path)
