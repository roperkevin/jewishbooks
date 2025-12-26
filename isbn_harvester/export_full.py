from __future__ import annotations

import csv
import os
import tempfile
from dataclasses import asdict
from typing import Iterable

from .models import BookRow


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


def write_full_csv(rows: Iterable[BookRow], out_path: str) -> None:
    rows = list(rows)
    fieldnames = [
        "isbn13", "isbn10",
        "title", "title_long",
        "authors",
        "date_published",
        "publisher",
        "language",
        "subjects",
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
        "task_endpoint",
        "task_group",
        "task_query",
        "page",
    ]

    def _write(path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(asdict(r))

    atomic_write_csv(_write, out_path)
