from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Iterable, List, Optional, Tuple

import requests

from isbn_harvester.core.models import BookRow

logger = logging.getLogger(__name__)


def _choose_cover_url(r: BookRow) -> str:
    return (r.cloudfront_cover_url or r.cover_url_original or r.cover_url or "").strip()


def _check_url(session: requests.Session, url: str, timeout_s: int) -> bool:
    try:
        r = session.head(url, timeout=timeout_s, allow_redirects=True)
        if r.status_code >= 400:
            r = session.get(url, timeout=timeout_s, stream=True)
        if r.status_code >= 400:
            return False
        ctype = (r.headers.get("Content-Type") or "").lower()
        return ctype.startswith("image/")
    except Exception:
        return False


def _verify_one(row: BookRow, timeout_s: int) -> Tuple[BookRow, bool]:
    url = _choose_cover_url(row)
    if not url:
        return row, True
    sess = requests.Session()
    ok = _check_url(sess, url, timeout_s)
    if ok:
        return row, True
    cleared = replace(
        row,
        cover_url="",
        cover_url_original="",
        cloudfront_cover_url="",
        s3_cover_key="",
        cover_expires_at=0,
    )
    return cleared, False


def verify_rows(
    rows: Iterable[BookRow],
    *,
    max_rows: int = 0,
    concurrency: int = 6,
    timeout_s: int = 10,
) -> List[BookRow]:
    rows = list(rows)
    rows.sort(key=lambda r: r.rank_score, reverse=True)
    if not rows:
        return []

    verify_count = len(rows)
    if max_rows and max_rows > 0:
        verify_count = min(max_rows, len(rows))

    verify_rows_list = rows[:verify_count]
    verified: List[Optional[BookRow]] = [None] * verify_count
    failures = 0

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        future_map = {
            ex.submit(_verify_one, row, timeout_s): i for i, row in enumerate(verify_rows_list)
        }
        for fut in as_completed(future_map):
            idx = future_map[fut]
            row, ok = fut.result()
            if not ok:
                failures += 1
                logger.warning("verify: cover failed %s", row.isbn13)
            verified[idx] = row

    logger.info("verify: checked=%s failed=%s", verify_count, failures)
    results = [r for r in verified if r is not None]
    if verify_count < len(rows):
        results.extend(rows[verify_count:])
    return results
