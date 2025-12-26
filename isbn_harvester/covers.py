from __future__ import annotations

import hashlib
import sys
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from queue import Queue, Empty
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, quote

import requests

from .checkpoint import CheckpointWriter, read_completed_covers
from .http_client import ISBNDB_BASE_URL, TokenBucket, isbndb_get
from .models import BookRow
from .store import RowStore

try:
    import boto3
except Exception:
    boto3 = None


def isbndb_fetch_book_detail(session: requests.Session, isbn13: str, *, timeout_s: int, retries: int) -> Optional[dict]:
    url = f"{ISBNDB_BASE_URL}/books/{quote(isbn13, safe='')}"
    data = isbndb_get(session, url, params={}, timeout_s=timeout_s, retries=retries)
    if isinstance(data, dict) and isinstance(data.get("book"), dict):
        return data["book"]
    return data if isinstance(data, dict) else None


def extract_isbndb_cover_urls(book: Optional[dict]) -> Tuple[Optional[str], Optional[str]]:
    if not book:
        return None, None
    cover = book.get("image") or book.get("cover") or book.get("cover_url") or book.get("thumbnail") or None
    cover_orig = book.get("image_original") or book.get("cover_large") or book.get("cover_highres") or None
    return cover, cover_orig


def guess_ext_from_url(url: str) -> str:
    parsed = urlparse(url)
    if "." in parsed.path:
        ext = parsed.path.rsplit(".", 1)[-1].lower()
        if ext in ("jpeg", "jpg", "png", "webp", "gif"):
            return ext
    return "jpg"


def fetch_image_bytes(session: requests.Session, url: str, *, timeout_s: int, retries: int) -> Tuple[bytes, str]:
    backoff = 1.0
    for attempt in range(1, retries + 2):
        try:
            r = session.get(url, timeout=timeout_s, stream=True)
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt <= retries:
                    time.sleep(min(30.0, backoff))
                    backoff = min(30.0, backoff * 2)
                    continue

            r.raise_for_status()
            content_type = (r.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip().lower()
            body = r.content or b""

            if not content_type.startswith("image/"):
                raise RuntimeError(f"Non-image content-type: {content_type}")
            if len(body) < 2048:
                raise RuntimeError(f"Image too small ({len(body)} bytes)")

            return body, content_type
        except Exception:
            if attempt <= retries:
                time.sleep(min(30.0, backoff))
                backoff = min(30.0, backoff * 2)
                continue
            raise


def s3_key_for_isbn_and_bytes(isbn13: str, ext: str, body: bytes) -> str:
    h = hashlib.sha256(body).hexdigest()[:12]
    return f"covers/{isbn13}/{h}.{ext}"


def s3_find_existing_cover_key(s3, bucket: str, isbn13: str) -> Optional[str]:
    prefix = f"covers/{isbn13}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    contents = resp.get("Contents") or []
    if contents:
        return contents[0].get("Key")
    return None


def upload_bytes_to_s3(s3, *, bucket: str, key: str, body: bytes, content_type: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        CacheControl="public, max-age=31536000, immutable",
    )


class CoverUploader:
    def __init__(
        self,
        *,
        isbndb_session: requests.Session,
        store: RowStore,
        max_covers: int,
        prefer_original: bool,
        skip_existing_s3: bool,
        min_rank: Optional[float],
        timeout_s: int,
        retries: int,
        cover_concurrency: int,
        rate_limiter: TokenBucket,
        s3_bucket: str,
        aws_region: str,
        cloudfront_domain: Optional[str],
        checkpoint_path: Optional[str],
        stop_file: Optional[str],
        max_seconds: int,
    ) -> None:
        self.isbndb_session = isbndb_session
        self.store = store
        self.max_covers = max_covers
        self.prefer_original = prefer_original
        self.skip_existing_s3 = skip_existing_s3
        self.min_rank = min_rank
        self.timeout_s = timeout_s
        self.retries = retries
        self.cover_concurrency = max(1, cover_concurrency)
        self.rate_limiter = rate_limiter
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        self.cloudfront_domain = cloudfront_domain
        self.checkpoint_path = checkpoint_path
        self.stop_file = stop_file
        self.max_seconds = max_seconds

        if boto3 is None:
            raise SystemExit("boto3 is required for --covers. Install: pip install boto3")

        self.s3 = boto3.client("s3", region_name=aws_region)
        self._start_ts = 0.0

        self._progress_lock = threading.Lock()
        self._uploaded = 0
        self._errors = 0
        self._s3_cache: Dict[str, Optional[str]] = {}
        self._s3_cache_lock = threading.Lock()

    def should_stop(self) -> bool:
        if self.max_seconds > 0 and (time.time() - self._start_ts) >= self.max_seconds:
            return True
        if self.stop_file and __import__("os").path.exists(self.stop_file):
            return True
        return False

    def _get_existing_cover_key(self, isbn13: str) -> Optional[str]:
        with self._s3_cache_lock:
            if isbn13 in self._s3_cache:
                return self._s3_cache[isbn13]
        key = s3_find_existing_cover_key(self.s3, self.s3_bucket, isbn13)
        with self._s3_cache_lock:
            self._s3_cache[isbn13] = key
        return key

    def run(self) -> int:
        done_covers = read_completed_covers(self.checkpoint_path)

        rows = self.store.snapshot_values()
        rows.sort(key=lambda r: r.rank_score, reverse=True)

        targets: list[str] = []
        for r in rows:
            if len(targets) >= self.max_covers:
                break
            if r.isbn13 in done_covers:
                continue
            if self.min_rank is not None and r.rank_score < self.min_rank:
                continue
            if r.s3_cover_key and r.cloudfront_cover_url:
                continue
            targets.append(r.isbn13)

        q: Queue[str] = Queue()
        for isbn13 in targets:
            q.put(isbn13)

        ck = CheckpointWriter(self.checkpoint_path)
        refreshed_at = int(datetime.now(timezone.utc).timestamp())

        self._start_ts = time.time()

        def bump_uploaded(n: int = 1) -> None:
            with self._progress_lock:
                self._uploaded += int(n)

        def bump_error(n: int = 1) -> None:
            with self._progress_lock:
                self._errors += int(n)

        def progress_snapshot() -> tuple[int, int]:
            with self._progress_lock:
                return self._uploaded, self._errors

        def cover_worker() -> None:
            img_sess = requests.Session()

            while True:
                if self.should_stop():
                    return
                try:
                    isbn13 = q.get_nowait()
                except Empty:
                    return

                try:
                    row = self.store.get(isbn13)
                    if not row:
                        q.task_done()
                        continue

                    if self.skip_existing_s3:
                        existing_key = self._get_existing_cover_key(isbn13)
                        if existing_key:
                            cf_url = f"https://{self.cloudfront_domain}/{existing_key}" if self.cloudfront_domain else ""

                            def _reuse(cur: BookRow) -> BookRow:
                                return replace(
                                    cur,
                                    s3_cover_key=existing_key,
                                    cover_expires_at=refreshed_at,
                                    cloudfront_cover_url=cf_url,
                                )

                            updated = self.store.update_if_present(isbn13, _reuse)
                            if updated:
                                bump_uploaded(1)

                            ck.write({"type": "cover_reused_existing_s3", "isbn13": isbn13, "s3_key": existing_key, "ts": time.time()})
                            ck.write({"type": "cover_done", "isbn13": isbn13, "ts": time.time(), "mode": "reuse_existing_s3"})
                            q.task_done()
                            continue

                    self.rate_limiter.take(1.0)
                    detail = isbndb_fetch_book_detail(self.isbndb_session, isbn13, timeout_s=self.timeout_s, retries=self.retries)
                    cover, cover_orig = extract_isbndb_cover_urls(detail)

                    if not cover and not cover_orig:
                        ck.write({"type": "cover_done", "isbn13": isbn13, "ts": time.time(), "mode": "no_cover"})
                        q.task_done()
                        continue

                    chosen = (cover_orig if self.prefer_original and cover_orig else cover) or cover_orig
                    if not chosen:
                        ck.write({"type": "cover_done", "isbn13": isbn13, "ts": time.time(), "mode": "no_chosen_url"})
                        q.task_done()
                        continue

                    img_bytes, content_type = fetch_image_bytes(img_sess, chosen, timeout_s=self.timeout_s, retries=self.retries)
                    ext = guess_ext_from_url(chosen)
                    key = s3_key_for_isbn_and_bytes(isbn13, ext, img_bytes)

                    upload_bytes_to_s3(self.s3, bucket=self.s3_bucket, key=key, body=img_bytes, content_type=content_type)

                    cf_url = f"https://{self.cloudfront_domain}/{key}" if self.cloudfront_domain else ""

                    def _apply(cur: BookRow) -> BookRow:
                        return replace(
                            cur,
                            s3_cover_key=key,
                            cover_url=cover or cur.cover_url,
                            cover_url_original=cover_orig or cur.cover_url_original,
                            cover_expires_at=refreshed_at,
                            cloudfront_cover_url=cf_url,
                        )

                    updated = self.store.update_if_present(isbn13, _apply)
                    if updated:
                        bump_uploaded(1)

                    ck.write({"type": "cover_uploaded", "isbn13": isbn13, "s3_key": key, "ts": time.time()})
                    ck.write({"type": "cover_done", "isbn13": isbn13, "ts": time.time(), "mode": "uploaded"})

                except Exception as e:
                    bump_error(1)
                    ck.write({"type": "cover_error", "isbn13": isbn13, "error": repr(e), "ts": time.time()})
                finally:
                    q.task_done()

        threads = [threading.Thread(target=cover_worker, daemon=True) for _ in range(self.cover_concurrency)]
        for t in threads:
            t.start()

        last = time.time()
        while any(t.is_alive() for t in threads):
            now = time.time()
            if now - last >= 1.0:
                done_n, err_n = progress_snapshot()
                remaining = q.qsize()
                stop_note = " | STOPPING" if self.should_stop() else ""
                sys.stdout.write(f"\rCovers {done_n}/{len(targets)} | errors {err_n} | remaining {remaining}{stop_note}")
                sys.stdout.flush()
                last = now
            if self.should_stop():
                break
            time.sleep(0.1)

        for t in threads:
            t.join(timeout=1.0)

        ck.close()
        print()
        done_n, _ = progress_snapshot()
        return done_n
