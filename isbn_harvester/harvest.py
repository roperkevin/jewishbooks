# isbn_harvester/harvest.py
from __future__ import annotations

import json
import logging
import os
import sys
import random
import threading
import time
from queue import Empty, Queue
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from .checkpoint import CheckpointWriter, read_completed_tasks, task_id
from .export_full import write_full_csv
from .http_client import (
    TokenBucket,
    ISBNdbError,
    ISBNdbQuotaError,
    build_task_request,
    clone_isbndb_session,
    isbndb_get,
)
from .models import BookRow, TaskSpec
from .normalize import build_shopify_tags
from .parse import parse_book
from .scoring import rank_score, jewish_relevance_score, fiction_flag, popularity_proxy
from .stats_tracker import StatsTracker
from .store import RowStore

logger = logging.getLogger(__name__)


# -----------------------------
# Checkpoint helpers
# -----------------------------
def _merge_sources(existing_sources: str, new_source: str, max_sources: int = 12) -> str:
    new_source = (new_source or "").strip()
    if not new_source:
        return existing_sources or ""
    parts = [p for p in (existing_sources or "").split("|") if p]
    if new_source in parts:
        return "|".join(parts[:max_sources])
    parts.append(new_source)
    return "|".join(parts[:max_sources])


def _completeness(r: BookRow) -> int:
    return sum(
        [
            1 if r.title else 0,
            1 if r.authors else 0,
            1 if r.subjects else 0,
            1 if r.synopsis else 0,
            1 if r.publisher else 0,
            1 if r.date_published else 0,
            1 if r.pages else 0,
            1 if r.subtitle else 0,
            1 if r.edition else 0,
            1 if r.dimensions else 0,
            1 if (r.cloudfront_cover_url or r.cover_url or r.cover_url_original) else 0,
        ]
    )


def merge_row(existing: BookRow, new: BookRow, fiction_only: bool) -> BookRow:
    """
    Merge duplicate ISBN13 rows:
      - increments seen_count
      - keeps "best" row by jewish_score then completeness then rank_score
      - merges sources + recomputes rank_score + shopify_tags
      - preserves cover fields if any row has them
    """
    seen_count = max(1, int(existing.seen_count)) + 1
    sources = _merge_sources(existing.sources, new.task_query)

    pick = existing
    if new.jewish_score > existing.jewish_score:
        pick = new
    elif new.jewish_score == existing.jewish_score:
        if _completeness(new) > _completeness(existing):
            pick = new
        elif _completeness(new) == _completeness(existing) and new.rank_score > existing.rank_score:
            pick = new

    subtitle = pick.subtitle or existing.subtitle
    edition = pick.edition or existing.edition
    dimensions = pick.dimensions or existing.dimensions
    cover_url = pick.cover_url or existing.cover_url
    cover_url_original = pick.cover_url_original or existing.cover_url_original
    s3_cover_key = pick.s3_cover_key or existing.s3_cover_key
    cloudfront_cover_url = pick.cloudfront_cover_url or existing.cloudfront_cover_url
    cover_expires_at = pick.cover_expires_at or existing.cover_expires_at

    rscore = rank_score(
        pick.jewish_score,
        pick.popularity_proxy,
        pick.fiction_flag,
        fiction_only,
        seen_count,
    )

    tags = build_shopify_tags(
        pick.subjects or existing.subjects,
        pick.matched_terms or existing.matched_terms,
        pick.publisher or existing.publisher,
    )

    # dataclasses.replace() keeps this file light, but importing replace everywhere is noisy,
    # so just construct a new BookRow with updated fields.
    return BookRow(
        isbn10=pick.isbn10,
        isbn13=pick.isbn13,
        title=pick.title,
        title_long=pick.title_long,
        subtitle=subtitle,
        edition=edition,
        dimensions=dimensions,
        authors=pick.authors,
        date_published=pick.date_published,
        publisher=pick.publisher,
        language=pick.language,
        subjects=pick.subjects,
        pages=pick.pages,
        format=pick.format,
        synopsis=pick.synopsis,
        overview=pick.overview,
        cover_url=cover_url,
        cover_url_original=cover_url_original,
        cover_expires_at=cover_expires_at,
        s3_cover_key=s3_cover_key,
        cloudfront_cover_url=cloudfront_cover_url,
        bookshop_url=pick.bookshop_url,
        bookshop_affiliate_url=pick.bookshop_affiliate_url,
        jewish_score=pick.jewish_score,
        fiction_flag=pick.fiction_flag,
        popularity_proxy=pick.popularity_proxy,
        rank_score=rscore,
        matched_terms=pick.matched_terms,
        seen_count=seen_count,
        sources=sources,
        shopify_tags=tags,
        task_endpoint=pick.task_endpoint,
        task_query=pick.task_query,
        task_group=pick.task_group,
        page=pick.page,
    )


# -----------------------------
# Harvester
# -----------------------------
class Harvester:
    def __init__(
        self,
        *,
        tasks: List[TaskSpec],
        session,
        session_factory: Optional[Callable[[], object]] = None,
        out_path: str,
        raw_jsonl: Optional[str],
        checkpoint_path: Optional[str],
        resume: bool,
        max_per_task: int,
        page_size: int,
        concurrency: int,
        rate_per_sec: float,
        burst: int,
        retries: int,
        timeout_s: int,
        min_score: int,
        langs: Optional[List[str]],
        shuffle_tasks: bool,
        start_index_jitter: int,
        snapshot_every_s: int,
        fiction_only: bool,
        bookshop_affiliate_id: Optional[str],
        bookshop_enabled: bool = True,
        stop_file: Optional[str],
        max_seconds: int,
        dry_run: bool = False,
        verbose_task_errors: bool = True,
    ) -> None:
        self.tasks = tasks
        self.session = session
        self.session_factory = session_factory or (lambda: clone_isbndb_session(session))
        self.out_path = out_path
        self.raw_jsonl = raw_jsonl
        self.checkpoint_path = checkpoint_path
        self.resume = resume

        self.max_per_task = max_per_task
        self.page_size = page_size
        self.concurrency = max(1, concurrency)
        self.retries = retries
        self.timeout_s = timeout_s
        self.min_score = min_score
        self.langs = langs or None
        self.shuffle_tasks = shuffle_tasks
        self.start_index_jitter = start_index_jitter
        self.snapshot_every_s = snapshot_every_s
        self.fiction_only = fiction_only
        self.bookshop_affiliate_id = bookshop_affiliate_id
        self.bookshop_enabled = bookshop_enabled

        self.stop_file = stop_file
        self.max_seconds = max_seconds
        self.dry_run = dry_run
        self.verbose_task_errors = verbose_task_errors

        self.limiter = TokenBucket(rate_per_sec, burst)

        self.store = RowStore()

        self.stats = StatsTracker(tasks_total=0)

        self.quota_stop = threading.Event()

        if checkpoint_path:
            os.makedirs(os.path.dirname(checkpoint_path) or ".", exist_ok=True)
        if raw_jsonl:
            os.makedirs(os.path.dirname(raw_jsonl) or ".", exist_ok=True)

        self.ck = CheckpointWriter(checkpoint_path)

        self.raw_lock = threading.Lock()
        self.raw_fh = open(raw_jsonl, "a", encoding="utf-8") if raw_jsonl else None

        self.start_ts = time.time()

    def close(self) -> None:
        try:
            if self.raw_fh:
                self.raw_fh.close()
        finally:
            self.raw_fh = None
        self.ck.close()

    def should_stop(self) -> bool:
        if self.quota_stop.is_set():
            return True
        if self.max_seconds > 0 and (time.time() - self.start_ts) >= self.max_seconds:
            return True
        if self.stop_file:
            try:
                import os

                if os.path.exists(self.stop_file):
                    return True
            except Exception:
                pass
        return False

    def ck_write(self, obj: dict) -> None:
        self.ck.write(obj)

    def snapshot_writer(self, stop_evt: threading.Event) -> None:
        last_n = 0
        while not stop_evt.is_set():
            time.sleep(max(1, int(self.snapshot_every_s)))
            if stop_evt.is_set() or self.should_stop():
                return
            try:
                rows = self.store.snapshot_values()
                if len(rows) == last_n:
                    continue
                last_n = len(rows)
                write_full_csv(rows, self.out_path)
            except Exception:
                # Snapshot is best-effort
                continue

    def run(self) -> Dict[str, BookRow]:
        completed = read_completed_tasks(self.checkpoint_path) if self.resume else set()

        work: List[TaskSpec] = []
        for t in self.tasks:
            tid = task_id(t)
            if tid in completed:
                continue
            work.append(t)

        if self.shuffle_tasks:
            random.shuffle(work)

        logger.info("Harvest start: tasks=%s concurrency=%s", len(work), self.concurrency)
        q: Queue[TaskSpec] = Queue()
        for t in work:
            q.put(t)

        self.stats.set_tasks_total(len(work))

        stop_snap = threading.Event()
        snap_thread = None
        if self.snapshot_every_s and self.snapshot_every_s > 0:
            snap_thread = threading.Thread(target=self.snapshot_writer, args=(stop_snap,), daemon=True)
            snap_thread.start()

        def worker() -> None:
            sess = self.session_factory()
            while True:
                if self.should_stop():
                    return

                try:
                    t = q.get_nowait()
                except Empty:
                    return

                tid = task_id(t)
                page = 1
                kept_local = 0
                seen_local = 0
                err_local = 0
                task_complete = False

                lang_list = self.langs or [None]

                try:
                    for lang in lang_list:
                        page = 1
                        if self.start_index_jitter > 0:
                            page = 1 + random.randint(
                                0, max(0, self.start_index_jitter // max(1, self.page_size))
                            )

                        while seen_local < self.max_per_task:
                            if self.should_stop():
                                raise RuntimeError("Stopped by stop condition (quota/max-seconds/stop-file).")

                            self.limiter.take(1.0)

                            url, params = build_task_request(
                                endpoint=t.endpoint,
                                query=t.query,
                                page=page,
                                page_size=self.page_size,
                                lang=lang,
                            )

                            try:
                                data = isbndb_get(
                                    sess,
                                    url,
                                    params=params,
                                    timeout_s=self.timeout_s,
                                    retries=self.retries,
                                )
                            except ISBNdbQuotaError as qe:
                                self.quota_stop.set()
                                self.ck_write(
                                    {
                                        "type": "quota_exhausted",
                                        "task_id": tid,
                                        "query_group": t.group,
                                        "query": t.query,
                                        "endpoint": t.endpoint,
                                        "page": page,
                                        "message": str(qe),
                                        "ts": time.time(),
                                    }
                                )
                                sys.stdout.write("\n")
                                sys.stdout.flush()
                                logger.error("Quota exhausted during %s:%s page=%s", t.endpoint, t.query, page)
                                raise
                            except ISBNdbError as e:
                                msg = str(e)
                                if t.endpoint in ("publisher", "subject") and "401" not in msg:
                                    logger.warning("Fallback to search for %s:%s due to %s", t.endpoint, t.query, msg)
                                    url, params = build_task_request(
                                        endpoint="search",
                                        query=t.query,
                                        page=page,
                                        page_size=self.page_size,
                                        lang=lang,
                                    )
                                    self.ck_write(
                                        {
                                            "type": "task_fallback",
                                            "task_id": tid,
                                            "query_group": t.group,
                                            "query": t.query,
                                            "endpoint": t.endpoint,
                                            "page": page,
                                            "ts": time.time(),
                                            "reason": msg,
                                            "fallback": "search",
                                        }
                                    )
                                    data = isbndb_get(
                                        sess,
                                        url,
                                        params=params,
                                        timeout_s=self.timeout_s,
                                        retries=self.retries,
                                    )
                                else:
                                    raise
                            except Exception:
                                raise

                            self.stats.inc_requests()

                            books = data.get("books") or []
                            if not books:
                                break

                            for b in books:
                                seen_local += 1
                                self.stats.inc_books_seen()

                                isbn13, isbn10, f = parse_book(b)
                                if not isbn13:
                                    continue

                                if not f["title"] or len(f["title"]) < 3:
                                    continue

                                js, matched = jewish_relevance_score(
                                    f["title"],
                                    f["authors"],
                                    f["subjects"],
                                    f["synopsis"],
                                    f["overview"],
                                    f["publisher"],
                                    field_weights=[2.0, 1.0, 1.5, 0.7, 0.7, 0.5],
                                )
                                if js < self.min_score:
                                    continue

                                is_fic = fiction_flag(f["subjects"], f["synopsis"], f["title"])
                                pop = popularity_proxy(
                                    f["pages"],
                                    f["date_published"],
                                    f["language"],
                                    bool(f["synopsis"]),
                                )
                                matched_terms = ", ".join(matched)
                                tags = build_shopify_tags(f["subjects"], matched_terms, f["publisher"])

                                if self.bookshop_enabled:
                                    bookshop_url = f"https://bookshop.org/books/{isbn13}"
                                    aff_url = (
                                        f"https://bookshop.org/a/{self.bookshop_affiliate_id}/{isbn13}"
                                        if self.bookshop_affiliate_id
                                        else ""
                                    )
                                else:
                                    bookshop_url = ""
                                    aff_url = ""

                                rscore = rank_score(js, pop, is_fic, self.fiction_only, seen_count=1)

                                row = BookRow(
                                    isbn10=isbn10,
                                    isbn13=isbn13,
                                    title=f["title"],
                                    title_long=f["title_long"],
                                    subtitle=f["subtitle"],
                                    edition=f["edition"],
                                    dimensions=f["dimensions"],
                                    authors=f["authors"],
                                    date_published=f["date_published"],
                                    publisher=f["publisher"],
                                    language=f["language"],
                                    subjects=f["subjects"],
                                    pages=f["pages"],
                                    format=f["format"],
                                    synopsis=f["synopsis"],
                                    overview=f["overview"],
                                    cover_url=f["cover_url"] or "",
                                    cover_url_original=f["cover_url_original"] or "",
                                    cover_expires_at=0,
                                    s3_cover_key="",
                                    cloudfront_cover_url="",
                                    bookshop_url=bookshop_url,
                                    bookshop_affiliate_url=aff_url,
                                    jewish_score=js,
                                    fiction_flag=is_fic,
                                    popularity_proxy=pop,
                                    rank_score=rscore,
                                    matched_terms=matched_terms,
                                    seen_count=1,
                                    sources=t.query[:200],
                                    shopify_tags=tags,
                                    task_endpoint=t.endpoint,
                                    task_query=t.query,
                                    task_group=t.group,
                                    page=page,
                                )

                                if self.raw_fh:
                                    with self.raw_lock:
                                        self.raw_fh.write(
                                            json.dumps(
                                                {
                                                    "type": "raw_book",
                                                    "task_endpoint": t.endpoint,
                                                    "task_group": t.group,
                                                    "task_query": t.query,
                                                    "page": page,
                                                    "isbn13": isbn13,
                                                    "isbn10": isbn10,
                                                    "jewish_score": js,
                                                    "rank_score": rscore,
                                                    "matched_terms": matched,
                                                    "book": b,
                                                },
                                                ensure_ascii=False,
                                            )
                                            + "\n"
                                        )

                                self.store.upsert(
                                    isbn13,
                                    row,
                                    merge_fn=lambda existing, new: merge_row(
                                        existing, new, fiction_only=self.fiction_only
                                    ),
                                )

                                kept_local += 1
                                self.stats.inc_kept()
                                self.stats.set_unique13(self.store.size())

                            if len(books) < self.page_size:
                                break
                            if self.dry_run:
                                break
                            page += 1
                            if seen_local >= self.max_per_task:
                                break
                    task_complete = True
                except Exception as e:
                    err_local += 1
                    self.stats.inc_errors()

                    if self.verbose_task_errors:
                        logger.error("Task error %s:%s page=%s -> %r", t.endpoint, t.query, page, e)

                    self.ck_write(
                        {
                            "type": "task_error",
                            "task_id": tid,
                            "query_group": t.group,
                            "query": t.query,
                            "endpoint": t.endpoint,
                            "page": page,
                            "error": repr(e),
                            "ts": time.time(),
                        }
                    )

                finally:
                    self.stats.inc_tasks_done()

                    self.ck_write(
                        {
                            "type": "task_done" if task_complete else "task_incomplete",
                            "task_id": tid,
                            "query_group": t.group,
                            "query": t.query,
                            "endpoint": t.endpoint,
                            "ts": time.time(),
                            "kept": kept_local,
                            "seen": seen_local,
                            "errors": err_local,
                        }
                    )
                    q.task_done()

        threads: List[threading.Thread] = []
        for _ in range(self.concurrency):
            th = threading.Thread(target=worker, daemon=True)
            threads.append(th)
            th.start()

        last = 0.0
        try:
            while any(t.is_alive() for t in threads):
                now = time.time()
                if now - last >= 1.0:
                    s = self.stats.snapshot(unique13=self.store.size())
                    rates = self.stats.snapshot_rates(unique13=self.store.size())
                    stop_note = " | STOPPING" if self.should_stop() else ""
                    print(
                        f"\rTasks {s.tasks_done}/{s.tasks_total} | "
                        f"ReqOK {s.requests_made} | Errors {s.errors} | "
                        f"Books {s.books_seen} | Kept {s.kept} | Unique13 {s.unique13} | "
                        f"Req/s {rates['requests_per_sec']:.2f} | Books/s {rates['books_per_sec']:.2f}{stop_note}",
                        end="",
                        flush=True,
                    )
                    last = now

                if self.should_stop():
                    break

                time.sleep(0.1)

            for th in threads:
                th.join(timeout=1.0)
            print()
            logger.info("Harvest complete: unique13=%s", self.store.size())
        finally:
            stop_snap.set()
            if snap_thread:
                snap_thread.join(timeout=1.0)
            self.close()

        rows = self.store.snapshot_values()
        # Always write final CSV even if empty
        write_full_csv(rows, self.out_path)
        return self.store.snapshot_dict()


# -----------------------------
# Public API (function wrapper)
# -----------------------------
def harvest(
    *,
    tasks: List[TaskSpec],
    isbndb_session,
    out_path: str,
    raw_jsonl: Optional[str],
    checkpoint_path: Optional[str],
    resume: bool,
    max_per_task: int,
    page_size: int,
    concurrency: int,
    rate_per_sec: float,
    burst: int,
    retries: int,
    timeout_s: int,
    min_score: int,
    langs: Optional[List[str]],
    shuffle_tasks: bool,
    start_index_jitter: int,
    snapshot_every_s: int,
    fiction_only: bool,
    bookshop_affiliate_id: Optional[str],
    bookshop_enabled: bool = True,
    stop_file: Optional[str],
    max_seconds: int,
    dry_run: bool = False,
    verbose_task_errors: bool = True,
) -> Dict[str, BookRow]:
    """
    Convenience wrapper for code that prefers a function call.
    """
    h = Harvester(
        tasks=tasks,
        session=isbndb_session,
        out_path=out_path,
        raw_jsonl=raw_jsonl,
        checkpoint_path=checkpoint_path,
        resume=resume,
        max_per_task=max_per_task,
        page_size=page_size,
        concurrency=concurrency,
        rate_per_sec=rate_per_sec,
        burst=burst,
        retries=retries,
        timeout_s=timeout_s,
        min_score=min_score,
        langs=langs,
        shuffle_tasks=shuffle_tasks,
        start_index_jitter=start_index_jitter,
        snapshot_every_s=snapshot_every_s,
        fiction_only=fiction_only,
        bookshop_affiliate_id=bookshop_affiliate_id,
        bookshop_enabled=bookshop_enabled,
        stop_file=stop_file,
        max_seconds=max_seconds,
        dry_run=dry_run,
        verbose_task_errors=verbose_task_errors,
    )
    return h.run()
