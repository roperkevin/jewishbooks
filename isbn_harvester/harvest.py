# isbn_harvester/harvest.py
from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .checkpoint import task_id
from .export_full import write_full_csv
from .http_client import (
    TokenBucket,
    ISBNdbError,
    ISBNdbQuotaError,
    build_task_request,
    isbndb_get,
)
from .models import BookRow, TaskSpec
from .normalize import build_shopify_tags
from .parse import parse_book
from .scoring import rank_score, jewish_relevance_score, fiction_flag, popularity_proxy


# -----------------------------
# Stats
# -----------------------------
@dataclass
class Stats:
    tasks_total: int = 0
    tasks_done: int = 0
    requests_ok: int = 0
    errors: int = 0
    books_seen: int = 0
    kept: int = 0
    unique13: int = 0


# -----------------------------
# Checkpoint helpers
# -----------------------------
def read_completed_tasks(checkpoint_path: Optional[str]) -> Set[str]:
    done: Set[str] = set()
    if not checkpoint_path:
        return done
    try:
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if rec.get("type") == "task_done" and rec.get("task_id"):
                    done.add(str(rec["task_id"]))
    except FileNotFoundError:
        return done
    except Exception:
        return done
    return done


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
        stop_file: Optional[str],
        max_seconds: int,
        verbose_task_errors: bool = True,
    ) -> None:
        self.tasks = tasks
        self.session = session
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

        self.stop_file = stop_file
        self.max_seconds = max_seconds
        self.verbose_task_errors = verbose_task_errors

        self.limiter = TokenBucket(rate_per_sec, burst)

        self.seen: Dict[str, BookRow] = {}
        self.seen_lock = threading.Lock()

        self.stats = Stats(tasks_total=0)
        self.stats_lock = threading.Lock()

        self.quota_stop = threading.Event()

        self.ck_lock = threading.Lock()
        self.ck_fh = open(checkpoint_path, "a", encoding="utf-8") if checkpoint_path else None

        self.raw_lock = threading.Lock()
        self.raw_fh = open(raw_jsonl, "a", encoding="utf-8") if raw_jsonl else None

        self.start_ts = time.time()

    def close(self) -> None:
        try:
            if self.raw_fh:
                self.raw_fh.close()
        finally:
            self.raw_fh = None
        try:
            if self.ck_fh:
                self.ck_fh.close()
        finally:
            self.ck_fh = None

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
        if not self.ck_fh:
            return
        with self.ck_lock:
            self.ck_fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            self.ck_fh.flush()

    def snapshot_writer(self, stop_evt: threading.Event) -> None:
        last_n = 0
        while not stop_evt.is_set():
            time.sleep(max(1, int(self.snapshot_every_s)))
            if stop_evt.is_set() or self.should_stop():
                return
            try:
                with self.seen_lock:
                    rows = list(self.seen.values())
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

        q: Queue[TaskSpec] = Queue()
        for t in work:
            q.put(t)

        with self.stats_lock:
            self.stats.tasks_total = len(work)

        stop_snap = threading.Event()
        snap_thread = None
        if self.snapshot_every_s and self.snapshot_every_s > 0:
            snap_thread = threading.Thread(target=self.snapshot_writer, args=(stop_snap,), daemon=True)
            snap_thread.start()

        def worker() -> None:
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
                                    self.session,
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
                                # Make it very obvious in console output
                                print(f"\n[quota] {qe}")
                                raise
                            except Exception:
                                raise

                            with self.stats_lock:
                                self.stats.requests_ok += 1

                            books = data.get("books") or []
                            if not books:
                                break

                            for b in books:
                                seen_local += 1
                                with self.stats_lock:
                                    self.stats.books_seen += 1

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

                                bookshop_url = f"https://bookshop.org/books/{isbn13}"
                                aff_url = (
                                    f"https://bookshop.org/a/{self.bookshop_affiliate_id}/{isbn13}"
                                    if self.bookshop_affiliate_id
                                    else ""
                                )

                                rscore = rank_score(js, pop, is_fic, self.fiction_only, seen_count=1)

                                row = BookRow(
                                    isbn10=isbn10,
                                    isbn13=isbn13,
                                    title=f["title"],
                                    title_long=f["title_long"],
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

                                with self.seen_lock:
                                    if isbn13 in self.seen:
                                        self.seen[isbn13] = merge_row(
                                            self.seen[isbn13], row, fiction_only=self.fiction_only
                                        )
                                    else:
                                        self.seen[isbn13] = row

                                kept_local += 1
                                with self.stats_lock:
                                    self.stats.kept += 1
                                    self.stats.unique13 = len(self.seen)

                            if len(books) < self.page_size:
                                break
                            page += 1
                            if seen_local >= self.max_per_task:
                                break

                except Exception as e:
                    err_local += 1
                    with self.stats_lock:
                        self.stats.errors += 1

                    if self.verbose_task_errors:
                        print(f"\n[task_error] {t.endpoint}:{t.query} page={page} -> {e!r}")

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
                    with self.stats_lock:
                        self.stats.tasks_done += 1

                    self.ck_write(
                        {
                            "type": "task_done",
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
                    with self.stats_lock:
                        s = self.stats
                    stop_note = " | STOPPING" if self.should_stop() else ""
                    print(
                        f"\rTasks {s.tasks_done}/{s.tasks_total} | "
                        f"ReqOK {s.requests_ok} | Errors {s.errors} | "
                        f"Books {s.books_seen} | Kept {s.kept} | Unique13 {s.unique13}{stop_note}",
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
        finally:
            stop_snap.set()
            if snap_thread:
                snap_thread.join(timeout=1.0)
            self.close()

        with self.seen_lock:
            rows = list(self.seen.values())
        # Always write final CSV even if empty
        write_full_csv(rows, self.out_path)
        return self.seen


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
    stop_file: Optional[str],
    max_seconds: int,
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
        stop_file=stop_file,
        max_seconds=max_seconds,
        verbose_task_errors=verbose_task_errors,
    )
    return h.run()
