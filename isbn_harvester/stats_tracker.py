from __future__ import annotations

import threading
from dataclasses import replace
from typing import Optional

from .models import StatsSnapshot


class StatsTracker:
    """
    Thread-safe stats collector for harvest progress.

    Rule: All mutation is done under one lock.
    Call snapshot(...) to get a consistent StatsSnapshot for printing/logging.
    """

    def __init__(self, tasks_total: int = 0) -> None:
        self._lock = threading.Lock()
        self._tasks_total = int(tasks_total)
        self._tasks_done = 0
        self._requests_made = 0
        self._errors = 0
        self._books_seen = 0
        self._kept = 0
        self._unique13 = 0

    def set_tasks_total(self, n: int) -> None:
        with self._lock:
            self._tasks_total = int(n)

    def set_unique13(self, n: int) -> None:
        with self._lock:
            self._unique13 = int(n)

    def inc_requests(self, n: int = 1) -> None:
        with self._lock:
            self._requests_made += int(n)

    def inc_errors(self, n: int = 1) -> None:
        with self._lock:
            self._errors += int(n)

    def inc_books_seen(self, n: int = 1) -> None:
        with self._lock:
            self._books_seen += int(n)

    def inc_kept(self, n: int = 1) -> None:
        with self._lock:
            self._kept += int(n)

    def inc_tasks_done(self, n: int = 1) -> None:
        with self._lock:
            self._tasks_done += int(n)

    def snapshot(self, *, unique13: Optional[int] = None) -> StatsSnapshot:
        with self._lock:
            if unique13 is not None:
                self._unique13 = int(unique13)
            return StatsSnapshot(
                tasks_total=self._tasks_total,
                tasks_done=self._tasks_done,
                requests_made=self._requests_made,
                errors=self._errors,
                books_seen=self._books_seen,
                kept=self._kept,
                unique13=self._unique13,
            )
