from __future__ import annotations

import threading
from typing import Callable, Dict, List, Optional

from isbn_harvester.core.models import BookRow


class RowStore:
    """
    Thread-safe store for immutable BookRow objects.
    All updates are atomic read/modify/write operations.
    """

    def __init__(self, initial: Optional[Dict[str, BookRow]] = None) -> None:
        self._lock = threading.Lock()
        self._rows: Dict[str, BookRow] = dict(initial or {})

    def get(self, isbn13: str) -> Optional[BookRow]:
        with self._lock:
            return self._rows.get(isbn13)

    def set(self, isbn13: str, row: BookRow) -> None:
        with self._lock:
            self._rows[isbn13] = row

    def size(self) -> int:
        with self._lock:
            return len(self._rows)

    def snapshot_values(self) -> List[BookRow]:
        with self._lock:
            return list(self._rows.values())

    def snapshot_dict(self) -> Dict[str, BookRow]:
        with self._lock:
            return dict(self._rows)

    def upsert(
        self,
        isbn13: str,
        row: BookRow,
        merge_fn: Optional[Callable[[BookRow, BookRow], BookRow]] = None,
    ) -> BookRow:
        with self._lock:
            if isbn13 in self._rows:
                if merge_fn is None:
                    self._rows[isbn13] = row
                    return row
                merged = merge_fn(self._rows[isbn13], row)
                self._rows[isbn13] = merged
                return merged
            self._rows[isbn13] = row
            return row

    def get_or_set(self, isbn13: str, row_factory: Callable[[], BookRow]) -> BookRow:
        with self._lock:
            existing = self._rows.get(isbn13)
            if existing is not None:
                return existing
            row = row_factory()
            self._rows[isbn13] = row
            return row

    def update_if_present(self, isbn13: str, updater: Callable[[BookRow], BookRow]) -> Optional[BookRow]:
        with self._lock:
            cur = self._rows.get(isbn13)
            if cur is None:
                return None
            nxt = updater(cur)
            self._rows[isbn13] = nxt
            return nxt
