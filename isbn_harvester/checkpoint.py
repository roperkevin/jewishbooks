from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Optional, Set

from .models import TaskSpec


def task_id(spec: TaskSpec) -> str:
    return hashlib.sha1(f"{spec.endpoint}|{spec.group}|{spec.query}".encode("utf-8")).hexdigest()


def read_completed_tasks(checkpoint_path: Optional[str]) -> Set[str]:
    done: Set[str] = set()
    if not checkpoint_path or not os.path.exists(checkpoint_path):
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
                    done.add(rec["task_id"])
    except Exception:
        return done
    return done


def read_completed_covers(checkpoint_path: Optional[str]) -> Set[str]:
    done: Set[str] = set()
    if not checkpoint_path or not os.path.exists(checkpoint_path):
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
                typ = rec.get("type")
                if typ in ("cover_done", "cover_uploaded", "cover_reused_existing_s3") and rec.get("isbn13"):
                    done.add(rec["isbn13"])
    except Exception:
        return done
    return done


class CheckpointWriter:
    def __init__(self, path: Optional[str]) -> None:
        self.path = path
        self._lock = threading.Lock()
        if path:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8") if path else None

    def write(self, obj: dict) -> None:
        if not self._fh:
            return
        with self._lock:
            self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            self._fh.flush()

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
