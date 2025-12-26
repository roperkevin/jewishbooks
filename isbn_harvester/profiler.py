from __future__ import annotations

import json
import threading
from collections import defaultdict
from typing import Dict


class RequestProfiler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: Dict[str, int] = defaultdict(int)
        self._errors: Dict[str, int] = defaultdict(int)
        self._lat_sum: Dict[str, float] = defaultdict(float)

    def record(self, endpoint: str, elapsed_s: float, ok: bool) -> None:
        with self._lock:
            self._counts[endpoint] += 1
            self._lat_sum[endpoint] += float(elapsed_s)
            if not ok:
                self._errors[endpoint] += 1

    def summary(self) -> dict:
        with self._lock:
            out = {}
            for key in self._counts:
                count = self._counts[key]
                err = self._errors.get(key, 0)
                lat = self._lat_sum.get(key, 0.0)
                out[key] = {
                    "requests": count,
                    "errors": err,
                    "error_rate": (err / count) if count else 0.0,
                    "avg_latency_s": (lat / count) if count else 0.0,
                }
            return out

    def write(self, path: str) -> None:
        payload = {"endpoints": self.summary()}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
