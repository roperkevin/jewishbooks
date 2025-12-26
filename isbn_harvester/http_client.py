from __future__ import annotations

import json
import random
import threading
import time
from typing import Dict, Optional, Tuple
from urllib.parse import quote

import requests

ISBNDB_BASE_URL = "https://api2.isbndb.com"


class ISBNdbError(RuntimeError):
    pass

class ISBNdbQuotaError(ISBNdbError):
    pass

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int) -> None:
        self.rate = max(0.01, float(rate_per_sec))
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.lock = threading.Lock()
        self.last = time.monotonic()

    def take(self, n: float = 1.0) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= n:
                    self.tokens -= n
                    return
                need = (n - self.tokens) / self.rate
            time.sleep(min(0.25, max(0.01, need)))


def make_isbndb_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": api_key,
        "Accept": "application/json",
        "User-Agent": "isbn-harvester/3.1",
    })
    return s


def clone_isbndb_session(session: requests.Session) -> requests.Session:
    cloned = requests.Session()
    cloned.headers.update(session.headers)
    return cloned


def _sleep_jitter(base: float, jitter: float = 0.25) -> None:
    time.sleep(max(0.0, base + random.random() * jitter))


def isbndb_get(session: requests.Session, url: str, *, params: dict, timeout_s: int, retries: int) -> dict:
    """
    Robust GET helper for ISBNdb with:
      - exponential backoff + jitter for 429/5xx/network
      - explicit 401/403 messages
      - quota-awareness: detects "Daily quota ... reached" and raises ISBNdbQuotaError immediately
    """
    backoff = 1.0
    for attempt in range(1, retries + 2):
        try:
            r = session.get(url, params=params, timeout=timeout_s)

            # Try to decode JSON early (some errors return structured JSON)
            data = None
            try:
                if r.content:
                    data = r.json()
            except Exception:
                data = None

            # Quota detection can appear on 403/429 (and sometimes other statuses)
            if isinstance(data, dict):
                msg = (data.get("message") or data.get("error") or data.get("errors") or "")
                if isinstance(msg, (list, dict)):
                    msg = json.dumps(msg, ensure_ascii=False)
                msg_s = str(msg)
                if "Daily quota" in msg_s and "reached" in msg_s:
                    raise ISBNdbQuotaError(msg_s)

            # Retryable HTTP statuses
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt <= retries:
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        _sleep_jitter(float(ra), 0.5)
                    else:
                        _sleep_jitter(backoff, 0.5)
                    backoff = min(30.0, backoff * 2)
                    continue

            # Auth / plan errors (not retryable)
            if r.status_code == 401:
                raise ISBNdbError("401 Unauthorized (check ISBNDB_API_KEY and plan access)")
            if r.status_code == 403:
                # If we didn't parse JSON above, the message might still be in text
                if isinstance(data, dict):
                    msg = (data.get("message") or data.get("error") or "")
                    if msg:
                        raise ISBNdbError(f"403 Forbidden: {msg}")
                raise ISBNdbError("403 Forbidden (plan/endpoint blocked or key invalid for this endpoint)")

            # Other non-2xx errors
            r.raise_for_status()

            # Return JSON body (prefer parsed JSON if we already did it)
            if isinstance(data, dict):
                return data
            return r.json() if r.content else {}

        except ISBNdbQuotaError:
            # Never retry quota exhaustion; exit fast so you don't burn more calls.
            raise

        except (requests.RequestException, ValueError) as e:
            # Network/JSON errors: retry with backoff
            if attempt <= retries:
                _sleep_jitter(backoff, 0.5)
                backoff = min(30.0, backoff * 2)
                continue
            raise ISBNdbError(f"Request failed: {url} params={params} error={e}") from e


def build_task_request(endpoint: str, query: str, page: int, page_size: int, lang: Optional[str]) -> Tuple[str, Dict[str, str]]:
    """
    Build request for harvesting.

    Strategy:
    - Use endpoint-specific routes when available (publisher/subject/search)
    - Fall back to /books list search for the generic case
    """
    q = quote(query, safe="")
    params: Dict[str, str] = {"page": str(page), "pageSize": str(page_size)}
    if lang:
        params["language"] = lang

    if endpoint == "search":
        params["q"] = query
        return f"{ISBNDB_BASE_URL}/books", params
    if endpoint in ("publisher", "subject"):
        return f"{ISBNDB_BASE_URL}/{endpoint}/{q}", params
    return f"{ISBNDB_BASE_URL}/books/{q}", params
