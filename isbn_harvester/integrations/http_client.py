from __future__ import annotations

import json
import logging
import random
import threading
import time
from typing import Dict, Optional, Tuple
from urllib.parse import quote

import requests

ISBNDB_BASE_URL = "https://api2.isbndb.com"
logger = logging.getLogger(__name__)


def _safe_body_preview(resp: requests.Response, limit: int = 800) -> str:
    try:
        if "application/json" in (resp.headers.get("Content-Type") or "").lower():
            try:
                payload = resp.json()
                text = json.dumps(payload, ensure_ascii=False, indent=2)
            except Exception:
                text = resp.text or ""
        else:
            text = resp.text or ""
    except Exception:
        return "<unavailable>"
    text = text.replace("\r", " ").strip()
    if len(text) > limit:
        return text[:limit].rstrip() + "..."
    return text


def _safe_headers(resp: requests.Response) -> dict:
    try:
        return dict(resp.headers or {})
    except Exception:
        return {}


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


def make_isbndb_session(api_key: str, auth_header: str = "authorization") -> requests.Session:
    s = requests.Session()
    header_mode = (auth_header or "authorization").strip().lower()
    headers: Dict[str, str] = {}
    if header_mode in ("x-api-key", "x_api_key", "xapikey"):
        headers["X-API-Key"] = api_key
        headers["X-Api-Key"] = api_key
    else:
        headers["Authorization"] = api_key
    s.headers.update({
        **headers,
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
            logger.debug(
                "request | method=GET | url=%s | params=%s | attempt=%s/%s",
                url,
                params,
                attempt,
                retries + 1,
            )
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
                        logger.warning(
                            "retrying after %ss | status=%s | url=%s | params=%s",
                            ra,
                            r.status_code,
                            url,
                            params,
                        )
                        _sleep_jitter(float(ra), 0.5)
                    else:
                        logger.warning(
                            "retrying | status=%s | backoff=%s | url=%s | params=%s",
                            r.status_code,
                            backoff,
                            url,
                            params,
                        )
                        _sleep_jitter(backoff, 0.5)
                    backoff = min(30.0, backoff * 2)
                    continue

            # Auth / plan errors (not retryable)
            if r.status_code == 401:
                msg = ""
                if isinstance(data, dict):
                    msg = (data.get("message") or data.get("error") or "")
                if not msg:
                    msg = _safe_body_preview(r)
                if msg:
                    logger.error("auth error | status=401 | url=%s | params=%s | msg=%s", url, params, msg)
                    raise ISBNdbError(f"401 Unauthorized: {msg}")
                logger.error("auth error | status=401 | url=%s | params=%s", url, params)
                raise ISBNdbError("401 Unauthorized (check ISBNDB_API_KEY and plan access)")
            if r.status_code == 403:
                # If we didn't parse JSON above, the message might still be in text
                if isinstance(data, dict):
                    msg = (data.get("message") or data.get("error") or "")
                    if msg:
                        logger.error("auth error | status=403 | url=%s | params=%s | msg=%s", url, params, msg)
                        raise ISBNdbError(f"403 Forbidden: {msg}")
                logger.error("auth error | status=403 | url=%s | params=%s", url, params)
                raise ISBNdbError("403 Forbidden (plan/endpoint blocked or key invalid for this endpoint)")

            # Other non-2xx errors
            if r.status_code >= 400:
                logger.error(
                    "http error | status=%s | url=%s | params=%s | headers=%s | body=%s",
                    r.status_code,
                    url,
                    params,
                    _safe_headers(r),
                    _safe_body_preview(r),
                )
            r.raise_for_status()

            # Return JSON body (prefer parsed JSON if we already did it)
            if isinstance(data, dict):
                return data
            return r.json() if r.content else {}

        except ISBNdbQuotaError as e:
            # Never retry quota exhaustion; exit fast so you don't burn more calls.
            logger.error("quota exhausted | msg=%s", e)
            raise

        except (requests.RequestException, ValueError) as e:
            # Network/JSON errors: retry with backoff
            if attempt <= retries:
                logger.warning("request error | url=%s | params=%s | err=%r (retrying)", url, params, e)
                _sleep_jitter(backoff, 0.5)
                backoff = min(30.0, backoff * 2)
                continue
            raise ISBNdbError(f"Request failed: {url} params={params} error={e}") from e


def build_task_request(
    endpoint: str,
    query: str,
    page: int,
    page_size: int,
    lang: Optional[str],
    *,
    search_mode: str = "query",
) -> Tuple[str, Dict[str, str]]:
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
        if (search_mode or "").lower() == "param":
            params["q"] = query
            return f"{ISBNDB_BASE_URL}/books", params
        params["shouldMatchAll"] = "0"
        return f"{ISBNDB_BASE_URL}/books/{q}", params
    if endpoint in ("publisher", "subject"):
        return f"{ISBNDB_BASE_URL}/{endpoint}/{q}", params
    return f"{ISBNDB_BASE_URL}/books/{q}", params
