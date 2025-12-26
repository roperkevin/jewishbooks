from __future__ import annotations

import html
import re
from typing import List, Set

ISBN10_RE = re.compile(r"^\d{9}[\dX]$")
ISBN13_RE = re.compile(r"^\d{13}$")

_TAG_SPLIT = re.compile(r"[;,/|]+")
_TAG_CLEAN = re.compile(r"[^a-z0-9 _-]+")


def normalize_isbn(x: str) -> str:
    x = (x or "").strip()
    x = re.sub(r"[^0-9Xx]", "", x).upper()
    return x


def is_valid_isbn10(isbn10: str) -> bool:
    isbn10 = normalize_isbn(isbn10)
    if not ISBN10_RE.match(isbn10):
        return False
    total = 0
    for i, ch in enumerate(isbn10[:9], start=1):
        total += i * int(ch)
    check = isbn10[9]
    check_val = 10 if check == "X" else int(check)
    total += 10 * check_val
    return total % 11 == 0


def is_valid_isbn13(isbn13: str) -> bool:
    isbn13 = normalize_isbn(isbn13)
    if not ISBN13_RE.match(isbn13):
        return False
    digits = [int(c) for c in isbn13]
    s = 0
    for i in range(12):
        s += digits[i] * (1 if i % 2 == 0 else 3)
    check = (10 - (s % 10)) % 10
    return check == digits[12]


def isbn10_to_isbn13(isbn10: str) -> str:
    isbn10 = normalize_isbn(isbn10)
    if not is_valid_isbn10(isbn10):
        return ""
    core = "978" + isbn10[:9]
    digits = [int(c) for c in core]
    s = 0
    for i in range(12):
        s += digits[i] * (1 if i % 2 == 0 else 3)
    check = (10 - (s % 10)) % 10
    return f"{core}{check}"


def snip_html(text: str, max_len: int = 260) -> str:
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", str(text))
    t = re.sub(r"\s+", " ", t).strip()
    return t[:max_len] + ("…" if len(t) > max_len else "")


def html_escape_text(s: str) -> str:
    return html.escape(s or "", quote=False)


def _norm_tag(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[-–—]", "-", s)
    s = _TAG_CLEAN.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_shopify_tags(
    subjects: str,
    matched_terms: str,
    publisher: str,
    max_tags: int = 250,
    max_total_len: int = 5000,
) -> str:
    raw: List[str] = []
    if subjects:
        raw.extend([x.strip() for x in _TAG_SPLIT.split(subjects) if x.strip()])
    if matched_terms:
        raw.extend([x.strip() for x in matched_terms.split(",") if x.strip()])
    if publisher:
        raw.append(publisher.strip())

    seen: Set[str] = set()
    out: List[str] = []
    for r in raw:
        t = _norm_tag(r)
        if not t or len(t) < 2:
            continue
        if t in seen:
            continue
        seen.add(t)

        if len(t) > 255:
            t = t[:255].strip()

        candidate = (", ".join(out + [t])) if out else t
        if len(candidate) > max_total_len:
            break

        out.append(t)
        if len(out) >= max_tags:
            break

    return ", ".join(out)
