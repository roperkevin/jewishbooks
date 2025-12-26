# isbn_harvester/parse.py
from __future__ import annotations

import re
from typing import Dict, Tuple

from isbn_harvester.normalize import normalize_isbn, is_valid_isbn13, is_valid_isbn10

_HTML_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean_text(text: str, max_len: int | None = None) -> str:
    if not text:
        return ""
    t = _HTML_RE.sub(" ", str(text))
    t = _WS_RE.sub(" ", t).strip()
    if max_len and len(t) > max_len:
        return t[: max_len - 1].rstrip() + "…"
    return t


def _join_list(val) -> str:
    if not val:
        return ""
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v)
    return str(val)


def parse_book(book: Dict) -> Tuple[str, str, Dict[str, str]]:
    """
    Parse a single ISBNdb 'book' payload into normalized fields.

    Returns:
        (isbn13, isbn10, fields_dict)

    fields_dict keys:
        title, title_long, authors, date_published, publisher, language,
        subjects, pages, format, synopsis, overview,
        cover_url, cover_url_original
    """

    # --- ISBN normalization ---
    isbn13 = normalize_isbn(
        book.get("isbn13")
        or book.get("isbn_13")
        or book.get("isbn")
        or ""
    )

    isbn10 = normalize_isbn(
        book.get("isbn10")
        or book.get("isbn_10")
        or ""
    )

    if isbn13 and not is_valid_isbn13(isbn13):
        isbn13 = ""
    if isbn10 and not is_valid_isbn10(isbn10):
        isbn10 = ""

    # --- Core text fields ---
    title = _clean_text(book.get("title") or "")
    title_long = _clean_text(
        book.get("title_long")
        or book.get("titleLong")
        or title
    )

    authors = _clean_text(_join_list(book.get("authors")))
    publisher = _clean_text(book.get("publisher") or "")
    language = _clean_text(book.get("language") or "")
    subjects = _clean_text(
        _join_list(
            book.get("subjects")
            or book.get("subject")
            or book.get("categories")
        )
    )

    # --- Dates / numeric-ish ---
    date_published = _clean_text(
        book.get("date_published")
        or book.get("published_date")
        or book.get("datePublished")
        or ""
    )

    pages = str(book.get("pages") or "")

    # --- Descriptions ---
    synopsis = _clean_text(book.get("synopsis") or "", max_len=320)
    overview = _clean_text(book.get("overview") or "", max_len=320)

    # --- Format / binding ---
    book_format = _clean_text(
        book.get("format")
        or book.get("binding")
        or ""
    )

    # --- Covers (list endpoints often omit these) ---
    cover_url = (
        book.get("image")
        or book.get("cover")
        or book.get("cover_url")
        or book.get("thumbnail")
        or ""
    )

    cover_url_original = (
        book.get("image_original")
        or book.get("cover_large")
        or book.get("cover_highres")
        or ""
    )

    fields = {
        "title": title,
        "title_long": title_long,
        "authors": authors,
        "date_published": date_published,
        "publisher": publisher,
        "language": language,
        "subjects": subjects,
        "pages": pages,
        "format": book_format,
        "synopsis": synopsis,
        "overview": overview,
        "cover_url": cover_url,
        "cover_url_original": cover_url_original,
    }

    return isbn13, isbn10, fields
