from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BookRow:
    isbn10: str
    isbn13: str
    title: str
    title_long: str
    authors: str
    date_published: str
    publisher: str
    language: str
    subjects: str
    pages: str
    format: str
    synopsis: str
    overview: str

    cover_url: str
    cover_url_original: str
    cover_expires_at: int
    s3_cover_key: str
    cloudfront_cover_url: str

    bookshop_url: str
    bookshop_affiliate_url: str

    jewish_score: int
    fiction_flag: int
    popularity_proxy: float
    rank_score: float
    matched_terms: str

    seen_count: int
    sources: str
    shopify_tags: str

    task_endpoint: str
    task_query: str
    task_group: str
    page: int


@dataclass(frozen=True)
class TaskSpec:
    endpoint: str  # "search" | "publisher" | "subject"
    query: str
    group: str


@dataclass(frozen=True)
class StatsSnapshot:
    tasks_total: int
    tasks_done: int
    requests_made: int
    errors: int
    books_seen: int
    kept: int
    unique13: int
