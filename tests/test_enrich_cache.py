from isbn_harvester.core.models import BookRow
from isbn_harvester.enrich import external_enrich as enrich_mod


def _make_row(isbn13: str) -> BookRow:
    return BookRow(
        isbn10="",
        isbn13=isbn13,
        title="Title",
        title_long="Title",
        subtitle="",
        edition="",
        dimensions="",
        authors="",
        date_published="",
        publisher="",
        language="",
        subjects="",
        ol_subjects="",
        loc_subjects="",
        pages="",
        format="",
        synopsis="",
        overview="",
        cover_url="",
        cover_url_original="",
        cover_expires_at=0,
        s3_cover_key="",
        cloudfront_cover_url="",
        bookshop_url="",
        bookshop_affiliate_url="",
        jewish_score=0,
        fiction_flag=0,
        popularity_proxy=0.0,
        rank_score=0.0,
        matched_terms="",
        seen_count=1,
        sources="",
        shopify_tags="",
        taxonomy_content_type="",
        taxonomy_primary_genre="",
        taxonomy_jewish_themes="[]",
        taxonomy_geography="[]",
        taxonomy_historical_era="[]",
        taxonomy_religious_orientation="[]",
        taxonomy_cultural_tradition="[]",
        taxonomy_language="[]",
        taxonomy_character_focus="[]",
        taxonomy_narrative_style="[]",
        taxonomy_emotional_tone="[]",
        taxonomy_high_level_categories="[]",
        taxonomy_confidence="{}",
        taxonomy_tags="",
        google_main_category="",
        google_categories="",
        google_average_rating=0.0,
        google_ratings_count=0,
        task_endpoint="search",
        task_query="q",
        task_group="g",
        page=1,
    )


def test_enrich_cache_stats_hits(monkeypatch, tmp_path) -> None:
    row = _make_row("9780000000001")
    cache_path = tmp_path / "cache.jsonl"
    cache_path.write_text(
        '{"isbn13":"9780000000001","combined":{},"subjects":[],"desc":""}\n',
        encoding="utf-8",
    )

    def _fail(*args, **kwargs):
        raise AssertionError("Should not call network when cache hit")

    monkeypatch.setattr(enrich_mod, "_get_openlibrary", _fail)
    monkeypatch.setattr(enrich_mod, "_get_google_books", _fail)
    monkeypatch.setattr(enrich_mod, "_get_loc", _fail)

    stats = {}
    out = enrich_mod.enrich_rows(
        [row],
        google_api_key=None,
        enable_openlibrary=True,
        enable_google_books=True,
        enable_loc=True,
        max_rows=0,
        concurrency=1,
        timeout_s=1,
        rate_per_sec=1.0,
        burst=1,
        enrich_all=True,
        cache_path=str(cache_path),
        cache_stats=stats,
    )

    assert out[0].isbn13 == "9780000000001"
    assert stats["hits"] == 1
    assert stats["misses"] == 0
