from isbn_harvester.core.models import BookRow
from isbn_harvester.enrich import verify as verify_mod


def _make_row(isbn13: str, rank_score: float) -> BookRow:
    return BookRow(
        isbn10="",
        isbn13=isbn13,
        title=f"Title {isbn13}",
        title_long="",
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
        cover_url="http://example.com/cover.jpg",
        cover_url_original="",
        cover_expires_at=0,
        s3_cover_key="",
        cloudfront_cover_url="",
        bookshop_url="",
        bookshop_affiliate_url="",
        jewish_score=0,
        fiction_flag=0,
        popularity_proxy=0.0,
        rank_score=rank_score,
        matched_terms="",
        seen_count=1,
        sources="",
        shopify_tags="",
        taxonomy_content_type="",
        taxonomy_primary_genre="",
        taxonomy_jewish_themes="",
        taxonomy_geography="",
        taxonomy_historical_era="",
        taxonomy_religious_orientation="",
        taxonomy_cultural_tradition="[]",
        taxonomy_language="[]",
        taxonomy_character_focus="",
        taxonomy_narrative_style="",
        taxonomy_emotional_tone="",
        taxonomy_high_level_categories="",
        taxonomy_confidence="",
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


def test_verify_rows_max_rows_keeps_unverified(monkeypatch) -> None:
    calls = []

    def fake_verify_one(row: BookRow, timeout_s: int):
        calls.append(row.isbn13)
        return row, True

    monkeypatch.setattr(verify_mod, "_verify_one_with_session", lambda row, url, session, timeout_s: fake_verify_one(row, timeout_s))

    rows = [
        _make_row("9780000000001", 3.0),
        _make_row("9780000000002", 2.0),
        _make_row("9780000000003", 1.0),
    ]

    out = verify_mod.verify_rows(rows, max_rows=2, concurrency=2, timeout_s=1)

    assert [r.isbn13 for r in out] == ["9780000000001", "9780000000002", "9780000000003"]
    assert set(calls) == {"9780000000001", "9780000000002"}
