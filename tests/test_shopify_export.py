import csv
import tempfile

from isbn_harvester.core.models import BookRow
from isbn_harvester.io.export_shopify import write_shopify_products_csv


def _make_row(isbn13: str, title: str) -> BookRow:
    return BookRow(
        isbn10="",
        isbn13=isbn13,
        title=title,
        title_long=title,
        subtitle="",
        edition="",
        dimensions="",
        authors="Author One",
        date_published="2020",
        publisher="Pub",
        language="en",
        subjects="Judaism, Fiction",
        ol_subjects="",
        loc_subjects="",
        pages="123",
        format="Hardcover",
        synopsis="Synopsis",
        overview="Overview",
        cover_url="",
        cover_url_original="",
        cover_expires_at=0,
        s3_cover_key="",
        cloudfront_cover_url="https://cdn.example.com/covers/abc.jpg",
        bookshop_url="",
        bookshop_affiliate_url="",
        jewish_score=2,
        fiction_flag=1,
        popularity_proxy=0.5,
        rank_score=0.5,
        matched_terms="jewish",
        seen_count=1,
        sources="test",
        shopify_tags="judaism, fiction",
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
        google_categories="[]",
        google_average_rating=0.0,
        google_ratings_count=0,
        task_endpoint="search",
        task_query="q",
        task_group="g",
        page=1,
    )


def test_shopify_export_headers_and_handle() -> None:
    row = _make_row("9780000000001", "")
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tf:
        write_shopify_products_csv([row], tf.name, publish=False)
        with open(tf.name, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            out_rows = list(reader)

    assert "Handle" in headers
    assert "Title" in headers
    assert "Image Src" in headers
    assert "Metafield: custom.isbn_13 [single_line_text_field]" in headers
    assert len(out_rows) == 1
    assert out_rows[0]["Handle"] == "9780000000001"
    assert out_rows[0]["Image Src"] == "https://cdn.example.com/covers/abc.jpg"
