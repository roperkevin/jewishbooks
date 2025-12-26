from isbn_harvester.normalize import build_shopify_tags


def test_build_shopify_tags_basic() -> None:
    tags = build_shopify_tags("Judaism, Jewish History", "jewish, holocaust", "Schocken")
    assert "judaism" in tags
    assert "jewish history" in tags
    assert "holocaust" in tags
    assert "schocken" in tags
