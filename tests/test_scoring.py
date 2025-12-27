from isbn_harvester.core.scoring import jewish_relevance_score, fiction_flag


def test_jewish_relevance_score_positive() -> None:
    score, matched = jewish_relevance_score("Jewish history", "Jerusalem")
    assert score > 0
    assert "jewish" in matched


def test_jewish_relevance_score_negative_terms() -> None:
    score, matched = jewish_relevance_score("Christmas stories")
    assert score < 0
    assert "christmas" in matched


def test_fiction_flag() -> None:
    assert fiction_flag("Jewish fiction", "A novel", "Test") == 1
    assert fiction_flag("History", "Nonfiction", "Test") == 0
