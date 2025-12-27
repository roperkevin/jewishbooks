from isbn_harvester.core.normalize import (
    isbn10_to_isbn13,
    is_valid_isbn10,
    is_valid_isbn13,
    normalize_isbn,
)
from isbn_harvester.core.parse import parse_book


def test_isbn_normalization_and_conversion() -> None:
    assert normalize_isbn("0-306-40615-2") == "0306406152"
    assert is_valid_isbn10("0306406152")
    assert is_valid_isbn13("9780306406157")
    assert isbn10_to_isbn13("0306406152") == "9780306406157"


def test_parse_book_converts_isbn10() -> None:
    book = {"isbn10": "0306406152", "title": "Test Book"}
    isbn13, isbn10, _ = parse_book(book)
    assert isbn10 == "0306406152"
    assert isbn13 == "9780306406157"
