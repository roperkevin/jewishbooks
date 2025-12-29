from __future__ import annotations

import csv
import json
import sys
from typing import List, Tuple

from isbn_harvester.io.export_shopify import SHOPIFY_FIELDNAMES


LIST_FIELDS = {
    "Metafield: custom.subjects_list [list.single_line_text_field]",
    "Metafield: custom.google_categories [list.single_line_text_field]",
    "Metafield: custom.jewish_themes [list.single_line_text_field]",
    "Metafield: custom.geography [list.single_line_text_field]",
    "Metafield: custom.historical_era [list.single_line_text_field]",
    "Metafield: custom.religious_orientation [list.single_line_text_field]",
    "Metafield: custom.cultural_tradition [list.single_line_text_field]",
    "Metafield: custom.language_taxonomy [list.single_line_text_field]",
    "Metafield: custom.character_focus [list.single_line_text_field]",
    "Metafield: custom.narrative_style [list.single_line_text_field]",
    "Metafield: custom.emotional_tone [list.single_line_text_field]",
    "Metafield: custom.high_level_categories [list.single_line_text_field]",
}

JSON_FIELDS = {
    "Metafield: custom.taxonomy_confidence [json]",
}

REQUIRED_FIELDS = {
    "Handle",
    "Title",
    "Variant SKU",
    "Variant Barcode",
    "Status",
}


def _is_json_list(value: str) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    try:
        parsed = json.loads(raw)
    except Exception:
        return False
    return isinstance(parsed, list)


def _is_json_object(value: str) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    try:
        parsed = json.loads(raw)
    except Exception:
        return False
    return isinstance(parsed, dict)


def validate_shopify_csv(path: str) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        missing = [h for h in SHOPIFY_FIELDNAMES if h not in headers]
        if missing:
            errors.append(f"Missing headers: {', '.join(missing)}")

        for i, row in enumerate(reader, start=2):
            for req in REQUIRED_FIELDS:
                if not (row.get(req) or "").strip():
                    errors.append(f"Row {i}: missing required field {req}")
            for key in LIST_FIELDS:
                if key in row and not _is_json_list(row.get(key, "")):
                    errors.append(f"Row {i}: {key} is not a JSON list")
            for key in JSON_FIELDS:
                if key in row and not _is_json_object(row.get(key, "")):
                    errors.append(f"Row {i}: {key} is not a JSON object")
            if row.get("Handle") and len(row["Handle"]) > 255:
                warnings.append(f"Row {i}: handle length > 255")
            if row.get("Tags") and len(row["Tags"]) > 5000:
                warnings.append(f"Row {i}: tags length > 5000")

    return errors, warnings


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print("Usage: python -m isbn_harvester.io.validate_shopify path/to/shopify.csv")
        return 2
    errors, warnings = validate_shopify_csv(argv[1])
    for w in warnings:
        print(f"WARNING: {w}")
    for e in errors:
        print(f"ERROR: {e}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
