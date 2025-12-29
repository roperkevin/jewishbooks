import csv
import tempfile

from isbn_harvester.io.export_shopify import SHOPIFY_FIELDNAMES
from isbn_harvester.io.validate_shopify import validate_shopify_csv


def test_validate_shopify_csv_ok() -> None:
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=True) as tf:
        with open(tf.name, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=SHOPIFY_FIELDNAMES)
            w.writeheader()
            row = {k: "" for k in SHOPIFY_FIELDNAMES}
            row["Handle"] = "9780000000001"
            row["Title"] = "Title"
            row["Variant SKU"] = "9780000000001"
            row["Variant Barcode"] = "9780000000001"
            row["Status"] = "draft"
            row["Metafield: custom.subjects_list [list.single_line_text_field]"] = "[]"
            row["Metafield: custom.taxonomy_confidence [json]"] = "{}"
            w.writerow(row)
        errors, warnings = validate_shopify_csv(tf.name)

    assert errors == []
    assert warnings == []
