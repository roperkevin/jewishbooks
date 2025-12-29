from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REQUIRED_KEYS = {"id", "label", "high_level_category", "weight", "signals", "applies_to_fields"}
ALLOWED_FIELDS = {
    "title",
    "subtitle",
    "title_long",
    "description",
    "overview",
    "synopsis",
    "subjects",
    "ol_subjects",
    "loc_subjects",
    "publisher",
}


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _signal_count(sig: dict) -> int:
    total = 0
    for key in ("keywords", "phrases", "regex"):
        vals = sig.get(key) or []
        if isinstance(vals, list):
            total += len([v for v in vals if str(v).strip()])
    return total


def _validate_node(node: dict, axis: str, ids: set, errors: list, warnings: list) -> None:
    missing = REQUIRED_KEYS - set(node.keys())
    if missing:
        errors.append(f"{axis}:{node.get('id','(no id)')} missing keys: {sorted(missing)}")

    node_id = node.get("id")
    if node_id in ids:
        errors.append(f"{axis}:{node_id} duplicate id")
    elif node_id:
        ids.add(node_id)

    fields = node.get("applies_to_fields") or []
    if not isinstance(fields, list) or not fields:
        errors.append(f"{axis}:{node_id} applies_to_fields missing or not a list")
    else:
        bad = [f for f in fields if f not in ALLOWED_FIELDS]
        if bad:
            errors.append(f"{axis}:{node_id} invalid applies_to_fields: {bad}")

    for key in ("signals", "negative_signals"):
        sig = node.get(key) or {}
        if not isinstance(sig, dict):
            errors.append(f"{axis}:{node_id} {key} is not an object")
            continue
        for rx in sig.get("regex", []) or []:
            try:
                re.compile(rx)
            except re.error as e:
                errors.append(f"{axis}:{node_id} invalid regex {rx}: {e}")

    signals = node.get("signals") or {}
    if isinstance(signals, dict):
        count = _signal_count(signals)
        if count < 2:
            warnings.append(f"{axis}:{node_id} has low signal count ({count})")
    weight = node.get("weight")
    try:
        if float(weight) < 1.5:
            warnings.append(f"{axis}:{node_id} has low weight ({weight})")
    except Exception:
        errors.append(f"{axis}:{node_id} invalid weight")


def validate(path: Path) -> int:
    data = _load(path)
    errors = []
    warnings = []
    ids = set()
    for axis, nodes in data.items():
        if axis == "meta":
            continue
        if not isinstance(nodes, list):
            errors.append(f"{axis} must be a list")
            continue
        for node in nodes:
            if not isinstance(node, dict):
                errors.append(f"{axis} contains non-object node")
                continue
            _validate_node(node, axis, ids, errors, warnings)

    for w in warnings:
        print(f"[warn] {w}")
    if errors:
        for e in errors:
            print(f"[error] {e}")
        return 1
    print("[ok] taxonomy validated")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate taxonomy.json")
    ap.add_argument("path", nargs="?", default="taxonomy/taxonomy.json")
    args = ap.parse_args()
    path = Path(args.path)
    if not path.exists():
        print(f"[error] file not found: {path}")
        sys.exit(1)
    sys.exit(validate(path))


if __name__ == "__main__":
    main()
