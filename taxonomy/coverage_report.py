from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from isbn_harvester.enrich.taxonomy_assign import apply_taxonomy
from isbn_harvester.io.export_full import read_full_csv


def _load_ids(s: str) -> list[str]:
    if not s:
        return []
    try:
        val = json.loads(s)
        if isinstance(val, list):
            return [str(v) for v in val if str(v).strip()]
    except Exception:
        return []
    return []


def main() -> None:
    ap = argparse.ArgumentParser(description="Taxonomy coverage report")
    ap.add_argument("--taxonomy", default="taxonomy/taxonomy.json")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()

    rows = read_full_csv(args.csv)
    if args.max_rows and args.max_rows > 0:
        rows = rows[: args.max_rows]

    rows = apply_taxonomy(rows, args.taxonomy)

    counters = {
        "content_type": Counter(),
        "primary_genre": Counter(),
        "jewish_themes": Counter(),
        "geography": Counter(),
        "historical_era": Counter(),
        "religious_orientation": Counter(),
        "cultural_tradition": Counter(),
        "language": Counter(),
        "character_focus": Counter(),
        "narrative_style": Counter(),
        "emotional_tone": Counter(),
        "high_level_categories": Counter(),
    }

    for r in rows:
        if r.taxonomy_content_type:
            counters["content_type"][r.taxonomy_content_type] += 1
        if r.taxonomy_primary_genre:
            counters["primary_genre"][r.taxonomy_primary_genre] += 1
        for key, field in [
            ("jewish_themes", r.taxonomy_jewish_themes),
            ("geography", r.taxonomy_geography),
            ("historical_era", r.taxonomy_historical_era),
            ("religious_orientation", r.taxonomy_religious_orientation),
            ("cultural_tradition", r.taxonomy_cultural_tradition),
            ("language", r.taxonomy_language),
            ("character_focus", r.taxonomy_character_focus),
            ("narrative_style", r.taxonomy_narrative_style),
            ("emotional_tone", r.taxonomy_emotional_tone),
            ("high_level_categories", r.taxonomy_high_level_categories),
        ]:
            for cid in _load_ids(field):
                counters[key][cid] += 1

    print("Taxonomy coverage report")
    for key, counter in counters.items():
        print(f"\n[{key}]")
        for cid, n in counter.most_common(15):
            print(f"  {cid}: {n}")


if __name__ == "__main__":
    main()
