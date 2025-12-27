from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from isbn_harvester.core.models import BookRow

HTML_RE = re.compile(r"<[^>]+>")
NON_WORD_RE = re.compile(r"[^0-9a-zA-Z\s]+")
WS_RE = re.compile(r"\s+")

FIELD_WEIGHTS = {
    "title": 3.0,
    "subtitle": 2.0,
    "title_long": 2.0,
    "overview": 1.5,
    "synopsis": 1.5,
    "excerpt": 1.0,
    "subjects": 2.0,
    "publisher": 0.5,
}

AXIS_THRESHOLDS = {
    "primary_genre": 5.0,
    "jewish_themes": 4.0,
    "geography": 4.0,
    "historical_era": 4.0,
    "narrative_style": 2.5,
    "emotional_tone": 2.0,
}

AXIS_MAX = {
    "jewish_themes": 6,
    "geography": 3,
    "historical_era": 2,
}

AXIS_TAG_PREFIX = {
    "primary_genre": "Genre",
    "jewish_themes": "Theme",
    "geography": "Place",
    "historical_era": "Era",
    "religious_orientation": "Orientation",
    "character_focus": "Character",
    "narrative_style": "Narrative",
    "emotional_tone": "Tone",
    "high_level_categories": "High",
}

DEFAULT_MULTI_THRESHOLD = 4.0
DEFAULT_MULTI_MAX = 4
PRIMARY_FALLBACK_THRESHOLD = 4.5


def _clean_text(text: str) -> str:
    if not text:
        return ""
    t = HTML_RE.sub(" ", str(text))
    t = t.replace("&", " and ")
    t = t.replace("_", " ")
    t = NON_WORD_RE.sub(" ", t)
    t = WS_RE.sub(" ", t).strip().lower()
    return t


def _jsonl_write(path: Optional[str], obj: dict) -> None:
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _load_taxonomy(path: str) -> Dict[str, List[dict]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {k: list(v) for k, v in data.items() if isinstance(v, list)}


def _get_list(node: dict, key: str) -> List[str]:
    val = node.get(key) or []
    if isinstance(val, list):
        return [str(v) for v in val if str(v).strip()]
    return []


def _extract_signals(signals: dict) -> Tuple[List[str], List[str], List[str]]:
    keywords = _get_list(signals, "keywords")
    phrases = _get_list(signals, "phrases")
    regex = _get_list(signals, "regex")
    return keywords, phrases, regex


def _default_fields() -> List[str]:
    return ["title", "subtitle", "title_long", "overview", "synopsis", "excerpt", "subjects", "publisher"]


def _field_weight(field: str) -> float:
    return float(FIELD_WEIGHTS.get(field, 1.0))


def _match_keyword(term: str, text: str) -> bool:
    if not term:
        return False
    return re.search(rf"\b{re.escape(term)}\b", text) is not None


def _score_node(node: dict, doc: dict) -> Tuple[float, List[dict]]:
    signals = node.get("signals") or {}
    neg = node.get("negative_signals") or {}
    weight = float(node.get("weight") or 1.0)
    fields = node.get("applies_to_fields") or _default_fields()
    if not isinstance(fields, list) or not fields:
        fields = _default_fields()
    if not any(doc.get(field) for field in fields):
        fields = _default_fields()

    keywords, phrases, regexes = _extract_signals(signals)
    neg_keywords, neg_phrases, neg_regexes = _extract_signals(neg)

    total = 0.0
    matches: List[dict] = []

    for field in fields:
        text = doc.get(field, "")
        if not text:
            continue
        fweight = _field_weight(field)

        for phrase in phrases:
            phrase_norm = _clean_text(phrase)
            if phrase_norm and phrase_norm in text:
                points = 2.5 * fweight
                total += points
                matches.append({"field": field, "term": phrase, "type": "phrase", "points": points})

        for kw in keywords:
            kw_norm = _clean_text(kw)
            if kw_norm and _match_keyword(kw_norm, text):
                points = 1.0 * fweight
                total += points
                matches.append({"field": field, "term": kw, "type": "keyword", "points": points})

        for rx in regexes:
            try:
                if re.search(rx, text):
                    points = 4.0 * fweight
                    total += points
                    matches.append({"field": field, "term": rx, "type": "regex", "points": points})
            except re.error:
                continue

        for phrase in neg_phrases:
            phrase_norm = _clean_text(phrase)
            if phrase_norm and phrase_norm in text:
                points = -3.0 * fweight
                total += points
                matches.append({"field": field, "term": phrase, "type": "negative_phrase", "points": points})

        for kw in neg_keywords:
            kw_norm = _clean_text(kw)
            if kw_norm and _match_keyword(kw_norm, text):
                points = -2.0 * fweight
                total += points
                matches.append({"field": field, "term": kw, "type": "negative_keyword", "points": points})

        for rx in neg_regexes:
            try:
                if re.search(rx, text):
                    points = -3.0 * fweight
                    total += points
                    matches.append({"field": field, "term": rx, "type": "negative_regex", "points": points})
            except re.error:
                continue

    return total * weight, matches


def _build_doc(row: BookRow) -> dict:
    return {
        "title": _clean_text(row.title),
        "subtitle": _clean_text(row.subtitle),
        "title_long": _clean_text(row.title_long),
        "overview": _clean_text(row.overview),
        "synopsis": _clean_text(row.synopsis),
        "excerpt": "",
        "subjects": _clean_text(row.subjects),
        "publisher": _clean_text(row.publisher),
    }


def assign_taxonomy(
    row: BookRow,
    taxonomy: Dict[str, List[dict]],
    *,
    review_queue_path: Optional[str] = None,
    debug_path: Optional[str] = None,
) -> BookRow:
    doc = _build_doc(row)
    assignments: Dict[str, List[dict]] = {}
    reasons: List[str] = []

    for axis, nodes in taxonomy.items():
        scored = []
        for node in nodes:
            score, matches = _score_node(node, doc)
            if score <= 0:
                continue
            scored.append(
                {
                    "id": node.get("id", ""),
                    "label": node.get("label", ""),
                    "high_level_category": node.get("high_level_category", ""),
                    "score": round(score, 3),
                    "matches": matches,
                }
            )

        scored.sort(key=lambda x: x["score"], reverse=True)
        assignments[axis] = scored

    chosen_by_axis: Dict[str, List[dict]] = {}
    for axis, scored in assignments.items():
        threshold = AXIS_THRESHOLDS.get(axis, DEFAULT_MULTI_THRESHOLD)
        if axis == "primary_genre":
            chosen = [scored[0]] if scored and scored[0]["score"] >= threshold else []
        else:
            max_n = AXIS_MAX.get(axis, DEFAULT_MULTI_MAX)
            chosen = [s for s in scored if s["score"] >= threshold][:max_n]
        chosen_by_axis[axis] = chosen

    primary_scored = assignments.get("primary_genre", [])
    primary_best = primary_scored[0]["score"] if primary_scored else 0.0

    themes_scored = assignments.get("jewish_themes", [])
    themes_threshold = AXIS_THRESHOLDS.get("jewish_themes", 6.0)
    themes_match_count = len([s for s in themes_scored if s["score"] >= themes_threshold])

    other_assigned = any(
        chosen_by_axis.get(axis)
        for axis in chosen_by_axis
        if axis != "primary_genre"
    )
    if not chosen_by_axis.get("primary_genre") and other_assigned and primary_scored:
        if primary_scored[0]["score"] >= PRIMARY_FALLBACK_THRESHOLD:
            chosen_by_axis["primary_genre"] = [primary_scored[0]]

    assigned_ids: Dict[str, List[str]] = {
        axis: [c["id"] for c in chosen if c.get("id")]
        for axis, chosen in chosen_by_axis.items()
    }

    high_levels: List[str] = []
    for axis, chosen in chosen_by_axis.items():
        for c in chosen:
            hl = c.get("high_level_category")
            if hl and hl not in high_levels:
                high_levels.append(hl)

    if not assigned_ids.get("primary_genre"):
        reasons.append("no_primary_genre")
    if len(assigned_ids.get("jewish_themes", [])) == 0:
        reasons.append("no_jewish_themes")
    if themes_match_count > AXIS_MAX.get("jewish_themes", 6):
        reasons.append("too_many_themes")
    if primary_best and primary_best < AXIS_THRESHOLDS.get("primary_genre", 8.0):
        reasons.append("low_confidence_primary")

    taxonomy_tags = []
    for axis, ids in assigned_ids.items():
        if axis == "primary_genre" and ids:
            match = next((a for a in assignments[axis] if a["id"] == ids[0]), None)
            if match:
                taxonomy_tags.append(f"{AXIS_TAG_PREFIX.get(axis, axis.title())}: {match['label']}")
        else:
            for cid in ids:
                match = next((a for a in assignments[axis] if a["id"] == cid), None)
                if match:
                    prefix = AXIS_TAG_PREFIX.get(axis, axis.title())
                    taxonomy_tags.append(f"{prefix}: {match['label']}")

    for hl in high_levels:
        taxonomy_tags.append(f"{AXIS_TAG_PREFIX.get('high_level_categories', 'High')}: {hl}")

    confidence = {
        "assigned": {k: v for k, v in assigned_ids.items()},
        "evidence": {
            axis: [
                {"id": s["id"], "score": s["score"], "matches": s["matches"]}
                for s in assignments.get(axis, [])
                if s["id"] in assigned_ids.get(axis, [])
            ]
            for axis in assignments
        },
    }

    if reasons:
        _jsonl_write(
            review_queue_path,
            {
                "isbn13": row.isbn13,
                "title": row.title,
                "reasons": reasons,
                "assigned": assigned_ids,
            },
        )

    _jsonl_write(
        debug_path,
        {
            "isbn13": row.isbn13,
            "title": row.title,
            "assignments": assignments,
        },
    )

    return replace(
        row,
        taxonomy_primary_genre=(assigned_ids.get("primary_genre") or [""])[0],
        taxonomy_jewish_themes=json.dumps(assigned_ids.get("jewish_themes", []), ensure_ascii=False),
        taxonomy_geography=json.dumps(assigned_ids.get("geography", []), ensure_ascii=False),
        taxonomy_historical_era=json.dumps(assigned_ids.get("historical_era", []), ensure_ascii=False),
        taxonomy_religious_orientation=json.dumps(assigned_ids.get("religious_orientation", []), ensure_ascii=False),
        taxonomy_character_focus=json.dumps(assigned_ids.get("character_focus", []), ensure_ascii=False),
        taxonomy_narrative_style=json.dumps(assigned_ids.get("narrative_style", []), ensure_ascii=False),
        taxonomy_emotional_tone=json.dumps(assigned_ids.get("emotional_tone", []), ensure_ascii=False),
        taxonomy_high_level_categories=json.dumps(high_levels, ensure_ascii=False),
        taxonomy_confidence=json.dumps(confidence, ensure_ascii=False),
        taxonomy_tags=", ".join(taxonomy_tags),
    )


def apply_taxonomy(
    rows: Iterable[BookRow],
    taxonomy_path: str,
    *,
    review_queue_path: Optional[str] = None,
    debug_path: Optional[str] = None,
) -> List[BookRow]:
    taxonomy = _load_taxonomy(taxonomy_path)
    return [
        assign_taxonomy(row, taxonomy, review_queue_path=review_queue_path, debug_path=debug_path)
        for row in rows
    ]
