from __future__ import annotations

import html
from collections import Counter
from datetime import datetime
from typing import Iterable, List, Tuple

from isbn_harvester.core.models import BookRow


def _top(counter: Counter, n: int = 20) -> List[Tuple[str, int]]:
    return counter.most_common(n)


def _subjects_list(subjects: str) -> List[str]:
    parts = [p.strip() for p in (subjects or "").split(",") if p.strip()]
    return parts


def build_report_data(rows: Iterable[BookRow]) -> dict:
    rows = list(rows)
    total = len(rows)
    by_lang = Counter((r.language or "unknown").strip().lower() or "unknown" for r in rows)
    by_publisher = Counter((r.publisher or "unknown").strip() or "unknown" for r in rows)
    by_group = Counter((r.task_group or "unknown").strip() or "unknown" for r in rows)
    subjects = Counter()
    missing_covers = 0

    for r in rows:
        for s in _subjects_list(r.subjects):
            subjects[s] += 1
        if not (r.cloudfront_cover_url or r.cover_url_original or r.cover_url):
            missing_covers += 1

    return {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
        "total": total,
        "missing_covers": missing_covers,
        "top_languages": _top(by_lang),
        "top_publishers": _top(by_publisher),
        "top_subjects": _top(subjects),
        "task_groups": _top(by_group, n=50),
    }


def _md_table(rows: List[Tuple[str, int]], headers: Tuple[str, str]) -> str:
    lines = [f"| {headers[0]} | {headers[1]} |", "| --- | --- |"]
    for k, v in rows:
        lines.append(f"| {k} | {v} |")
    return "\n".join(lines)


def render_markdown(data: dict) -> str:
    out = []
    out.append("# Harvest Report")
    out.append("")
    out.append(f"Generated: {data['generated_at']}")
    out.append("")
    out.append(f"Total rows: {data['total']}")
    out.append(f"Missing covers: {data['missing_covers']}")
    out.append("")
    out.append("## Top Languages")
    out.append(_md_table(data["top_languages"], ("Language", "Count")))
    out.append("")
    out.append("## Top Publishers")
    out.append(_md_table(data["top_publishers"], ("Publisher", "Count")))
    out.append("")
    out.append("## Top Subjects")
    out.append(_md_table(data["top_subjects"], ("Subject", "Count")))
    out.append("")
    out.append("## Task Groups")
    out.append(_md_table(data["task_groups"], ("Group", "Count")))
    return "\n".join(out)


def _html_table(rows: List[Tuple[str, int]], headers: Tuple[str, str]) -> str:
    head = f"<tr><th>{html.escape(headers[0])}</th><th>{html.escape(headers[1])}</th></tr>"
    body = "".join(
        f"<tr><td>{html.escape(str(k))}</td><td>{v}</td></tr>" for k, v in rows
    )
    return f"<table>{head}{body}</table>"


def render_html(data: dict) -> str:
    sections = []
    sections.append(f"<h1>Harvest Report</h1>")
    sections.append(f"<p><strong>Generated:</strong> {html.escape(data['generated_at'])}</p>")
    sections.append(
        f"<p><strong>Total rows:</strong> {data['total']} | "
        f"<strong>Missing covers:</strong> {data['missing_covers']}</p>"
    )
    sections.append("<h2>Top Languages</h2>")
    sections.append(_html_table(data["top_languages"], ("Language", "Count")))
    sections.append("<h2>Top Publishers</h2>")
    sections.append(_html_table(data["top_publishers"], ("Publisher", "Count")))
    sections.append("<h2>Top Subjects</h2>")
    sections.append(_html_table(data["top_subjects"], ("Subject", "Count")))
    sections.append("<h2>Task Groups</h2>")
    sections.append(_html_table(data["task_groups"], ("Group", "Count")))

    return (
        "<!doctype html>"
        "<html><head><meta charset='utf-8'>"
        "<title>Harvest Report</title>"
        "<style>"
        "body{font-family:Arial, sans-serif; margin:24px; color:#111;}"
        "table{border-collapse:collapse; margin:12px 0; width:100%; max-width:900px;}"
        "th,td{border:1px solid #ddd; padding:6px 10px; text-align:left;}"
        "th{background:#f3f3f3;}"
        "</style></head><body>"
        + "".join(sections)
        + "</body></html>"
    )


def write_report(rows: Iterable[BookRow], out_path: str) -> None:
    data = build_report_data(rows)
    if out_path.lower().endswith(".html"):
        content = render_html(data)
    else:
        content = render_markdown(data)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(content)
