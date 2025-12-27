from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional

import logging

import yaml

logger = logging.getLogger(__name__)

from isbn_harvester.core.models import TaskSpec


def _read_tasks_file(path: Path) -> Dict[str, List[str]]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as e:
        raise SystemExit(f"Tasks file not found: {path}") from e
    except Exception as e:
        raise SystemExit(f"Failed to read tasks file: {path} ({e})") from e
    logger.info("Loaded tasks file: %s", path)
    out: Dict[str, List[str]] = {}
    for key, value in data.items():
        if isinstance(value, list):
            out[key] = [str(v) for v in value if str(v).strip()]
    return out


def _to_pairs(items: Iterable[str], group: str) -> List[tuple[str, str]]:
    return [(str(x).strip(), group) for x in items if str(x).strip()]


def _dedupe_tasks(tasks: List[TaskSpec]) -> List[TaskSpec]:
    seen = set()
    ded = []
    for t in tasks:
        k = (t.endpoint, t.query.lower().strip())
        if k in seen:
            continue
        seen.add(k)
        ded.append(t)
    return ded


def build_tasks(
    fiction_only: bool,
    groups: Optional[List[str]] = None,
    limit: Optional[int] = None,
    tasks_file: Optional[str] = None,
) -> List[TaskSpec]:
    data = {}
    if tasks_file:
        data = _read_tasks_file(Path(tasks_file))
    else:
        default_path = Path(__file__).resolve().parent.parent / "tasks.yaml"
        if default_path.exists():
            data = _read_tasks_file(default_path)
        else:
            logger.warning("No tasks file found at %s", default_path)

    publishers = _to_pairs(data.get("publishers", []), "publisher_seed")
    subjects = _to_pairs(data.get("subjects", []), "subject_seed")
    base_queries = data.get("base_queries", [])
    intent_queries = data.get("intent_queries", [])
    fiction_queries = data.get("fiction_queries", [])
    children_queries = data.get("children_queries", [])
    exclude_queries = {q.strip().lower() for q in data.get("exclude_queries", []) if str(q).strip()}

    if fiction_only:
        base_queries = base_queries + fiction_queries
        intent_queries = intent_queries + fiction_queries

    out: List[TaskSpec] = []
    for pub, g in publishers:
        out.append(TaskSpec(endpoint="publisher", query=pub, group=g))
    for sub, g in subjects:
        out.append(TaskSpec(endpoint="subject", query=sub, group=g))
    for q in base_queries:
        out.append(TaskSpec(endpoint="search", query=q, group="alpha"))
    for q in intent_queries:
        out.append(TaskSpec(endpoint="search", query=q, group="intent"))
    for q in children_queries:
        out.append(TaskSpec(endpoint="search", query=q, group="children"))

    ded = _dedupe_tasks(out)
    if exclude_queries:
        ded = [t for t in ded if t.query.lower().strip() not in exclude_queries]
    if groups:
        groups_norm = {g.strip().lower() for g in groups if g.strip()}
        ded = [t for t in ded if t.group.lower() in groups_norm]
    if limit is not None and limit > 0:
        ded = ded[:limit]
    logger.info("Built tasks: %s", len(ded))
    return ded
