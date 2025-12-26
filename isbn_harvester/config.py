from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


def _strip_inline_comment(val: str) -> str:
    in_single = False
    in_double = False
    for i, ch in enumerate(val):
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if ch == "#" and not in_single and not in_double:
            return val[:i].rstrip()
    return val.rstrip()


def _parse_env_file(path: Path) -> None:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export ") :].lstrip()
            k, v = line.split("=", 1)
            k = k.strip()
            v = _strip_inline_comment(v.strip())
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                v = v[1:-1]
            if k and k not in os.environ:
                os.environ[k] = v
    except Exception:
        return


def load_dotenv(path: str = ".env") -> Optional[str]:
    """
    Loads environment variables from a .env file.

    Search order:
    1) ENV_PATH (if set)
    2) explicit `path` as provided (relative to CWD or absolute)
    3) project root (parent of the isbn_harvester package directory)
    4) current working directory

    Returns the resolved .env path used, or None if not found.
    """
    # 1) explicit override
    override = os.getenv("ENV_PATH")
    candidates: List[Path] = []
    if override:
        candidates.append(Path(override).expanduser())

    p = Path(path).expanduser()
    candidates.append(p if p.is_absolute() else (Path.cwd() / p))

    # 3) project root = parent of this file's package dir
    pkg_dir = Path(__file__).resolve().parent
    project_root = pkg_dir.parent
    candidates.append(project_root / ".env")

    # 4) CWD fallback
    candidates.append(Path.cwd() / ".env")

    seen = set()
    for c in candidates:
        try:
            c = c.resolve()
        except Exception:
            pass
        if str(c) in seen:
            continue
        seen.add(str(c))
        if c.exists() and c.is_file():
            _parse_env_file(c)
            return str(c)

    return None


@dataclass
class AppConfig:
    isbndb_api_key: str
    out_full: str
    out_shopify: Optional[str]
    shopify_publish: bool

    checkpoint_path: Optional[str]
    resume: bool
    raw_jsonl: Optional[str]

    max_per_task: int
    page_size: int
    concurrency: int
    rate_per_sec: float
    burst: int
    retries: int
    timeout_s: int
    min_score: int
    langs: Optional[List[str]]
    shuffle_tasks: bool
    start_index_jitter: int
    snapshot_every_s: int
    fiction_only: bool
    bookshop_affiliate_id: Optional[str]

    stop_file: Optional[str]
    max_seconds: int

    covers: bool
    max_covers: int
    covers_min_rank: Optional[float]
    covers_timeout: int
    covers_retries: int
    covers_concurrency: int
    covers_rate_per_sec: float
    covers_burst: int
    covers_skip_existing_s3: bool
    covers_prefer_original: bool
    covers_max_seconds: int

    s3_bucket: Optional[str]
    aws_region: str
    cloudfront_domain: Optional[str]

    def validate(self) -> None:
        if not self.isbndb_api_key.strip():
            raise SystemExit("Missing ISBNDB_API_KEY (set in .env or environment).")

        if self.covers:
            if not (self.s3_bucket or "").strip():
                raise SystemExit("Covers enabled but S3_BUCKET is not set.")
            if self.cloudfront_domain:
                bad = self.cloudfront_domain.startswith("http://") or self.cloudfront_domain.startswith("https://")
                if bad:
                    raise SystemExit("CLOUDFRONT_DOMAIN should be a domain only (no https://).")
