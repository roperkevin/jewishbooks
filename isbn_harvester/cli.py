# isbn_harvester/cli.py
from __future__ import annotations

import argparse
import os
from typing import List, Optional

from isbn_harvester.export_full import write_full_csv
from isbn_harvester.export_shopify import write_shopify_products_csv
from .config import load_dotenv
from .covers import CoverUploader
from .harvest import harvest
from .http_client import TokenBucket, make_isbndb_session
from .store import RowStore
from .tasks import build_tasks


def _split_langs(s: str) -> Optional[List[str]]:
    parts = [x.strip() for x in (s or "").split(",") if x.strip()]
    return parts or None


def main(argv: Optional[List[str]] = None) -> None:
    used = load_dotenv(".env")
    if used:
        print(f"[info] loaded .env: {used}")
    else:
        print("[warn] .env not found via search paths; relying on existing environment variables")

    ap = argparse.ArgumentParser(
        prog="isbn_harvester",
        description="ISBNdb Jewish/Israel/Holocaust Book Harvester + Cover Uploader + Shopify CSV (metafields)",
    )

    # Outputs
    ap.add_argument("--out", default="jewish_books_full.csv", help="Full metadata CSV output (BookRow schema)")
    ap.add_argument("--shopify-out", default=None, help="Optional Shopify Products CSV output (imports to Shopify)")
    ap.add_argument("--shopify-publish", action="store_true", help="Set Shopify Status=active and Published=TRUE")

    # Resume / debug
    ap.add_argument("--checkpoint", default=None, help="NDJSON checkpoint path")
    ap.add_argument("--resume", action="store_true", help="Skip completed tasks found in checkpoint")
    ap.add_argument("--raw-jsonl", default=None, help="Optional raw JSONL dump (debug)")

    # Harvest tuning
    ap.add_argument("--max-per-task", type=int, default=2000, help="Max books processed per task")
    ap.add_argument("--page-size", type=int, default=1000, help="ISBNdb pageSize (plan-dependent; 1000 typical)")
    ap.add_argument("--concurrency", type=int, default=6, help="Task worker threads")
    ap.add_argument("--rate-per-sec", type=float, default=2.0, help="Global request rate (token bucket refill)")
    ap.add_argument("--burst", type=int, default=4, help="Token bucket burst capacity")
    ap.add_argument("--retries", type=int, default=8, help="Retry count for 429/5xx/network")
    ap.add_argument("--timeout", type=int, default=25, help="HTTP timeout seconds")
    ap.add_argument("--min-score", type=int, default=0, help="Minimum Jewish relevance score to keep")
    ap.add_argument("--langs", default="en", help="Comma list of languages to try (e.g. en,he,yi) or empty for none")

    ap.add_argument("--shuffle-tasks", action="store_true", help="Shuffle tasks for better coverage sooner")
    ap.add_argument("--start-index-jitter", type=int, default=0, help="Randomly start at later pages (spreads load)")
    ap.add_argument("--snapshot-every", type=int, default=45, help="Write partial CSV every N seconds (0 disables)")
    ap.add_argument("--fiction-only", action="store_true", help="Bias toward fiction titles")

    # Graceful stop controls
    ap.add_argument("--stop-file", default=".STOP", help="If this file exists, stop gracefully (harvest and covers)")
    ap.add_argument("--max-seconds", type=int, default=0, help="Max runtime seconds for harvesting (0 = no limit)")

    # Covers
    ap.add_argument("--covers", action="store_true", help="Enrich covers via /books/{isbn13} and upload to S3")
    ap.add_argument("--max-covers", type=int, default=5000, help="Max covers to upload/reuse")
    ap.add_argument("--covers-min-rank", type=float, default=None, help="Only upload covers for rows with rank >= this")
    ap.add_argument("--covers-timeout", type=int, default=40, help="Timeout for cover detail/image fetch")
    ap.add_argument("--covers-retries", type=int, default=6, help="Retries for cover detail/image fetch")
    ap.add_argument("--covers-concurrency", type=int, default=6, help="Parallel cover workers")
    ap.add_argument("--covers-rate-per-sec", type=float, default=3.0, help="Rate limit for cover detail calls")
    ap.add_argument("--covers-burst", type=int, default=6, help="Burst for cover detail calls")
    ap.add_argument("--covers-skip-existing-s3", action="store_true", help="Reuse any existing S3 object under covers/{isbn13}/")
    ap.add_argument("--covers-prefer-original", action="store_true", help="Prefer original/high-res cover URL when available")
    ap.add_argument("--covers-max-seconds", type=int, default=0, help="Max runtime seconds for covers (0 = no limit)")

    args = ap.parse_args(argv)

    api_key = (os.getenv("ISBNDB_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("Missing ISBNDB_API_KEY (set in .env or environment).")

    affiliate_id = (os.getenv("BOOKSHOP_AFFILIATE_ID") or "").strip() or None
    langs = _split_langs(args.langs)

    sess = make_isbndb_session(api_key)
    tasks = build_tasks(fiction_only=args.fiction_only)

    print(f"Tasks: {len(tasks)} | fiction_only={args.fiction_only} | langs={langs or ['(none)']}")
    print(f"Output(full): {args.out}")
    if args.shopify_out:
        print(f"Output(shopify): {args.shopify_out} (publish={args.shopify_publish})")
    if args.checkpoint:
        print(f"Checkpoint: {args.checkpoint} (resume={args.resume})")
    if args.stop_file:
        print(f"Stop file: {args.stop_file} (create it to stop gracefully)")
    if args.max_seconds:
        print(f"Harvest max seconds: {args.max_seconds}")

    # -----------------------
    # Harvest (dict pipeline)
    # -----------------------
    rows_by_isbn13 = harvest(
        tasks=tasks,
        isbndb_session=sess,
        out_path=args.out,
        raw_jsonl=args.raw_jsonl,
        checkpoint_path=args.checkpoint,
        resume=args.resume,
        max_per_task=args.max_per_task,
        page_size=args.page_size,
        concurrency=args.concurrency,
        rate_per_sec=args.rate_per_sec,
        burst=args.burst,
        retries=args.retries,
        timeout_s=args.timeout,
        min_score=args.min_score,
        langs=langs,
        shuffle_tasks=args.shuffle_tasks,
        start_index_jitter=args.start_index_jitter,
        snapshot_every_s=args.snapshot_every,
        fiction_only=args.fiction_only,
        bookshop_affiliate_id=affiliate_id,
        stop_file=args.stop_file,
        max_seconds=args.max_seconds,
        verbose_task_errors=True,
    )

    # Convert to RowStore (covers.py expects RowStore)
    store = RowStore()
    for row in rows_by_isbn13.values():
        store.upsert(row)

    # -----------------------
    # Write outputs (pre-covers)
    # -----------------------
    rows = store.snapshot_values()
    rows.sort(key=lambda r: r.rank_score, reverse=True)

    write_full_csv(rows, args.out)
    print(f"Done: wrote {len(rows)} full rows -> {args.out}")

    if args.shopify_out:
        write_shopify_products_csv(rows, args.shopify_out, publish=args.shopify_publish)
        print(f"Done: wrote {len(rows)} Shopify rows (with metafields) -> {args.shopify_out}")

    # -----------------------
    # Covers (CoverUploader)
    # -----------------------
    uploaded = 0
    if args.covers:
        s3_bucket = (os.getenv("S3_BUCKET") or "").strip()
        aws_region = (os.getenv("AWS_REGION") or "us-west-2").strip()
        cloudfront_domain = (os.getenv("CLOUDFRONT_DOMAIN") or "").strip() or None

        if not s3_bucket:
            raise SystemExit("Covers enabled but S3_BUCKET is not set.")

        print(f"S3_BUCKET={s3_bucket} | AWS_REGION={aws_region} | CLOUDFRONT_DOMAIN={cloudfront_domain or '(none)'}")
        if args.covers_max_seconds:
            print(f"Covers max seconds: {args.covers_max_seconds}")

        cover_limiter = TokenBucket(args.covers_rate_per_sec, args.covers_burst)

        uploader = CoverUploader(
            isbndb_session=sess,
            store=store,
            max_covers=args.max_covers,
            prefer_original=args.covers_prefer_original,
            skip_existing_s3=args.covers_skip_existing_s3,
            min_rank=args.covers_min_rank,
            timeout_s=args.covers_timeout,
            retries=args.covers_retries,
            cover_concurrency=args.covers_concurrency,
            rate_limiter=cover_limiter,
            s3_bucket=s3_bucket,
            aws_region=aws_region,
            cloudfront_domain=cloudfront_domain,
            checkpoint_path=args.checkpoint,
            stop_file=args.stop_file,
            max_seconds=args.covers_max_seconds,
        )

        uploaded = uploader.run()

        # Rewrite outputs so CSVs include cover fields/CloudFront URL
        rows = store.snapshot_values()
        rows.sort(key=lambda r: r.rank_score, reverse=True)

        write_full_csv(rows, args.out)
        print(f"Done: rewrote {len(rows)} full rows with cover fields -> {args.out}")

        if args.shopify_out:
            write_shopify_products_csv(rows, args.shopify_out, publish=args.shopify_publish)
            print(f"Done: rewrote {len(rows)} Shopify rows with cover fields -> {args.shopify_out}")

        print(f"Covers: uploaded/reused {uploaded}.")

    # Helpful warning: missing images
    rows_final = store.snapshot_values()
    missing_covers = [r for r in rows_final if not (r.cloudfront_cover_url or r.cover_url_original or r.cover_url)]
    if missing_covers:
        print(f"\n[Warning] {len(missing_covers)} rows missing an image URL.")
        for r in missing_covers[:10]:
            print(f"  - {r.isbn13} | {r.title}")


if __name__ == "__main__":
    main()
