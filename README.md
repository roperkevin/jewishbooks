# ISBN Harvester

Discover, enrich, and organize Jewish-related books at scale.

This project aggregates ISBNs from multiple sources, scores Jewish relevance and popularity, enriches metadata (descriptions, tags, covers), and outputs clean, commerce-ready data for platforms like Shopify.

## Highlights
- Multi-strategy ISBN harvesting (publisher, subject, search)
- Weighted Jewish relevance scoring with normalization
- Metadata enrichment (descriptions, tags, covers)
- Shopify-compatible CSV output with metafields
- Resumable runs, covers-only mode, and task overrides

## Setup

```bash
git clone https://github.com/yourname/jewish-books.git
cd jewish-books
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Note: `rich` is included for nicer console logs (automatic formatting and color).

## Environment

Create a `.env` file in the project root with:

```
ISBNDB_API_KEY=your_key
ISBNDB_AUTH_HEADER=authorization
GOOGLE_BOOKS_API_KEY=your_key_optional
BOOKSHOP_AFFILIATE_ID=your_affiliate_id_optional
S3_BUCKET=your_bucket_optional
AWS_REGION=us-west-2
CLOUDFRONT_DOMAIN=your_domain_optional
```

Tip: keep `.env` out of version control.

## Quickstart

Basic harvest:

```
python -m isbn_harvester --out jewish_books_full.csv
```

Harvest with Shopify output:

```
python -m isbn_harvester --out jewish_books_full.csv --shopify-out jewish_books_shopify.csv
```

Dry run (1 page per task):

```
python -m isbn_harvester --dry-run --task-limit 5
```

Write timestamped snapshots while harvesting:

```
python -m isbn_harvester --snapshot-every 60 --snapshot-dir runs/snapshots
```

Write a JSON run summary:

```
python -m isbn_harvester --run-summary runs/summary.json
```

This also writes a JSON schema alongside it as `runs/summary.json.schema.json`.

## Covers

Enable covers:

```
python -m isbn_harvester --covers --max-covers 1000
```

Run covers-only from an existing full CSV:

```
python -m isbn_harvester --covers-only --full-in jewish_books_full.csv --covers
```

## Examples

Harvest only publishers and subjects (no search queries):

```
python -m isbn_harvester --task-groups publisher_seed,subject_seed --task-limit 25
```

Run a quick smoke test with low rate limits:

```
python -m isbn_harvester --dry-run --rate-per-sec 1 --burst 1 --concurrency 1
```

Use conservative defaults:

```
python -m isbn_harvester --safe-defaults
```
Debug logging for request-level detail:

```
python -m isbn_harvester --log-level debug --dry-run --task-limit 2
```

Generate a summary report and dashboard:

```
python -m isbn_harvester --report report.md --dashboard dashboard.html
```

Create a top-100 sample CSV:

```
python -m isbn_harvester --sample-top 100 --sample-out jewish_books_sample.csv
```

Children's books only:

```
python -m isbn_harvester --task-groups children --task-limit 50
```

Harvest Hebrew + English content:

```
python -m isbn_harvester --langs en,he --min-score 2
```

Resume a previous run:

```
python -m isbn_harvester --checkpoint runs/harvest.ndjson --resume
```

Run covers only and reuse existing S3 objects:

```
python -m isbn_harvester --covers-only --full-in jewish_books_full.csv --covers --covers-skip-existing-s3
```

Generate Shopify CSV without Bookshop URLs:

```
python -m isbn_harvester --no-bookshop --shopify-out jewish_books_shopify.csv
```

Validate a Shopify CSV:

```
python -m isbn_harvester.io.validate_shopify jewish_books_shopify.csv
```

Metafield definitions (Shopify Admin API):

```
cat shopify/metafields.json
```

Verify cover URLs and prune dead links:

```
python -m isbn_harvester --verify --verify-timeout 8 --verify-concurrency 8
```

Profile API request latency and error rate:

```
python -m isbn_harvester --profile profile.json --dry-run --task-limit 5
```

## Tasks Configuration

Default task lists live in `isbn_harvester/tasks.yaml`. You can clone and customize it:

```
cp isbn_harvester/tasks.yaml /tmp/tasks.yaml
python -m isbn_harvester --tasks-file /tmp/tasks.yaml
```

Keys supported in the YAML:
- `publishers`
- `subjects`
- `base_queries`
- `intent_queries`
- `fiction_queries`
- `children_queries`
- `exclude_queries`

## Taxonomy Classification

Place your taxonomy file at `taxonomy/taxonomy.json` (or pass a custom path with `--taxonomy`).

Validate:

```
python taxonomy/validate_taxonomy.py taxonomy/taxonomy.json
```

Coverage report on a CSV:

```
python taxonomy/coverage_report.py --csv jewish_books_full.csv
```

Run with taxonomy enabled:

```
python -m isbn_harvester --taxonomy taxonomy/taxonomy.json --taxonomy-debug taxonomy_debug.jsonl --taxonomy-review taxonomy_review.jsonl
```

Note: taxonomy v2 can use `ol_subjects` and `loc_subjects` when external enrichment is enabled.

Skip taxonomy debug/review outputs for speed:

```
python -m isbn_harvester --taxonomy taxonomy/taxonomy.json --taxonomy-no-debug
```

Write taxonomy snapshots while classifying:

```
python -m isbn_harvester --taxonomy taxonomy/taxonomy.json --taxonomy-snapshot-every 60 --taxonomy-snapshot-dir runs/taxonomy_snaps
```

## External Enrichment

Pull additional subjects/descriptions from OpenLibrary, Google Books, and the Library of Congress to improve taxonomy matches:

```
python -m isbn_harvester --external-enrich --taxonomy taxonomy/taxonomy.json
```

External enrichment overwrites empty fields for: title, subtitle, authors, publisher, date_published, language, pages, cover_url, cover_url_original, synopsis, overview (and merges subjects).
Google Books adds: google_main_category and google_categories (stored as JSON array when available).

Optional controls:
- `--external-enrich-max 2000` (limit rows)
- `--external-enrich-all` (enrich even if fields already present)
- `--external-enrich-cache runs/enrich_cache.jsonl` (reuse cached payloads)
- `--external-enrich-no-openlibrary` (skip OpenLibrary)
- `--external-enrich-no-google` (skip Google Books)
- `--external-enrich-no-loc` (skip Library of Congress)
- `--external-enrich-no-shortcircuit` (disable early exit when enough data is found)
- `--external-enrich-debug` (verbose per-ISBN enrichment logging)
- `--external-enrich-no-ol-fallback` (disable OpenLibrary title/author fallback search)
- `--external-enrich-loc-disable-after 3` (disable LOC after N rate limits)
- `--external-enrich-no-google-fallback` (disable Google Books title/author fallback search)

Search mode controls:
- `--search-mode query` (default, uses `/books/{query}` for search)
- `--search-mode param` (uses `/books?q=` if your plan supports it)

Sample input and output:

```
book_doc = {
  "title": "The Shtetl Bride",
  "subtitle": "",
  "title_long": "The Shtetl Bride: A Novel",
  "description": "A Holocaust-era love story set in Eastern Europe...",
  "subjects": "Holocaust, Jewish fiction, Shtetl life",
  "publisher": "Schocken"
}

assigned = {
  "primary_genre": "historical_jewish_fiction",
  "jewish_themes": ["holocaust_fiction"],
  "geography": ["eastern_europe_shtetl"],
  "historical_era": ["world_war_ii"],
  "high_level_categories": ["Genre & Form", "Place & Setting"],
  "tags": [
    "Genre: Historical Jewish Fiction",
    "Theme: Holocaust Fiction",
    "Place: Eastern Europe (Shtetl Life)",
    "Era: World War II",
    "High: Genre & Form",
    "High: Place & Setting"
  ]
}
```

## Outputs

- Full CSV: `jewish_books_full.csv` (includes all enriched metadata and cover fields)
- Schema sidecar: `jewish_books_full.csv.schema.json`
- Shopify CSV: `jewish_books_shopify.csv` (optional)
- Checkpoint: NDJSON events for resume/recovery (optional)

## CLI Reference

To run linting on a Shopify CSV via Make:

```
make lint-shopify CSV=jewish_books_shopify.csv
```

Quick smoke run:

```
make smoke
```

### Outputs

| Flag | Default | Description |
| --- | --- | --- |
| `--out` | `jewish_books_full.csv` | Full metadata CSV output |
| `--shopify-out` | `None` | Shopify Products CSV output |
| `--shopify-publish` | `False` | Set Shopify Status=active and Published=TRUE |

### Resume / debug

| Flag | Default | Description |
| --- | --- | --- |
| `--checkpoint` | `None` | NDJSON checkpoint path |
| `--resume` | `False` | Skip completed tasks found in checkpoint |
| `--raw-jsonl` | `None` | Optional raw JSONL dump (debug) |

### Harvest tuning

| Flag | Default | Description |
| --- | --- | --- |
| `--max-per-task` | `2000` | Max books processed per task |
| `--page-size` | `1000` | ISBNdb pageSize |
| `--concurrency` | `6` | Task worker threads |
| `--rate-per-sec` | `2.0` | Global request rate (token bucket refill) |
| `--burst` | `4` | Token bucket burst capacity |
| `--retries` | `8` | Retry count for 429/5xx/network |
| `--timeout` | `25` | HTTP timeout seconds |
| `--min-score` | `0` | Minimum Jewish relevance score to keep |
| `--langs` | `en` | Comma list of languages to try (e.g. `en,he,yi`) or empty for none |
| `--shuffle-tasks` | `False` | Shuffle tasks for better coverage sooner |
| `--start-index-jitter` | `0` | Randomly start at later pages (spreads load) |
| `--snapshot-every` | `45` | Write partial CSV every N seconds (0 disables) |
| `--fiction-only` | `False` | Bias toward fiction titles |
| `--task-groups` | `None` | Comma list of task groups to run (e.g. `alpha,intent`) |
| `--task-limit` | `0` | Limit number of tasks (0 = no limit) |
| `--tasks-file` | `None` | YAML task config override |
| `--dry-run` | `False` | Fetch 1 page per task for a quick smoke run |
| `--no-bookshop` | `False` | Disable Bookshop URLs in output |
| `--safe-defaults` | `False` | Use conservative concurrency/rate defaults |
| `--log-level` | `info` | Log level: debug, info, warning, error |

### Graceful stop

| Flag | Default | Description |
| --- | --- | --- |
| `--stop-file` | `.STOP` | If this file exists, stop gracefully (harvest and covers) |
| `--max-seconds` | `0` | Max runtime seconds for harvesting (0 = no limit) |

### Covers

| Flag | Default | Description |
| --- | --- | --- |
| `--covers` | `False` | Enrich covers via `/books/{isbn13}` and upload to S3 |
| `--covers-only` | `False` | Run covers only (skip harvest, read full CSV) |
| `--full-in` | `None` | Existing full CSV input (used with `--covers-only`) |
| `--max-covers` | `5000` | Max covers to upload/reuse |
| `--covers-min-rank` | `None` | Only upload covers for rows with rank >= this |
| `--covers-timeout` | `40` | Timeout for cover detail/image fetch |
| `--covers-retries` | `6` | Retries for cover detail/image fetch |
| `--covers-concurrency` | `6` | Parallel cover workers |
| `--covers-rate-per-sec` | `3.0` | Rate limit for cover detail calls |
| `--covers-burst` | `6` | Burst for cover detail calls |
| `--covers-skip-existing-s3` | `False` | Reuse any existing S3 object under `covers/{isbn13}/` |
| `--covers-prefer-original` | `False` | Prefer original/high-res cover URL when available |
| `--covers-max-seconds` | `0` | Max runtime seconds for covers (0 = no limit) |

### Reporting and QA

| Flag | Default | Description |
| --- | --- | --- |
| `--report` | `None` | Write a summary report (.md or .html) |
| `--sample-top` | `0` | Write top-N rows by rank_score to sample CSV |
| `--sample-out` | `None` | Sample CSV output path (used with `--sample-top`) |
| `--dashboard` | `None` | Write an HTML dashboard for browsing results |
| `--dashboard-max` | `500` | Max rows to include in dashboard |
| `--verify` | `False` | Verify cover URLs and prune dead links |
| `--verify-timeout` | `10` | Timeout for cover verification calls |
| `--verify-concurrency` | `6` | Parallel workers for verification |
| `--verify-max` | `0` | Max rows to verify (0 = all) |
| `--verify-out` | `None` | Output CSV path after verification (default: `--out`) |
| `--profile` | `None` | Write request profiling summary (JSON) |

### Taxonomy

| Flag | Default | Description |
| --- | --- | --- |
| `--taxonomy` | `taxonomy/taxonomy.json` | Taxonomy JSON path |
| `--taxonomy-debug` | `None` | Write taxonomy debug JSONL |
| `--taxonomy-review` | `None` | Write taxonomy review queue JSONL |
| `--taxonomy-no-debug` | `False` | Skip taxonomy debug/review outputs |
| `--taxonomy-snapshot-every` | `0` | Write taxonomy snapshots every N seconds |
| `--taxonomy-snapshot-dir` | `None` | Directory for taxonomy snapshots |

## Troubleshooting

Quota errors (ISBNdb):
- If you see `Daily quota ... reached`, wait for reset or use a key with remaining calls.
- Use `--dry-run` to validate setup without spending many requests.

Missing API key:
- Ensure `ISBNDB_API_KEY` is set in `.env` or your environment.

Empty output:
- Lower `--min-score` or expand `--task-groups`/`--tasks-file` to increase coverage.
