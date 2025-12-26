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
BOOKSHOP_AFFILIATE_ID=your_affiliate_id_optional
S3_BUCKET=your_bucket_optional
AWS_REGION=us-west-2
CLOUDFRONT_DOMAIN=your_domain_optional
```

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

Debug logging for request-level detail:

```
python -m isbn_harvester --log-level debug --dry-run --task-limit 2
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

## Outputs

- Full CSV: `jewish_books_full.csv` (includes all enriched metadata and cover fields)
- Schema sidecar: `jewish_books_full.csv.schema.json`
- Shopify CSV: `jewish_books_shopify.csv` (optional)
- Checkpoint: NDJSON events for resume/recovery (optional)

## CLI Reference

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

## Troubleshooting

Quota errors (ISBNdb):
- If you see `Daily quota ... reached`, wait for reset or use a key with remaining calls.
- Use `--dry-run` to validate setup without spending many requests.

Missing API key:
- Ensure `ISBNDB_API_KEY` is set in `.env` or your environment.

Empty output:
- Lower `--min-score` or expand `--task-groups`/`--tasks-file` to increase coverage.
