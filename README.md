# ISBN Harvester

The isbn-harvester is a data-driven project for discovering, enriching, and organizing Jewish-related books at scale.

It aggregates ISBNs from multiple sources, evaluates Jewish relevance and popularity, enriches metadata (descriptions, subjects, covers), and outputs clean, commerce-ready data for platforms like Shopify.

## Features
- Multi-strategy ISBN harvesting
- Jewish relevance scoring
- Metadata enrichment
- Cover image fetching + hosting
- Shopify-compatible CSV output

## Setup

```bash
git clone https://github.com/yourname/jewish-books.git
cd jewish-books
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Environment

Create a `.env` file in the project root with:

```
ISBNDB_API_KEY=your_key
BOOKSHOP_AFFILIATE_ID=your_affiliate_id_optional
S3_BUCKET=your_bucket_optional
AWS_REGION=us-west-2
CLOUDFRONT_DOMAIN=your_domain_optional
```

## Usage

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

Run only specific task groups:

```
python -m isbn_harvester --task-groups alpha,intent --task-limit 10
```

Override tasks via YAML:

```
python -m isbn_harvester --tasks-file isbn_harvester/tasks.yaml
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
