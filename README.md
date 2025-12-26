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
