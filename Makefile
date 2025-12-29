.PHONY: test lint-shopify smoke

test:
	pytest

lint-shopify:
	python -m isbn_harvester.io.validate_shopify $(CSV)

smoke:
	python -m isbn_harvester --dry-run --task-limit 2 --safe-defaults --run-summary runs/summary.json
