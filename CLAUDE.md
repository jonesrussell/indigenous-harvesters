# Indigenous Harvesters

Python CLI tools that harvest Indigenous content from public data sources and deliver it through North Cloud's pipeline to Minoo.

## Commands

```
task test           # Run all tests
task lint           # ruff + mypy
task format         # ruff format
harvest run <name>  # Run a harvester (--dry-run for preview)
harvest list        # List available harvesters
```

## Architecture

- `src/harvest/core/` — shared infrastructure (envelope builder, NC client, runner, registry)
- `src/harvest/harvesters/` — one module per data source (opd.py, fpcc.py, etc.)
- `src/harvest/config/sources.yaml` — source definitions (URL, license, taxonomy tags)
- Each harvester implements the `Harvester` protocol: `source_registration()`, `fetch()`, `transform()`
- Runner pipeline: register source in NC → fetch → transform → build envelopes → deliver to NC

## Envelope Contract

Envelopes match Minoo's `PayloadValidator` — NOT the Waaseyaa framework envelope:
- Required: `payload_id`, `version`, `source`, `snapshot_type`, `timestamp`, `entity_type`, `source_url`, `data`
- Optional: `metadata` (taxonomy tags, license/attribution)

## Boundary Rules

- Harvesters produce envelopes and deliver to NC — they never write directly to Minoo
- Taxonomy constants come from `indigenous-taxonomy` Python package — never hardcoded
- Source registration goes through NC source-manager API
- License/attribution metadata is mandatory for every envelope
