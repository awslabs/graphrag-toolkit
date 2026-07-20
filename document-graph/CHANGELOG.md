# Changelog

## 3.0.4 (2026-06-26)

### Added
- **100% docstring coverage** across all 163 source files and 141 classes
- **59 tests passing** — schema providers, transformers, ETL model, cypher edge cases, CSV discovery
- **Edge relationships** — AUTHORED_BY, CITES edge generation in hybrid pipeline
- **Lineage preservation** — source metadata structure for graphrag-toolkit (`source.sourceId` + `source.metadata`)
- **Cleanup notebook** (00-Cleanup) — remove old test tenants from Neptune

### Fixed
- `S3SchemaProvider` — removed dead `DocumentGraphConfig` references, uses `boto3.Session()` directly
- `StaticSchemaProvider` — added missing `from_config()` classmethod
- `PIIRedactorProvider` — lazy import for `sanitary` (optional dependency)
- MockStore tests — `query()` → `execute_query()` to match actual API

## 3.0.3 (2026-06-25)

### Added
- **Sphinx docs** scaffolding
- **Hybrid notebooks** — 07a (data processing), 07b (lexical-graph query + correlation)
- **Mandela demo** — 08a (build), 08b (query)
- **Multi-tenant demo** — 09 (isolation proven)
- **Schema notebook** — 005 (providers, integration, validation)
- **Transformer notebook** — 006 (all 7 categories)

### Changed
- `graphrag-lexical-graph` moved to optional dependency (avoids pip resolver conflicts)
- `push-to-sagemaker.sh` — S3 cleanup before upload, only latest wheel
- Neptune upgraded to 1.4.7.0 (graphrag-toolkit nested UNWIND compatibility)
- Batch writes enabled (Neptune 1.4.x supports it)

### Fixed
- All corrupt notebooks (ai4triage removal artifacts) — JSON repaired
- Notebook numbering consolidated: 00–09 sequential

## 3.0.0 (2026-06-24)

### Breaking Changes
- **Removed `root_id`** — use `tenant_id` only (aligned with graphrag-toolkit)
- **Removed vendored storage** — uses graphrag-toolkit as dependency
- **Removed `tenant_id.py`, `versioning.py`, `root_id.py`** — sourced from graphrag-toolkit

### Added
- `storage/` — thin extension layer (ReadOnlyGraphStore, factory extensions, complementary drivers)
- `graph_build/` — cypher_builder (node_to_cypher, edge_to_cypher, batch)
- `query/` — DocumentGraphQueryEngine
- Schema providers: CSV, JSON, S3, Static, File, Glue, Parquet, Excel, XML, YAML
- Schema discovery: auto-infer ETLSchema from data files
- 20+ transformers across 6 categories

## 2.0.0 (2026-06-24)

### Breaking Changes
- Pydantic V2 migration (all validators, ConfigDict)
- Removed visualisation, resilient_client, graph_id
- graphrag-toolkit as dependency (not vendored)
