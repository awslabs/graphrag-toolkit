# PR Description: Add document-graph and codeproperty-graph packages

## Summary

Two new packages for graphrag-toolkit, following the existing namespace and packaging pattern:

- **document-graph** — Structured data ingestion (Domain Graph)
- **codeproperty-graph** — Delta-aware code analysis ingestion (Domain Graph, DRY)

Both address the feedback from the original document-graph proposal:
namespace move, tenant model fix, dependency direction, repo hygiene.

## What Changed

### New Packages

| Package | Dist Name | Namespace | Tests |
|---------|-----------|-----------|-------|
| document-graph | `graphrag-toolkit-document-graph` | `src/graphrag_toolkit/document_graph/` | 58 passing |
| codeproperty-graph | `graphrag-toolkit-codeproperty-graph` | `src/graphrag_toolkit/codeproperty_graph/` | 17 passing |

### Addressing Prior Feedback

| Feedback Item | Resolution |
|---------------|------------|
| Namespace: move under `graphrag_toolkit/` | ✅ Both packages follow byokg-rag pattern |
| Tenant model: use `TenantId.format_label()` | ✅ Optional import with standalone fallback producing identical format |
| Dependency direction: lexical optional | ✅ Base install works standalone; `[graphrag]` extra for Neptune |
| Repo hygiene: strip outputs, minimal data | ✅ No executed notebooks, sample data is minimal |

### Hardcoded Data Cleanup

A previous commit inadvertently included hardcoded values. This PR removes all of them:

| What was hardcoded | Where | Fixed to |
|--------------------|-------|----------|
| AWS account number (`705909755305`) | install.sh, push-to-sagemaker.sh, notebooks | `${AWS_ACCOUNT_ID}` / `<your-artifacts-bucket>` |
| Neptune endpoint (`obs-app-dev-graph.cluster-...`) | 11 notebooks | `<your-neptune-cluster-endpoint>` |
| OpenSearch endpoint (`1lci0mi6xy...aoss.amazonaws.com`) | 2 notebooks | `<your-opensearch-serverless-endpoint>` |
| S3 bucket ARN (`ccms-rag-extract-188967239867`) | `update-stack.sh` | `<your-s3-bucket-name>` |
| AWS profile name (`master`, `nw`) | scripts | `${AWS_PROFILE:-default}` |
| SageMaker paths (`~/SageMaker/document-graph/...`) | notebooks | Relative paths (`data/...`) |
| Python path (`/Library/Frameworks/Python...`) | push-to-sagemaker.sh | `python3` |

### New Files (by area)

**Packages:**
- `document-graph/` — src, tests, pyproject.toml, LICENSE, README, CHANGELOG
- `codeproperty-graph/` — src, tests, pyproject.toml, LICENSE, README, docs

**Documentation (docs-site):**
- `docs-site/src/content/docs/document-graph/` — 5 pages (overview, pipeline, schema, hybrid, config)
- `docs-site/src/content/docs/codeproperty-graph/` — 3 pages (overview, delta-ingestion, config)
- Updated `astro.config.mjs` sidebar
- Updated `index.mdx` landing page cards

**Examples:**
- `examples/document-graph/` — 24 notebooks + sample data + scripts
- `examples/codeproperty-graph/` — 2 notebooks + sample data

**CI:**
- `.github/workflows/document-graph-tests.yml`
- `.github/workflows/codeproperty-graph-tests.yml`

**Root:**
- Updated `README.md` (package descriptions)
- Updated `examples/README.md`
- Added `READMORE.md` (GraphRAG positioning, DRY architecture explanation)

### Deleted

- `document-graph/docs/` — old Sphinx docs (replaced by docs-site Starlight pages)
- `document-graph/examples/` — moved to monorepo top-level `examples/document-graph/`
- Broken `utility_functions.py` (referenced non-existent internal dependencies)

## Key Design Decisions

1. **document-graph is a Domain Graph** in the GraphRAG taxonomy — schema-first, typed nodes, deterministic
2. **codeproperty-graph proves DRY works** — ~200 lines adds an entire code analysis domain
3. **Tenant format verified against lexical's actual output** — `TenantId.format_label("User")` produces `` `Usertenant__` ``
4. **Plugin system added** (pluggy-based) — 4 extension points with sample plugins
5. **Full Joern CPG schema** — 20 node types, 14+ edge types, 9 languages, `from_joern()` factories
6. **No hardcoded credentials, endpoints, or account numbers** anywhere in the contribution

## Testing

```bash
# document-graph (58 tests)
cd document-graph && pip install -e ".[dev,graphrag]" && pytest tests/

# codeproperty-graph (17 tests)
cd codeproperty-graph && pip install -e ".[dev]" && pytest tests/
```

## Notebooks That Run Without AWS

The following notebooks require zero cloud dependencies (no Neptune, no credentials):

- `02-Standalone-ETL` — Transform + Cypher generation locally
- `04-Full-Pipeline-Test` — All pipeline stages
- `05a-Ingestors` — Column/row operations
- `05d-Multi-Format-Extraction` — Parquet, Excel, XML, YAML
- `06a-06e` — All transformers, normalizers, constructors, enrichers
- `08-Schema-Providers` — Discovery and validation
- `09-Transformer-Deep-Dive` — Comprehensive guide
- `09b-Plugins` — Plugin system demo
- `01-CPG-Models-and-GraphDiff` (codeproperty-graph) — Schema + delta logic

## Ownership

We plan to use and maintain both packages. document-graph is in active production use
for structured data ingestion. codeproperty-graph runs in CI/CD for code analysis.
We will respond to issues, review external PRs, and maintain compatibility with
lexical-graph releases.
