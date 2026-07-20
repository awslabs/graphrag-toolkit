# Document Graph Documentation

Structured data ETL for graph-enhanced GenAI applications.

```{toctree}
:maxdepth: 2
:caption: Contents

getting-started
lexical-graph-integration
pipeline
ingestors
schema-providers
transformers
graph-build
query-engine
multi-tenancy
examples
api/index
```

## Overview

Document-graph extends [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit) to handle **structured data** (CSV, Excel, JSON) alongside unstructured text in the same Neptune graph database.

| Feature | Module |
|---------|--------|
| Schema Providers | Auto-discover or define ETL schemas |
| Transformers | 20+ built-in data transformers |
| Graph Build | Cypher generation with tenant scoping |
| Query Engine | Typed node queries |
| Hybrid Search | Semantic search across both graph types |
