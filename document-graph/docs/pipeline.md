# Pipeline

The document-graph pipeline processes structured data through four stages: **Extract → Ingest → Transform → Build**.

## Overview

```
Source File (CSV/JSON/Excel/Parquet)
    │
    ├── Extract: Read into DataFrame
    │
    ├── Ingest: Column select, rename, row filter
    │
    ├── Transform: Normalize, enrich, reshape
    │
    └── Build: Generate Cypher → Write to Neptune
```

## Extract

Reads a data file into a pandas DataFrame with schema discovery.

```python
from document_graph.pipeline.extract.extract_provider_config import ExtractProviderConfig
from document_graph.pipeline.extract.extract_provider_csv import CSVExtractProvider
from document_graph.config import DocumentGraphConfig

config = ExtractProviderConfig(type='csv', source='users.csv', document_id='users-pipeline')
provider = CSVExtractProvider(config, DocumentGraphConfig())
result = provider.extract('data/users.csv')

print(f'Rows: {result.dataframe.shape[0]}')
print(f'Schema: {result.extracted_schema}')
print(f'Nodes: {len(result.nodes)}')
```

Supported formats:
- CSV (`CSVExtractProvider`)
- Excel (`ExcelExtractProvider`)
- JSON (`JSONExtractProvider`)
- Parquet (`ParquetExtractProvider`)

## Ingest

Shapes the DataFrame before transformation.

```python
from document_graph.ingest.ingestors_provider_config import IngestorProviderConfig
from document_graph.ingest.column.column_selector import ColumnSelectorProvider
from document_graph.ingest.column.column_renamer import ColumnRenamerProvider
from document_graph.ingest.row.skip_row import SkipRowProvider

# Select columns
config = IngestorProviderConfig(name='select', type='column', args={'columns': ['id', 'name', 'role']})
df = ColumnSelectorProvider(config).ingest(df)

# Rename columns
config = IngestorProviderConfig(name='rename', type='column', args={'mapping': {'id': 'user_id'}})
df = ColumnRenamerProvider(config).ingest(df)

# Filter rows
config = IngestorProviderConfig(name='filter', type='row', args={'field': 'role', 'values': ['viewer']})
df = SkipRowProvider(config).ingest(df)  # Removes viewers
```

## Transform

Processes records through one or more transformers (see [Transformers](transformers.md) for all types).

```python
from document_graph.transform.transformer_provider_config import TransformerProviderConfig
from document_graph.transform.normalizers.normalize_whitespace_provider import NormalizeWhitespaceProvider
from document_graph.transform.graph_transformers.row_to_node import RowToNodeTransformer

records = df.to_dict('records')

# Normalize
records = NormalizeWhitespaceProvider(TransformerProviderConfig(name='ws', args={})).transform(records)

# Convert to graph nodes
nodes = RowToNodeTransformer(TransformerProviderConfig(name='r2n', args={'type': 'User'})).transform(records)
```

## Build

Generates Cypher and writes to Neptune.

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from document_graph.graph_build import node_to_cypher, edge_to_cypher
from document_graph import Node, Edge

gs = GraphStoreFactory.for_graph_store('neptune-db://endpoint:8182').__enter__()
TENANT = 'my_app'

for record in records:
    node = Node(id=record['user_id'], labels=['User'], properties=record)
    cypher, params = node_to_cypher(node, tenant_id=TENANT)
    gs.execute_query(cypher, params)
```

## Full Example

```python
import pandas as pd
from document_graph.ingest.ingestors_provider_config import IngestorProviderConfig
from document_graph.ingest.column.column_selector import ColumnSelectorProvider
from document_graph.transform.transformer_provider_config import TransformerProviderConfig
from document_graph.transform.normalizers.normalize_whitespace_provider import NormalizeWhitespaceProvider
from document_graph.graph_build import node_to_cypher
from document_graph import Node

# Extract
df = pd.read_csv('data/users.csv')

# Ingest
config = IngestorProviderConfig(name='sel', type='column', args={'columns': ['id', 'name', 'email', 'role']})
df = ColumnSelectorProvider(config).ingest(df)

# Transform
records = df.to_dict('records')
records = NormalizeWhitespaceProvider(TransformerProviderConfig(name='ws', args={})).transform(records)

# Build
for r in records:
    node = Node(id=str(r['id']), labels=['User'], properties=r)
    cypher, params = node_to_cypher(node, tenant_id='my_app')
    gs.execute_query(cypher, params)
```

## Related Notebooks

- `001-Combined-Extract-Load-Build.ipynb` — full pipeline in one flow
- `002-Full-Pipeline-Test.ipynb` — all stages tested individually
