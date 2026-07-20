# Ingestors

Ingestors shape DataFrames before transformation — selecting columns, renaming fields, and filtering rows.

## Configuration

All ingestors use `IngestorProviderConfig`:

```python
from document_graph.ingest.ingestors_provider_config import IngestorProviderConfig

config = IngestorProviderConfig(
    name='my_ingestor',   # identifier
    type='column',        # 'column' or 'row'
    args={...}            # ingestor-specific arguments
)
```

## Column Ingestors

### Column Selector

Keep only specified columns.

```python
from document_graph.ingest.column.column_selector import ColumnSelectorProvider

config = IngestorProviderConfig(name='select', type='column', args={
    'columns': ['id', 'name', 'email', 'role']
})
df = ColumnSelectorProvider(config).ingest(df)
```

### Column Renamer

Rename columns.

```python
from document_graph.ingest.column.column_renamer import ColumnRenamerProvider

config = IngestorProviderConfig(name='rename', type='column', args={
    'mapping': {'id': 'user_id', 'name': 'full_name'}
})
df = ColumnRenamerProvider(config).ingest(df)
```

## Row Ingestors

### Skip Row

Remove rows matching specific field values.

```python
from document_graph.ingest.row.skip_row import SkipRowProvider

config = IngestorProviderConfig(name='skip', type='row', args={
    'field': 'role',
    'values': ['viewer', 'inactive']
})
df = SkipRowProvider(config).ingest(df)  # Removes rows where role is viewer or inactive
```

## Chaining Ingestors

Ingestors return DataFrames, so they chain naturally:

```python
df = pd.read_csv('users.csv')

# Chain: select → rename → filter
df = ColumnSelectorProvider(IngestorProviderConfig(
    name='sel', type='column', args={'columns': ['id', 'name', 'role', 'status']}
)).ingest(df)

df = ColumnRenamerProvider(IngestorProviderConfig(
    name='ren', type='column', args={'mapping': {'id': 'user_id'}}
)).ingest(df)

df = SkipRowProvider(IngestorProviderConfig(
    name='skip', type='row', args={'field': 'status', 'values': ['deleted']}
)).ingest(df)

records = df.to_dict('records')  # Ready for transformers
```

## Related Notebooks

- `03a-Ingestors-Test.ipynb` — all ingestor types tested
- `002-Full-Pipeline-Test.ipynb` — ingestors in full pipeline context
