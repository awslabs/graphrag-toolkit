# Transformers

20+ built-in transformers across 6 categories. All configured via `TransformerProviderConfig`.

## TransformerProviderConfig

```python
from document_graph.transform.transformer_provider_config import TransformerProviderConfig

config = TransformerProviderConfig(
    name="normalize_case",
    type="normalizer",
    args={"target_case": "lower", "fields": ["title", "description"]}
)
```

## Categories

### Normalizers

Clean and standardize raw values.

| Transformer | Purpose |
|-------------|---------|
| `normalize_case` | Lowercase/uppercase/titlecase |
| `normalize_whitespace` | Collapse whitespace |
| `normalize_nulls` | Standardize null representations |
| `normalize_timestamp` | Parse dates to ISO format |
| `normalize_enum` | Map values to canonical enums |
| `normalize_spelling` | Fix common misspellings |

```python
TransformerProviderConfig(
    name="normalize_timestamp",
    args={"fields": ["created_at"], "format": "%Y-%m-%d"}
)
```

### Field Transformers

Operate on individual field values.

| Transformer | Purpose |
|-------------|---------|
| `json_flattener` | Flatten nested JSON |
| `comma_flattener` | Split comma-delimited values |
| `json_array_expander` | Expand arrays into rows |
| `regex_cleaner` | Regex-based cleaning |
| `standardize_enum` | Normalize enumerations |
| `normalize_timestamp` | Date field normalization |
| `embedded_json` | Parse embedded JSON strings |
| `uuid_generator` | Generate UUIDs |
| `paired_flattener` | Flatten key-value pairs |

```python
TransformerProviderConfig(
    name="json_flattener",
    args={"field": "metadata", "separator": "_"}
)
```

### Document Transformers

Transform entire documents/records.

| Transformer | Purpose |
|-------------|---------|
| `text_chunker` | Split text into chunks |
| `json_to_rows` | Convert JSON to tabular rows |
| `pii_redactor` | Redact PII from text |

```python
TransformerProviderConfig(
    name="text_chunker",
    args={"strategy": "by_heading", "min_length": 100}
)
```

### Filter Transformers

Include or exclude rows/columns.

| Transformer | Purpose |
|-------------|---------|
| `row_filter` | Filter rows by condition |
| `column_pruner` | Remove unwanted columns |

```python
TransformerProviderConfig(
    name="row_filter",
    args={"field": "status", "operator": "eq", "value": "active"}
)
```

### Graph Transformers

Convert data into graph elements.

| Transformer | Purpose |
|-------------|---------|
| `row_to_node` | Convert each row to a Node |
| `infer_edges` | Infer edges from foreign keys |

```python
TransformerProviderConfig(
    name="row_to_node",
    args={"label": "Document", "id_field": "doc_id"}
)
```

### Truncators

Limit output size.

| Transformer | Purpose |
|-------------|---------|
| `length_truncator` | Truncate by character length |
| `token_truncator` | Truncate by token count |
| `field_count_truncator` | Limit number of fields |

```python
TransformerProviderConfig(
    name="token_truncator",
    args={"max_tokens": 512, "fields": ["content"]}
)
```

## Writing Custom Transformers

Extend `TransformerProviderBase`:

```python
from document_graph.transform.transformer_provider_base import TransformerProviderBase
from document_graph.transform.transformer_provider_config import TransformerProviderConfig


class MyCustomTransformer(TransformerProviderBase):
    """Custom transformer that adds a computed field."""

    name = "my_custom"

    def __init__(self, config: TransformerProviderConfig):
        super().__init__(config)
        self._suffix = config.args.get("suffix", "_processed")

    def transform(self, records: list[dict]) -> list[dict]:
        for record in records:
            record[f"title{self._suffix}"] = record.get("title", "").upper()
        return records
```

Register it:

```python
from document_graph.transform.transformer_provider_registry import TransformerProviderRegistry

registry = TransformerProviderRegistry()
registry.register("my_custom", MyCustomTransformer)
```

Use it:

```python
config = TransformerProviderConfig(name="my_custom", args={"suffix": "_clean"})
transformer = registry.get("my_custom", config)
output = transformer.transform(records)
```
