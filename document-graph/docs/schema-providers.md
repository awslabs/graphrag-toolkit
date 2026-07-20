# Schema Providers

Schema providers load or discover ETL schemas that define how data flows through the pipeline.

## SchemaProviderConfig

All providers are configured with a single model:

```python
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig

config = SchemaProviderConfig(
    type="csv",                    # Provider type
    schema_id="my-dataset",        # Optional override
    connection_config={            # Provider-specific params
        "path": "/data/users.csv"
    }
)
```

**Supported types:** `file`, `s3`, `static`, `csv`, `json`, `excel`, `glue`, `parquet`, `yaml`, `xml`

## Provider Types

### CSV Provider

```python
config = SchemaProviderConfig(
    type="csv",
    connection_config={"path": "data/events.csv", "delimiter": ","}
)
```

### JSON Provider

```python
config = SchemaProviderConfig(
    type="json",
    connection_config={"path": "data/records.json"}
)
```

### S3 Provider

```python
config = SchemaProviderConfig(
    type="s3",
    connection_config={"bucket": "my-bucket", "key": "schemas/pipeline.yaml"}
)
```

### Static Provider

Returns a hardcoded default schema — useful for testing:

```python
from document_graph.schema import StaticSchemaProvider

provider = StaticSchemaProvider(config={})
schema = provider.load_schema()
```

### Factory (Auto-resolution)

```python
from document_graph.schema.providers.schema_provider_factory import get_schema_provider

provider = get_schema_provider(config)
schema = provider.load_schema()
```

## ETLSchema Model

The core schema model returned by all providers:

```python
from document_graph.schema import ETLSchema, ExtractConfig, TransformConfig, LoadConfig

schema = ETLSchema(
    schema_id="my-pipeline",
    description="Customer data pipeline",
    extract=ExtractConfig(
        source_type="csv",
        bucket=None,
        prefix=None,
        file_type="csv"
    ),
    transform=TransformConfig(
        chunking=ChunkingConfig(strategy="by_heading", min_length=100),
        metadata_mapping=MetadataMapping(title="document.title"),
        entity_extraction=EntityExtractionConfig(method="ner"),
        normalize=NormalizeConfig(remove_headers=True)
    ),
    load=LoadConfig(
        document_node=NodeDefinition(type="Document", fields=["title", "status"]),
        section_node=NodeDefinition(type="Section", fields=["text"]),
        relationships=[
            RelationshipDefinition(type="HAS_SECTION", source="doc_id", target="section_id")
        ]
    )
)
```

## Schema Discovery

Auto-infer schemas from data files:

```python
from document_graph.schema.discovery import SchemaDiscoveryRegistry

registry = SchemaDiscoveryRegistry()

# Discovers columns, types, and relationships from a CSV
schema = registry.discover("data/events.csv")
```

Supported discovery formats: CSV, JSON, Excel, Parquet, YAML, XML.
