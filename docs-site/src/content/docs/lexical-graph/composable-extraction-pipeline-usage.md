# Composable Extraction Pipeline — Usage Guide

The composable extraction pipeline lets you customize how the lexical graph extracts entities, relationships, and topics from your documents. Instead of the fixed default pipeline, you define a sequence of **stages** that are validated at build time and composed into a processing pipeline.

## Quick Start

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LLMPropositionStage,
    LLMTopicExtractionStage,
    PipelineBuilder,
)

# Build a pipeline identical to the default
builder = PipelineBuilder()
builder.add(LLMPropositionStage())
builder.add(LLMTopicExtractionStage())
transforms = builder.build()
```

Or pass stages directly to `ExtractionConfig`:

```python
from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig, LexicalGraphIndex

config = ExtractionConfig(
    stages=[
        LLMPropositionStage(),
        LLMTopicExtractionStage(),
    ]
)
```

When `stages` is provided, the pipeline builder validates that each stage's required inputs are satisfied by prior stages' outputs. If not, it raises a `ValueError` at build time — no silent failures at runtime.

## Available Stages

### Proposition Extraction

| Stage | Description | Requires |
|-------|-------------|----------|
| `LLMPropositionStage()` | Extracts propositions using an LLM | — |
| `LocalPropositionStage()` | Extracts propositions using a local transformer model | — |
| `BatchLLMPropositionStage(batch_config)` | Batch proposition extraction via Bedrock | `BatchConfig` |

### Topic & Entity Extraction

| Stage | Description | Requires |
|-------|-------------|----------|
| `LLMTopicExtractionStage()` | Extracts topics, entities, and relationships from propositions | propositions |
| `LLMTopicExtractionStage(use_propositions=False)` | Extracts directly from source text | — |
| `BatchTopicExtractionStage(batch_config)` | Batch topic extraction via Bedrock | `BatchConfig` |

### NER (Named Entity Recognition)

| Stage | Description | Requires |
|-------|-------------|----------|
| `NERExtractionStage(entity_labels=[...])` | CPU-based NER using GLiNER | `pip install gliner` |
| `EntityMergeStage()` | Merges NER entities into LLM-extracted topics | NER output + topics |

### Filtering

| Stage | Description | Requires |
|-------|-------------|----------|
| `SchemaFilterStage(schema)` | Filters entities/relationships against a schema | topics |

## Defining a Schema

Use `ExtractionSchema` to constrain what entity types and relationship types are extracted:

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    ExtractionSchema,
    EntityTypeConfig,
    SchemaFilterStage,
)

schema = ExtractionSchema(
    entity_types={
        'Person': EntityTypeConfig(
            description='A human individual',
            attributes=['age', 'role'],
            aliases=['Individual', 'Human'],
        ),
        'Company': EntityTypeConfig(
            description='A business organization',
        ),
    },
    relationship_types=['WORKS_FOR', 'FOUNDED', 'ACQUIRED'],
    strict=True,  # When True, SchemaFilterStage removes non-matching entities/relationships
)
```

Add the filter after topic extraction:

```python
config = ExtractionConfig(
    stages=[
        LLMPropositionStage(),
        LLMTopicExtractionStage(),
        SchemaFilterStage(schema),
    ]
)
```

When `strict=False` (the default), the filter stage passes everything through unchanged — useful for soft constraints where you want the schema for prompt guidance but not hard filtering.

## Combining NER with LLM Extraction

Augment LLM extraction with GLiNER-based NER to catch entities the LLM might miss:

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LLMPropositionStage,
    LLMTopicExtractionStage,
    NERExtractionStage,
    EntityMergeStage,
)

config = ExtractionConfig(
    stages=[
        LLMPropositionStage(),
        NERExtractionStage(
            entity_labels=['Person', 'Organization', 'Location'],
            threshold=0.5,
        ),
        LLMTopicExtractionStage(),
        EntityMergeStage(fuzzy_threshold=0.85),  # Merges NER entities, fuzzy dedup
    ]
)
```

`EntityMergeStage` performs case-insensitive deduplication — if the LLM already extracted "John" and NER also found "john", it won't be added twice.

For fuzzy deduplication (e.g., "DataBridge" ≈ "DataBridge AI"), set a similarity threshold:

```python
EntityMergeStage(fuzzy_threshold=0.85)  # 0-1, higher = stricter matching
```

With `fuzzy_threshold=None` (default), only exact case-insensitive matches are deduplicated.

## Using JSON Output for Topic Extraction

The `TopicExtractor` now supports JSON-formatted LLM output, which is more reliable than text parsing:

```python
from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor

extractor = TopicExtractor(output_format='json')
```

When `output_format='json'`, the extractor tries JSON parsing first and falls back to the legacy text parser if JSON parsing fails.

## Using Local Proposition Extraction

Skip the LLM for proposition extraction and use a local transformer model instead:

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LocalPropositionStage,
    LLMTopicExtractionStage,
)

config = ExtractionConfig(
    stages=[
        LocalPropositionStage(),  # Runs locally, no LLM cost
        LLMTopicExtractionStage(),
    ]
)
```

## Full Example: Schema-Constrained Pipeline with NER

```python
from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LLMPropositionStage,
    LLMTopicExtractionStage,
    NERExtractionStage,
    EntityMergeStage,
    SchemaFilterStage,
    ExtractionSchema,
    EntityTypeConfig,
)

schema = ExtractionSchema(
    entity_types={
        'Person': EntityTypeConfig(aliases=['Individual']),
        'Company': EntityTypeConfig(aliases=['Organization', 'Corp']),
        'Technology': EntityTypeConfig(),
    },
    relationship_types=['WORKS_FOR', 'USES', 'DEVELOPS', 'ACQUIRED'],
    strict=True,
)

config = ExtractionConfig(
    stages=[
        LLMPropositionStage(),
        NERExtractionStage(entity_labels=['Person', 'Organization', 'Technology']),
        LLMTopicExtractionStage(),
        EntityMergeStage(fuzzy_threshold=0.85),
        SchemaFilterStage(schema),
    ],
    schema=schema,
)
```

## Writing Custom Stages

Implement the `ExtractionStage` ABC:

```python
from typing import List
from llama_index.core.schema import BaseNode, TransformComponent
from graphrag_toolkit.lexical_graph.indexing.extract import ExtractionStage

class MyCustomTransform(TransformComponent):
    def __call__(self, nodes, **kwargs):
        for node in nodes:
            # Your processing logic here
            node.metadata['my_key'] = 'my_value'
        return nodes

class MyCustomStage(ExtractionStage):
    def input_keys(self) -> List[str]:
        return []  # Keys this stage requires in node metadata

    def output_keys(self) -> List[str]:
        return ['my_key']  # Keys this stage adds to node metadata

    def as_transform(self) -> TransformComponent:
        return MyCustomTransform()

    @property
    def stage_type(self) -> str:
        return 'custom'
```

The `PipelineBuilder` validates that `input_keys()` are available (from initial context or prior stages' `output_keys()`) before the pipeline runs. This catches wiring errors at build time.

## Backward Compatibility

When `stages` is not provided to `ExtractionConfig`, the existing default pipeline (LLM propositions → topic extraction) is used unchanged. All existing code continues to work without modification.
