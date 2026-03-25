# Composable Extraction Pipeline — Validation Guide

This guide describes how to validate the composable extraction pipeline feature (Option 2: Stage Registry with Typed Contracts).

## Prerequisites

```bash
cd lexical-graph
pip install -e ".[dev]"
```

## 1. Run Unit Tests

All new tests are in `tests/unit/indexing/extract/`:

```bash
cd lexical-graph
PYTHONPATH=src python -m pytest tests/unit/indexing/extract/ -v --no-cov
```

Expected: 80+ tests pass across these test files:
- `test_extraction_stage.py` — ExtractionStage ABC (7 tests)
- `test_pipeline_builder.py` — PipelineBuilder validation (10 tests)
- `test_stages.py` — LLM/Local/Topic stage wrappers (18 tests)
- `test_extraction_schema.py` — ExtractionSchema + EntityTypeConfig (11 tests)
- `test_schema_filter_stage.py` — SchemaFilterStage (12 tests)
- `test_json_topic_extraction.py` — JSON prompt + parser (8 tests)
- `test_ner_and_merge_stages.py` — NER + EntityMerge stages (14 tests)

## 2. Verify Full Suite Regression

```bash
cd lexical-graph
PYTHONPATH=src python -m pytest tests/unit/ -q --no-cov
```

Expected: All 1252+ tests pass. The coverage threshold failure (24% vs 40%) is pre-existing and unrelated.

## 3. Verify Imports

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    ExtractionStage,
    ExtractionSchema,
    EntityTypeConfig,
    PipelineBuilder,
    LLMPropositionStage,
    LocalPropositionStage,
    LLMTopicExtractionStage,
    SchemaFilterStage,
    NERExtractionStage,
    EntityMergeStage,
    BatchLLMPropositionStage,
    BatchTopicExtractionStage,
)
```

## 4. Validate PipelineBuilder Contract Checking

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    PipelineBuilder, LLMPropositionStage, LLMTopicExtractionStage,
)

# Valid pipeline — should succeed
builder = PipelineBuilder()
builder.add(LLMPropositionStage()).add(LLMTopicExtractionStage())
transforms = builder.build()
assert len(transforms) == 2

# Invalid pipeline — should raise ValueError (topics stage needs propositions)
try:
    PipelineBuilder().add(LLMTopicExtractionStage()).build()
    assert False, "Should have raised ValueError"
except ValueError as e:
    assert "propositions" in str(e).lower() or "missing" in str(e).lower()
```

## 5. Validate Schema Filtering

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    ExtractionSchema, EntityTypeConfig, SchemaFilterStage,
)

schema = ExtractionSchema(
    entity_types={
        'Person': EntityTypeConfig(aliases=['Individual']),
        'Company': EntityTypeConfig(),
    },
    relationship_types=['WORKS_FOR', 'FOUNDED'],
    strict=True,
)

# Verify prompt constraint formatting
print(schema.format_as_prompt_constraint())

# Verify stage contract
stage = SchemaFilterStage(schema=schema)
assert stage.stage_type == 'filter'
assert stage.input_keys() == ['aws::graph::topics']
assert stage.output_keys() == ['aws::graph::topics']
```

## 6. Validate JSON Topic Parsing

```python
import json
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import parse_extracted_topics_json

data = {
    "topics": [{
        "value": "Employment",
        "entities": [
            {"value": "Alice", "classification": "Person"},
            {"value": "Acme Corp", "classification": "Company"},
        ],
        "statements": [{
            "value": "Alice works at Acme Corp",
            "facts": [{
                "subject": {"value": "Alice", "classification": "Person"},
                "predicate": {"value": "WORKS_FOR"},
                "object": {"value": "Acme Corp", "classification": "Company"},
            }]
        }]
    }]
}

tc, errors = parse_extracted_topics_json(json.dumps(data))
assert len(errors) == 0
assert tc.topics[0].value == "Employment"
assert len(tc.topics[0].entities) == 2

# Verify fallback on bad JSON
tc2, errors2 = parse_extracted_topics_json("not json")
assert len(errors2) == 1
assert "JSON_PARSE_ERROR" in errors2[0]
```

## 7. Validate ExtractionConfig Integration

```python
from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LLMPropositionStage, LLMTopicExtractionStage, ExtractionSchema, EntityTypeConfig,
)

# Default — no stages, backward compatible
config = ExtractionConfig()
assert config.stages is None
assert config.schema is None

# Custom stages
config = ExtractionConfig(
    stages=[LLMPropositionStage(), LLMTopicExtractionStage()],
    schema=ExtractionSchema(
        entity_types={'Person': EntityTypeConfig()},
        strict=True,
    ),
)
assert len(config.stages) == 2
assert config.schema.strict is True
```

## 8. Validate NER + Merge Stage Contracts

```python
from graphrag_toolkit.lexical_graph.indexing.extract import (
    NERExtractionStage, EntityMergeStage, PipelineBuilder,
    LLMPropositionStage, LLMTopicExtractionStage,
)

# Full pipeline with NER
builder = PipelineBuilder()
builder.add(LLMPropositionStage())
builder.add(NERExtractionStage(entity_labels=['Person', 'Organization']))
builder.add(LLMTopicExtractionStage())
builder.add(EntityMergeStage())
transforms = builder.build()
assert len(transforms) == 4
```

## Component Summary

| Component | File | Purpose |
|-----------|------|---------|
| `ExtractionStage` | `extraction_stage.py` | ABC for pipeline stages |
| `PipelineBuilder` | `pipeline_builder.py` | Validates and composes stages |
| `ExtractionSchema` | `extraction_schema.py` | Entity/relationship type constraints |
| `LLMPropositionStage` | `stages/llm_proposition_stage.py` | Wraps LLMPropositionExtractor |
| `LocalPropositionStage` | `stages/local_proposition_stage.py` | Wraps PropositionExtractor |
| `LLMTopicExtractionStage` | `stages/llm_topic_extraction_stage.py` | Wraps TopicExtractor |
| `SchemaFilterStage` | `stages/schema_filter_stage.py` | Filters topics against schema |
| `NERExtractionStage` | `stages/ner_extraction_stage.py` | GLiNER-based NER |
| `EntityMergeStage` | `stages/entity_merge_stage.py` | Merges NER + LLM entities |
| `BatchLLMPropositionStage` | `stages/batch_stages.py` | Batch proposition extraction |
| `BatchTopicExtractionStage` | `stages/batch_stages.py` | Batch topic extraction |
| `EXTRACT_TOPICS_JSON_PROMPT` | `prompts.py` | JSON-output topic prompt |
| `parse_extracted_topics_json` | `utils/topic_utils.py` | JSON topic parser |
