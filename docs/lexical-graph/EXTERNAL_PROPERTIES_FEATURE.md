# External Properties Feature - Implementation Summary

## Overview

Added a flexible external properties feature that allows adding any business-specific properties from source document metadata to chunk nodes in the graph database.

## Changes Made

### 1. Configuration (`lexical-graph/src/graphrag_toolkit/lexical_graph/config.py`)
- Added `chunk_external_properties` property to `GraphRAGConfig`
- Accepts dictionary mapping chunk property names to source metadata keys
- Supports environment variable: `CHUNK_EXTERNAL_PROPERTIES` (JSON format)
- Default: `None` (feature disabled)

### 2. Chunk Node Builder (`lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/build/chunk_node_builder.py`)
- Extracts multiple properties from validated source metadata when configured
- Iterates through property mapping and adds each available property
- Adds to chunk metadata: `metadata['chunk']['metadata'][property_name]` (nested structure matching source metadata)
- Uses `_get_source_info_metadata()` to ensure only valid (non-collection-based) metadata is used

### 3. Chunk Graph Builder (`lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/build/chunk_graph_builder.py`)
- Stores all external properties as properties on chunk nodes
- Reads from nested `metadata['chunk']['metadata']` dictionary
- Dynamically generates SET statements for each property
- Uses: `SET chunk.property_name = params.property_name`

## Usage

```python
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from llama_index.core.schema import Document

# Configure multiple properties
GraphRAGConfig.chunk_external_properties = {
    'article_code': 'article_id',
    'document_type': 'doc_type',
    'department': 'dept_code'
}

# Create document with metadata
doc = Document(
    text="Your content...",
    metadata={
        'article_id': 'ART-2024-001',
        'doc_type': 'research',
        'dept_code': 'ENG'
    }
)

# Build graph - chunks will have all configured properties
```

## Query Examples

```cypher
// Find chunks by article code
MATCH (chunk:__Chunk__ {article_code: 'ART-2024-001'})
RETURN chunk

// Find chunks by document type
MATCH (chunk:__Chunk__ {document_type: 'research'})
RETURN chunk

// Complex multi-property query
MATCH (chunk:__Chunk__)
WHERE chunk.document_type = 'research'
  AND chunk.department = 'ENG'
RETURN chunk
```

## Files Modified

- `lexical-graph/src/graphrag_toolkit/lexical_graph/config.py`
- `lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/build/chunk_node_builder.py`
- `lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/build/chunk_graph_builder.py`

## Key Features

- **Flexible**: Support any number of properties
- **Configurable**: Dictionary-based mapping
- **Graceful**: Handles missing metadata keys
- **Backward Compatible**: No breaking changes
- **Safe**: Uses validated source metadata to avoid write failures
