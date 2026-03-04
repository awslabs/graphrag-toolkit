# Indexing

## Overview

Indexing in byokg-rag enables efficient entity linking by mapping natural language mentions to knowledge graph nodes. The system supports three complementary index types that work together to match entities from user queries to graph entities with varying degrees of precision and semantic understanding.

Entity linking is a critical step in Knowledge Graph Question Answering (KGQA). When a user asks a question, the system must identify which entities in the knowledge graph are relevant. Indexes provide fast lookup mechanisms to find candidate entities based on string similarity, semantic meaning, or direct graph storage.

This document covers:

- Dense indexes for semantic similarity matching
- Fuzzy string indexes for approximate string matching
- Graph-store indexes for embedding-based retrieval directly from Neptune Analytics
- Guidance on selecting the appropriate index for your use case

## Dense Index

### Purpose

Dense indexes use embeddings to find entities based on semantic similarity rather than exact string matches. This approach captures meaning and context, allowing the system to link entities even when the query uses different wording than the entity labels in the graph.

### Architecture

The dense index stores vector embeddings of entity labels and uses similarity search to find the closest matches to a query embedding. The system supports local FAISS-based indexes for development and testing.

**LocalFaissDenseIndex** provides an in-memory vector index using FAISS (Facebook AI Similarity Search). It computes embeddings for entity labels and stores them in a FAISS index structure that enables fast approximate nearest neighbor search.

### AWS Services

Dense indexes require an embedding model to generate vector representations. The system integrates with:

- **Amazon Bedrock** - Provides access to foundation models for generating embeddings

### IAM Permissions

To use dense indexes with Amazon Bedrock embeddings, you need the following IAM permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:<region>::foundation-model/*"
    }
  ]
}
```

NOTE: Replace `<region>` with your AWS region (e.g., us-west-2).

### Configuration

Configure a local FAISS dense index:

```python
from graphrag_toolkit.byokg_rag.indexing import LocalFaissDenseIndex, LangChainEmbedding
from langchain_aws import BedrockEmbeddings

# Set up embedding model
bedrock_embeddings = BedrockEmbeddings(
    model_id="amazon.titan-embed-text-v2:0",
    region_name="<region>"
)
embedding = LangChainEmbedding(bedrock_embeddings)

# Create dense index
dense_index = LocalFaissDenseIndex(
    embedding=embedding,
    distance_type="l2",  # Options: "l2", "cosine"
    embedding_dim=1024   # Must match embedding model dimension
)

# Add entities to index
entities = ["Albert Einstein", "Marie Curie", "Isaac Newton"]
dense_index.add(entities)

# Query the index
results = dense_index.query("physicist who developed relativity", topk=3)
```

**Parameters:**

- `embedding` - Embedding instance that generates vector representations
- `distance_type` - Distance metric for similarity ("l2" or "cosine")
- `embedding_dim` - Dimension of embedding vectors (must match model output)

## Fuzzy String Index

### Purpose

Fuzzy string indexes handle variations in entity names through approximate string matching. This approach is effective for typos, abbreviations, and minor spelling differences without requiring embeddings or semantic understanding.

### Architecture

The fuzzy string index uses the `thefuzz` library to compute string similarity scores between query text and entity labels. It supports configurable matching thresholds and can filter candidates based on string length differences.

**FuzzyStringIndex** provides fast approximate string matching using Levenshtein distance and other string similarity algorithms. It maintains an in-memory mapping of entity labels and returns matches ranked by similarity score.

### Configuration

Configure a fuzzy string index:

```python
from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex

# Create fuzzy string index
fuzzy_index = FuzzyStringIndex()

# Add entities to index
entities = ["Albert Einstein", "Marie Curie", "Isaac Newton"]
fuzzy_index.add(entities)

# Query with fuzzy matching
results = fuzzy_index.match(
    inputs=["Albert Einstien", "Mary Curie"],  # Note: typos
    topk=1,
    max_len_difference=4
)
```

**Parameters:**

- `topk` - Number of top matches to return per query
- `max_len_difference` - Maximum allowed length difference between query and candidate
- `id_selector` - Optional function to filter candidates before matching

TIP: Fuzzy string matching works best for entity names with consistent structure. For highly variable entity descriptions, consider using dense indexes instead.

## Graph Store Index

### Purpose

Graph-store indexes store embeddings directly in the graph database, eliminating the need for separate index infrastructure. This approach is available for Amazon Neptune Analytics, which supports vector storage and similarity search natively.

### Architecture

**NeptuneAnalyticsGraphStoreIndex** stores entity embeddings as node properties in Neptune Analytics and uses the graph database's built-in vector search capabilities. This provides a unified storage layer for both graph structure and semantic embeddings.

### AWS Services

Graph-store indexes require:

- **Amazon Neptune Analytics** - Graph database with native vector search support
- **Amazon Bedrock** - Embedding model for generating vectors
- **Amazon S3** - Storage for embedding data during bulk loading

### IAM Permissions

To use graph-store indexes with Neptune Analytics, you need:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "neptune-graph:ReadDataViaQuery",
        "neptune-graph:GetGraph"
      ],
      "Resource": "arn:aws:neptune-graph:<region>:<account-id>:graph/<graph-id>"
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:<region>::foundation-model/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::<bucket-name>/*"
    }
  ]
}
```

NOTE: Replace `<region>`, `<account-id>`, `<graph-id>`, and `<bucket-name>` with your specific values.

### Configuration

Configure a Neptune Analytics graph-store index:

```python
from graphrag_toolkit.byokg_rag.graphstore import NeptuneAnalyticsGraphStore
from graphrag_toolkit.byokg_rag.indexing import NeptuneAnalyticsGraphStoreIndex, LangChainEmbedding
from langchain_aws import BedrockEmbeddings

# Set up graph store
graph_store = NeptuneAnalyticsGraphStore(
    graph_identifier="<graph-id>",
    region="<region>"
)

# Set up embedding model
bedrock_embeddings = BedrockEmbeddings(
    model_id="amazon.titan-embed-text-v2:0",
    region_name="<region>"
)
embedding = LangChainEmbedding(bedrock_embeddings)

# Create graph-store index
graph_index = NeptuneAnalyticsGraphStoreIndex(
    graphstore=graph_store,
    embedding=embedding,
    distance_type="l2",
    embedding_s3_save_path="s3://<bucket-name>/embeddings/"
)

# Query the index
results = graph_index.query("physicist who developed relativity", topk=3)
```

**Parameters:**

- `graphstore` - NeptuneAnalyticsGraphStore instance
- `embedding` - Embedding instance for generating vectors
- `distance_type` - Distance metric for similarity ("l2" or "cosine")
- `embedding_s3_save_path` - S3 path for storing embeddings during bulk operations

## Index Selection Guide

Choose the appropriate index type based on your requirements:

| Index Type | Best For | Pros | Cons |
|------------|----------|------|------|
| Dense Index | Semantic matching, paraphrases, synonyms | Captures meaning, handles varied wording | Requires embedding model, higher latency |
| Fuzzy String Index | Typos, abbreviations, exact name variations | Fast, no external dependencies | Limited to string similarity, no semantic understanding |
| Graph Store Index | Neptune Analytics deployments, unified storage | No separate index infrastructure, integrated with graph | Requires Neptune Analytics, S3 for bulk loading |

**Recommendations:**

- Use **fuzzy string index** as the default for most applications. It provides good performance with minimal setup.
- Add **dense index** when queries use varied terminology or when entity labels are inconsistent.
- Use **graph-store index** when deploying on Neptune Analytics to simplify infrastructure.
- Combine multiple indexes for comprehensive coverage. The entity linker can use multiple indexes in sequence.

TIP: Start with fuzzy string matching and add semantic indexes only if you observe poor entity linking performance in testing.
