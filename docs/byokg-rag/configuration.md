# Configuration

## Overview

The byokg-rag library provides extensive configuration options to customize query processing, retrieval strategies, and LLM (Large Language Model) behavior. Configuration occurs at multiple levels: query engine initialization, retriever setup, entity linking, and LLM parameters.

This document provides complete parameter documentation for all configurable components. Most components provide sensible defaults, allowing you to start with minimal configuration and adjust as needed for your specific use case.

## Query Engine Configuration

### ByoKGQueryEngine

The query engine orchestrates the entire KGQA (Knowledge Graph Question Answering) pipeline, coordinating entity linking, retrieval, and answer generation.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `graph_store` | GraphStore | Required | Graph store instance providing access to knowledge graph data | `NeptuneAnalyticsGraphStore(...)` |
| `entity_linker` | EntityLinker | Auto-created | Component for linking text mentions to graph entities | `EntityLinker(...)` |
| `triplet_retriever` | GRetriever | Auto-created | Retriever for extracting relevant triplets from the graph | `AgenticRetriever(...)` |
| `path_retriever` | PathRetriever | Auto-created | Retriever for finding paths between entities | `PathRetriever(...)` |
| `graph_query_executor` | GraphQueryRetriever | Auto-created | Executor for running structured graph queries | `GraphQueryRetriever(...)` |
| `llm_generator` | BaseGenerator | Auto-created | Language model for generating responses | `BedrockGenerator(...)` |
| `kg_linker` | KGLinker | Auto-created | Linker for multi-strategy retrieval operations | `KGLinker(...)` |
| `cypher_kg_linker` | CypherKGLinker | None | Specialized linker for Cypher-based retrieval | `CypherKGLinker(...)` |
| `direct_query_linking` | bool | False | Enable direct entity linking using query embeddings | `True` |

NOTE: When parameters are not provided, the query engine creates default instances with standard configurations. You can override any component to customize behavior.

#### Query Method Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `query` | str | Required | The natural language question to answer | "Who won the Nobel Prize in Physics in 1921?" |
| `iterations` | int | 2 | Number of multi-strategy retrieval iterations | 3 |
| `cypher_iterations` | int | 2 | Number of Cypher query generation attempts | 3 |
| `user_input` | str | "" | Additional instructions or context for the LLM | "Focus on recent discoveries" |

**Valid Ranges:**

- `iterations`: 1-10 (higher values increase retrieval coverage but also latency)
- `cypher_iterations`: 1-5 (higher values allow more query refinement attempts)

## Retriever Configuration

### AgenticRetriever

The agentic retriever implements iterative, LLM-guided exploration of the knowledge graph.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `llm_generator` | BaseGenerator | Required | Language model for guiding exploration | `BedrockGenerator(...)` |
| `graph_traversal` | GTraversal | Required | Component for traversing graph structure | `GTraversal(graph_store)` |
| `graph_verbalizer` | TripletGVerbalizer | Required | Component for converting triplets to text | `TripletGVerbalizer()` |
| `pruning_reranker` | Reranker | None | Optional reranker for pruning results | `BGEReranker()` |
| `max_num_relations` | int | 5 | Maximum relations to consider per iteration | 10 |
| `max_num_entities` | int | 3 | Maximum entities to explore per iteration | 5 |
| `max_num_iterations` | int | 3 | Maximum exploration iterations | 5 |
| `max_num_triplets` | int | 50 | Maximum triplets to retain after pruning | 100 |

**Parameter Guidelines:**

- Increase `max_num_relations` for broader exploration of relationship types
- Increase `max_num_entities` to explore more entity neighborhoods
- Increase `max_num_iterations` for complex multi-hop reasoning
- Increase `max_num_triplets` to retain more context (at the cost of LLM input length)

### PathRetriever

The path retriever finds structured paths between entities following metapath patterns.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `graph_traversal` | GTraversal | Required | Component for traversing graph structure | `GTraversal(graph_store)` |
| `path_verbalizer` | PathVerbalizer | Required | Component for converting paths to text | `PathVerbalizer()` |

The path retriever has minimal configuration. Its behavior is primarily controlled by the metapaths provided during retrieval.

### GraphQueryRetriever

The graph query retriever executes structured queries (openCypher) against the graph store.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `graph_store` | GraphStore | Required | Graph store instance for query execution | `NeptuneAnalyticsGraphStore(...)` |
| `block_graph_modification` | bool | True | Block queries that modify the graph | `True` |

WARNING: Setting `block_graph_modification` to False allows DELETE, CREATE, and other modification operations. Only disable this in controlled environments where query safety is guaranteed.

## Entity Linker Configuration

### KGLinker

The KG linker coordinates LLM-based entity extraction and linking for multi-strategy retrieval.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `llm_generator` | BaseGenerator | Required | Language model for entity extraction | `BedrockGenerator(...)` |
| `graph_store` | GraphStore | Required | Graph store for schema and entity information | `NeptuneAnalyticsGraphStore(...)` |
| `max_input_tokens` | int | 32000 | Maximum tokens allowed in user input and question | 16000 |

The `max_input_tokens` parameter prevents excessively long inputs that could cause LLM errors or high costs.

### CypherKGLinker

The Cypher KG linker specializes in generating and executing openCypher queries.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `llm_generator` | BaseGenerator | Required | Language model for Cypher generation | `BedrockGenerator(...)` |
| `graph_store` | GraphStore | Required | Graph store supporting openCypher execution | `NeptuneAnalyticsGraphStore(...)` |
| `max_input_tokens` | int | 32000 | Maximum tokens allowed in user input and question | 16000 |

NOTE: The graph store must support openCypher query execution. Use Neptune Analytics or Neptune Database graph stores.

## LLM Configuration

### BedrockGenerator

The Bedrock generator provides access to foundation models through Amazon Bedrock.

#### Constructor Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `model_name` | str | "anthropic.claude-3-7-sonnet-20250219-v1:0" | Bedrock model identifier | "anthropic.claude-3-5-sonnet-20240620-v1:0" |
| `region_name` | str | "us-west-2" | AWS region for Bedrock service | "us-east-1" |
| `max_tokens` | int | 4096 | Maximum tokens to generate in responses | 8192 |
| `max_retries` | int | 10 | Maximum retry attempts for failed requests | 5 |
| `prefill` | bool | False | Enable response prefilling (advanced) | False |
| `inference_config` | dict | None | Custom inference configuration | `{"temperature": 0.7}` |
| `reasoning_config` | dict | None | Reasoning configuration for supported models | None |

**Supported Models:**

- Claude 3.5 Sonnet: `anthropic.claude-3-5-sonnet-20240620-v1:0`
- Claude 3.7 Sonnet: `anthropic.claude-3-7-sonnet-20250219-v1:0`
- Claude 3 Opus: `anthropic.claude-3-opus-20240229-v1:0`
- Claude 3 Haiku: `anthropic.claude-3-haiku-20240307-v1:0`

TIP: Claude 3.5 Sonnet provides the best balance of performance and cost for most KGQA applications. Use Claude 3.7 Sonnet for the latest capabilities.

**Inference Configuration:**

The `inference_config` parameter accepts a dictionary with Bedrock inference parameters:

```python
inference_config = {
    "temperature": 0.7,      # Controls randomness (0.0-1.0)
    "topP": 0.9,             # Nucleus sampling threshold
    "maxTokens": 4096        # Maximum tokens to generate
}
```

## Complete Configuration Example

This example shows a fully configured query engine with custom components:

```python
from graphrag_toolkit.byokg_rag.graphstore import NeptuneAnalyticsGraphStore
from graphrag_toolkit.byokg_rag.llm import BedrockGenerator
from graphrag_toolkit.byokg_rag.graph_connectors import KGLinker
from graphrag_toolkit.byokg_rag.graph_retrievers import (
    AgenticRetriever,
    PathRetriever,
    GraphQueryRetriever,
    EntityLinker,
    GTraversal,
    TripletGVerbalizer,
    PathVerbalizer
)
from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex
from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine

# Step 1: Set up graph store
graph_store = NeptuneAnalyticsGraphStore(
    graph_identifier="<graph-id>",
    region="<region>"
)

# Step 2: Set up LLM
llm_generator = BedrockGenerator(
    model_name="anthropic.claude-3-5-sonnet-20240620-v1:0",
    region_name="<region>",
    max_tokens=4096,
    max_retries=10
)

# Step 3: Set up entity linking
fuzzy_index = FuzzyStringIndex()
fuzzy_index.add(graph_store.nodes())
entity_matcher = fuzzy_index.as_entity_matcher()
entity_linker = EntityLinker(entity_matcher)

# Step 4: Set up retrievers
graph_traversal = GTraversal(graph_store)
triplet_verbalizer = TripletGVerbalizer()
path_verbalizer = PathVerbalizer()

triplet_retriever = AgenticRetriever(
    llm_generator=llm_generator,
    graph_traversal=graph_traversal,
    graph_verbalizer=triplet_verbalizer,
    max_num_relations=5,
    max_num_entities=3,
    max_num_iterations=3,
    max_num_triplets=50
)

path_retriever = PathRetriever(
    graph_traversal=graph_traversal,
    path_verbalizer=path_verbalizer
)

graph_query_executor = GraphQueryRetriever(
    graph_store=graph_store,
    block_graph_modification=True
)

# Step 5: Set up KG linker
kg_linker = KGLinker(
    llm_generator=llm_generator,
    graph_store=graph_store,
    max_input_tokens=32000
)

# Step 6: Create query engine
query_engine = ByoKGQueryEngine(
    graph_store=graph_store,
    entity_linker=entity_linker,
    triplet_retriever=triplet_retriever,
    path_retriever=path_retriever,
    graph_query_executor=graph_query_executor,
    llm_generator=llm_generator,
    kg_linker=kg_linker,
    direct_query_linking=False
)

# Step 7: Execute query
context = query_engine.query(
    query="Who won the Nobel Prize in Physics in 1921?",
    iterations=2,
    user_input=""
)

print("Retrieved context:")
for item in context:
    print(f"  - {item}")
```

This example demonstrates explicit configuration of all components. In practice, you can rely on defaults for most parameters and only customize what you need.
