[[Home](./)]

## Overview

The graphrag-toolkit [byokg-rag](../../byokg-rag/) library provides a framework for Knowledge Graph Question Answering (KGQA) that combines Large Language Models (LLMs) with existing knowledge graphs. The system allows applications to bring their own knowledge graph and perform complex question answering using multiple retrieval strategies.

  - [Graph stores and model providers](#graph-stores-and-model-providers)
  - [Multi-strategy retrieval](#multi-strategy-retrieval)
    - [Agentic retrieval](#agentic-retrieval)
    - [Scoring-based retrieval](#scoring-based-retrieval)
    - [Path-based retrieval](#path-based-retrieval)
    - [Query-based retrieval](#query-based-retrieval)
  - [System components](#system-components)
  - [Query processing](#query-processing)
  - [Getting started](#getting-started)

### Graph stores and model providers

The byokg-rag library depends on two backend systems: a _graph store_ and a _foundation model provider_. The graph store manages the knowledge graph data structure and provides interfaces for graph traversal and querying. The foundation model provider hosts the Large Language Models (LLMs) used for question understanding, entity linking, and answer generation.

The library supports Amazon Neptune graph databases that provide schema information and query execution capabilities. The default LLM provider is Amazon Bedrock with Claude 3.5 Sonnet, though it can be extended to support other LLM providers.

### Multi-strategy retrieval

The byokg-rag library implements a multi-strategy approach to information retrieval from knowledge graphs. Unlike traditional single-strategy approaches, it combines four complementary retrieval methods to provide comprehensive coverage of relevant information.

#### Agentic retrieval

Agentic retrieval uses LLM-powered agents to dynamically explore the knowledge graph based on the question context. The agents make decisions about which graph paths to follow, adapting their exploration strategy based on intermediate findings. This approach is particularly effective for complex, multi-step reasoning tasks where the optimal retrieval path cannot be predetermined.

#### Scoring-based retrieval

Scoring-based retrieval assigns relevance scores to graph triplets based on their relationship to the user question. The system uses scoring functions relying on semantic similarity. Triplets (edges) are ranked by their scores, and the top-k results are selected for answer generation.

#### Path-based retrieval

Path-based retrieval focuses on multi-hop reasoning by following structured paths through the knowledge graph. The system identifies relevant metapath patterns and traverses the graph following these patterns to connect entities through intermediate nodes. This approach is effective for questions that require understanding complex relationships and dependencies between entities.

#### Query-based retrieval

Query-based retrieval translates natural language questions into structured graph queries (e.g., Cypher, SPARQL) and executes them directly against the knowledge graph. This approach provides precise, efficient access to specific information when the question can be mapped to well-defined query patterns.

### System components

The byokg-rag framework consists of several key components:

1. **Graph Store** ([src/graphrag_toolkit/byokg_rag/graphstore](../../byokg-rag/src/graphrag_toolkit/byokg_rag/graphstore)) - Manages the knowledge graph data structure and provides interfaces for graph traversal and querying.

2. **Graph Connectors** ([src/graphrag_toolkit/byokg_rag/graph_connectors](../../byokg-rag/src/graphrag_toolkit/byokg_rag/graph_connectors)) - Links natural language queries to graph entities and paths using LLMs. Includes KGLinker for basic linking functionality and CypherKGLinker for Cypher-specific operations.

3. **Graph Retrievers** ([src/graphrag_toolkit/byokg_rag/graph_retrievers](../../byokg-rag/src/graphrag_toolkit/byokg_rag/graph_retrievers)) - Implements the various retrieval strategies:
   - **Entity Linker** - Matches entities from text to graph nodes using exact matching, fuzzy string matching, and semantic similarity
   - **Triplet Retriever** - Retrieves relevant triplets from the graph and verbalizes them in natural language format
   - **Path Retriever** - Finds paths between entities following metapath patterns for structured traversal

4. **Query Engine** ([src/graphrag_toolkit/byokg_rag/byokg_query_engine.py](../../byokg-rag/src/graphrag_toolkit/byokg_rag/byokg_query_engine.py)) - Orchestrates all components to process natural language questions and generate answers based on retrieved information.

### Query processing

Query processing in byokg-rag follows an iterative pipeline through the `ByoKGQueryEngine`:

1. **Initialization** - Set up context lists and entity tracking for the retrieval process
2. **Direct Query Linking** (optional) - Use semantic similarity to link the query directly to graph entities
3. **Cypher-based Retrieval** (if CypherKGLinker provided) - Generate and execute Cypher queries with iterative refinement
4. **Multi-Strategy Retrieval** (if KGLinker provided) - Use iterative LLM-guided retrieval:
   - Extract entities from natural language using LLM
   - Link extracted entities to graph nodes using fuzzy string matching
   - Retrieve triplets using agentic exploration from source entities
   - Follow metapaths extracted by LLM between entities
   - Execute structured graph queries generated by LLM
5. **Context Management** - Combine results with deduplication and order preservation
6. **Task Completion** - Monitor for LLM completion signals or reach maximum iterations

The system's performance has been evaluated across multiple knowledge graph benchmarks:

| KGQA Hit (%) | Wiki-KG | Temp-KG | Med-KG |
|--------------|---------|---------|--------|
| Agent        | 77.8    | 57.3    | 59.2   |
| BYOKG-RAG    | 80.1    | 65.5    | 65.0   |

### Getting started

You can get started with byokg-rag by installing the package and running the demo notebook:

```bash
pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.15.3.zip#subdirectory=byokg-rag
```

The repository includes several [example notebooks](../../examples/byokg-rag/) that demonstrate how to use the library with different graph stores and datasets:

- [Local Graph Demo](../../examples/byokg-rag/byokg_rag_demo_local_graph.ipynb) - Getting started with local graph databases
- [Neptune Analytics Demo](../../examples/byokg-rag/byokg_rag_neptune_analytics_demo.ipynb) - Using Amazon Neptune Analytics
- [Neptune Analytics with Cypher](../../examples/byokg-rag/byokg_rag_neptune_analytics_demo_cypher.ipynb) - Cypher-based retrieval with Neptune Analytics
- [Neptune Database Demo](../../examples/byokg-rag/byokg_rag_neptune_db_cluster_demo.ipynb) - Using Amazon Neptune Database clusters
- [Neptune Analytics Embeddings](../../examples/byokg-rag/byokg_rag_neptune_analytics_embeddings.ipynb) - Working with embeddings in Neptune Analytics
