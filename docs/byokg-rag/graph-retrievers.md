[[Home](./)]

## Graph Retrievers

Graph retrievers implement the various retrieval strategies used by BYOKG-RAG to find relevant information from knowledge graphs. Each retriever specializes in a different approach to graph exploration and information extraction.

### Topics

  - [Overview](#overview)
  - [Entity Linker](#entity-linker)
  - [Agentic Retriever](#agentic-retriever)
  - [Graph Scoring Retriever](#graph-scoring-retriever)
  - [Path Retriever](#path-retriever)
  - [Graph Query Retriever](#graph-query-retriever)
  - [Usage examples](#usage-examples)

### Overview

The graph retrievers component provides multiple strategies for retrieving relevant information from knowledge graphs. Each retriever implements the abstract `GRetriever` interface and specializes in different aspects of graph exploration:

- **Entity Linker** - Links query entities to graph nodes using various matching strategies
- **Agentic Retriever** - Uses LLM-guided iterative exploration for dynamic graph traversal
- **Graph Scoring Retriever** - Applies scoring and ranking to multi-hop graph traversal
- **Path Retriever** - Specializes in finding and verbalizing paths between entities
- **Graph Query Retriever** - Executes structured graph queries and verbalizes results

### Entity Linker

The Entity Linker performs two-step linking to connect natural language entities with graph nodes.

#### Architecture

The `EntityLinker` class extends the abstract `Linker` base class and provides entity matching capabilities:

```python
from graphrag_toolkit.byokg_rag.graph_retrievers import EntityLinker

entity_linker = EntityLinker(
    retriever=your_entity_matcher,
    topk=3
)
```

#### Key methods

**`link(query_extracted_entities, retriever, topk, id_selector, return_dict)`**

Links extracted entities to graph nodes using the configured retriever.

Parameters:
- `query_extracted_entities` (List[str]): List of entity lists to perform linking on
- `retriever` (object, optional): Entity retriever to use for lookup
- `topk` (int, optional): Number of items to return per entity
- `id_selector` (list, optional): Allowlist of entity IDs to consider
- `return_dict` (bool): Whether to return detailed results or just entity IDs

Returns:
- If `return_dict=True`: List of dictionaries with detailed linking results
- If `return_dict=False`: List of matched entity IDs

### Agentic Retriever

The Agentic Retriever implements an iterative exploration strategy using LLM-guided decision making to dynamically explore the knowledge graph.

#### Architecture

```python
from graphrag_toolkit.byokg_rag.graph_retrievers import AgenticRetriever

agentic_retriever = AgenticRetriever(
    llm_generator=your_llm,
    graph_traversal=your_traversal_component,
    graph_verbalizer=your_verbalizer,
    pruning_reranker=your_reranker,
    max_num_relations=5,
    max_num_entities=3,
    max_num_iterations=3,
    max_num_triplets=50
)
```

#### Retrieval process

1. **Start with source nodes** - Begin exploration from provided starting points
2. **Iterative exploration** - Use LLM to select relevant relations and entities
3. **Pruning and reranking** - Apply scoring to filter and rank results
4. **Context building** - Accumulate verbalized triplets as context
5. **Early termination** - Stop when LLM determines sufficient information is found

#### Key methods

**`retrieve(query, source_nodes, history_context)`**

Performs iterative graph exploration guided by LLM decisions.

**`relation_search_prune(query, entities, max_num_relations)`**

Searches and prunes relations based on relevance to the query.

### Graph Scoring Retriever

The Graph Scoring Retriever uses multi-hop traversal combined with scoring and reranking to efficiently retrieve relevant information.

#### Architecture

```python
from graphrag_toolkit.byokg_rag.graph_retrievers import GraphScoringRetriever

scoring_retriever = GraphScoringRetriever(
    graph_traversal=your_traversal_component,
    graph_verbalizer=your_verbalizer,
    graph_reranker=your_reranker,
    pruning_reranker=your_pruning_reranker
)
```

#### Key methods

**`retrieve(query, source_nodes, hops, topk, max_num_relations, max_num_triplets)`**

Retrieves information using multi-hop traversal with pruning and reranking.

Parameters:
- `query` (str): The search query
- `source_nodes` (list): Starting nodes for traversal
- `hops` (int): Number of hops to traverse (default: 2)
- `topk` (int): Maximum results to return
- `max_num_relations` (int): Maximum relations after pruning
- `max_num_triplets` (int): Maximum triplets after pruning

### Path Retriever

The Path Retriever specializes in finding and verbalizing paths in the knowledge graph, supporting both metapath-based traversal and shortest path finding.

#### Architecture

```python
from graphrag_toolkit.byokg_rag.graph_retrievers import PathRetriever

path_retriever = PathRetriever(
    graph_traversal=your_traversal_component,
    path_verbalizer=your_path_verbalizer
)
```

#### Key methods

**`follow_paths(source_nodes, metapaths)`**

Follows predefined metapaths from source nodes.

**`shortest_paths(source_nodes, target_nodes)`**

Finds shortest paths between source and target nodes.

**`retrieve(source_nodes, metapaths, target_nodes)`**

Combines metapath traversal and shortest path finding.

### Graph Query Retriever

The Graph Query Retriever executes structured graph queries and verbalizes the results.

#### Architecture

```python
from graphrag_toolkit.byokg_rag.graph_retrievers import GraphQueryRetriever

query_retriever = GraphQueryRetriever(
    graph_store=your_graph_store
)
```

#### Key methods

**`retrieve(graph_query, return_answers)`**

Executes a graph query and returns verbalized results.

Parameters:
- `graph_query` (str): The graph query to execute
- `return_answers` (bool): Whether to return answers along with results

Returns:
- If `return_answers=True`: Tuple of (verbalized results, raw answers)
- If `return_answers=False`: List of verbalized results

### Usage examples

#### Entity linking example

```python
# Initialize entity linker
entity_linker = EntityLinker(retriever=entity_matcher, topk=5)

# Link extracted entities
extracted_entities = [["aspirin", "headache"], ["drug", "treatment"]]
linking_results = entity_linker.link(
    query_extracted_entities=extracted_entities,
    return_dict=True
)

# Access linking results
for result in linking_results:
    hits = result['hits']
    for hit in hits:
        entity_ids = hit['document_id']
        entities = hit['document']
        scores = hit['match_score']
```

#### Agentic retrieval example

```python
# Initialize agentic retriever
agentic_retriever = AgenticRetriever(
    llm_generator=llm,
    graph_traversal=traversal,
    graph_verbalizer=verbalizer,
    max_num_iterations=3
)

# Perform iterative exploration
query = "What are the side effects of aspirin?"
source_nodes = ["aspirin_node_id"]
retrieved_triplets = agentic_retriever.retrieve(
    query=query,
    source_nodes=source_nodes
)
```

#### Multi-strategy combination

```python
# Combine multiple retrievers for comprehensive coverage
retrievers = {
    'agentic': AgenticRetriever(...),
    'scoring': GraphScoringRetriever(...),
    'path': PathRetriever(...),
    'query': GraphQueryRetriever(...)
}

# Use different retrievers based on query type
def retrieve_with_strategy(query, source_nodes, strategy='agentic'):
    retriever = retrievers[strategy]
    return retriever.retrieve(query, source_nodes)
