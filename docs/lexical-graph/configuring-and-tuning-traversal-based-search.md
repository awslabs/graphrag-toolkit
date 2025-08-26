[[Home](./)]

## Configuring and Tuning Traversal-Based Search

### Topics

  - [Overview](#overview)
  - [Search results configuration](#search-results-configuration)
    - [max_search_results](#max_search_results)
    - [max_statements_per_topic](#max_statements_per_topic)
    - [max_statements](#max_statements)
    - [statement_pruning_factor](#statement_pruning_factor)
    - [statement_pruning_threshold](#statement_pruning_threshold)
    - [When to use search results configuration](#when-to-use-search-results-configuration)
  - [Retriever selection](#retriever-selection)
    - [retrievers](#retrievers)
    - [When to use different retrievers](#when-to-use-different-retrievers)
  - [Reranking strategy](#reranking-strategy)
    - [reranker](#reranker)
    - [Choosing a reranker strategy](#choosing-a-reranker-strategy)
    - [Troubleshooting reranking results](#troubleshooting-reranking-results)
  - [Graph and vector search parameters](#graph-and-vector-search-parameters)
    - [intermediate_limit](#intermediate_limit)
    - [query_limit](#query_limit)
    - [vss_top_k](#vss_top_k)
    - [vss_diversity_factor](#vss_diversity_factor)
    - [num_workers](#num_workers)
    - [When to change the graph and vector search parameters](#when-to-change-the-graph-and-vector-search-parameters)
  - [Entity network context selection](#entity-network-context-selection)
    - [Entity network generation](#entity-network-generation)
    - [ec_max_depth](#ec_max_depth)
    - [ec_max_contexts](#ec_max_contexts)
    - [ec_max_score_factor](#ec_max_score_factor)
    - [ec_min_score_factor](#ec_min_score_factor)
    - [When to adjust entity network generation](#when-to-adjust-entity-network-generation)


   

### Overview

You can customize traversal-based search operations to better suit your specific application, dataset, and query types. The following configuration options are available to help you optimize search performance:

  - [**Search results configuration**](#search-results-configuration) Adjust the number of search results and statements returned and set scoring thresholds to filter out low-quality statements and results
  - [**Retriever selection**](#retriever-selection) Specify which retrievers to use when fetching information
  - [**Reranking strategy**](#reranking-strategy) Modify how statements and results are reranked and sorted
  - [**Graph and vector search parameters**](#graph-and-vector-search-parameters) Customize parameters that control graph queries and vector searches
  - [**Entity network context selection**](#entity-network-context-selection) Configure parameters used to select entity network contexts

These options allow you to fine-tune your search behavior based on your specific requirements and improve the relevance of returned results.
___

### Search results configuration

When configuring search functionality, you can use the following parameters to control the number and quality of returned results:

#####  `max_search_results`

Defines the maximum number of search results to return. Each search result contains one or more statements that belong to the same topic (and source). If you set this to `None`, all matching search results will be returned. The default value is `5`.

#####  `max_statements_per_topic`

Controls how many statements can be included with a single topic, effectively limiting the size of each search result. If set to `None`, all statements belonging to the topic that match the search will be included in the result. The default value is `10`.

#####  `max_statements`

Limits the total number of statements across the entire resultset. If you set this to `None`, all statements from all results will be returned. The default value is `100`.

#####  `statement_pruning_factor`

This parameter helps filter out lower-quality statements based on a percentage of the highest statement score in the entire set of results. Any statement with a score less than `<maximum_statement_score> * statement_pruning_factor` will be removed from the results. The default value is `0.1` (10% of the maximum score).

##### `statement_pruning_threshold`

Sets an absolute minimum score threshold for statements. Any statement with a score lower than this threshold will be removed from the results. The default value is `None`.

#### Example

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    statement_pruning_threshold=0.2
)
```

#### When to use search results configuration

The `max_search_results`, `max_statements_per_topic` and `max_statements` parameters allow you to control the overall size of the results. 

Each search result comprises one or more statements belonging to a single topic from a single source. Statements from the same source but different topics appear as separate search results. Increasing `max_search_results` increases the variety of sources in your results. Increasing `max_statements_per_topic` adds more detail to each individual search result.

When increasing the number of statements (either overall or per topic), you should consider increasing the statement pruning parameters as well. This helps ensure that even with larger result sets, you're still getting highly relevant statements rather than less relevant information.

___

### Retriever selection

You can use the `retrievers` parameter to configure traversal-based search with up to [three different retrievers](./traversal-based-search.md#retrievers).

#####  `retrievers`

Accepts an array of retriever class names. Choose from:

  - **`ChunkBasedSearch`** This retriever uses a vector similarity search to find information that is similar to the original query. The retriever first finds relevant chunks using vector similarity search. From these chunks, the retriever traverses topics, statements, and facts. Chunk-based search tends to return a narrowly-scoped set of results based on the statement and fact neighbourhoods of chunks that match the original query.
  - **`EntityBasedSearch`** This retriever uses as its starting points the entities in an entity network context. From these entities, the retriever traverses facts, statements and topics. Entity-based search tends to return a broadly-scoped set of results, based on the neighbourhoods of individual entities and the facts that connect entities.
  - **`EntityNetworkSearch`** This retriever uses textual transcriptions of an entity network context to drive vector searches for information that is dissimilar to the original query but nonetheless structurally relevant for creating an accurate and full response. These vector searches return chunks that are similar to 'something different from the question being asked'. From these chunks, the retriever traverses topics, statements, and facts to explore the structurally relevant space of dissimilar content.
	
#### Example

```python
from graphrag_toolkit.lexical_graph.retrieval.retrievers import *

query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    retrievers=[ChunkBasedSearch, EntityBasedSearch]
)
```

#### When to use different retrievers

By default, traversal-based search is configured to use a combination of `ChunkBasedSearch` and `EntityNetworkSearch`. This combination provides access to content that is both directly similar to the question and content that may be relevant but not explicitly mentioned in the query.

Consider using the `ChunkBasedSearch` retriever by itself if:

  - Your queries need primarily similarity-based search
  - You want to focus on individual relevant statements rather than entire chunks
  - You need broader search scope than traditional vector search

This retriever uses local connectivity to find relevant statements in other chunks from the same source, expanding beyond basic vector similarity.

The `EntityBasedSearch` and `EntityNetworkSearch` retrievers provide different ways of utilising entity networks in a search:

  - The `EntityBasedSearch` uses global connectivity to find statements from different sources connected by the same facts. It often produces more diverse results than other retrievers. 
   - The `EntityNetworkSearch` retriever converts an entity network (retrieved through graph traversal) into a set of similarity searches. This approach balances global and local connectivity.

___

### Reranking strategy

Traversal-based search incorporates reranking at two key points during the retrieval process:

  - When generating entity network contexts, both entities and entity networks are reranked
  - Before finalizing search results, the complete set of statements undergoes reranking

Reranking is managed through a single parameter:

#####  `reranker`

Parameters options:

  - `model`: Uses a LlamaIndex-based `SentenceReranker` to rerank all statements in the result set
  - `tfidf` (default): Applies a term frequency-inverse document frequency measure to rank statements 
  - `None`: Disables the reranking feature completely

The tfidf-based option is significantly faster than the model-based approach. To use the model reranker, you must first install the following additional dependencies:

```
pip install torch sentence_transformers
```

#### Example

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    reranker='model'
)
```

#### Choosing a reranker strategy

The tfidf reranker option provides a fast, cost-effective, and generally effective solution for most use cases. However, if you find that the results don't meet your requirements, consider switching to the model reranker. Be aware that while model may provide different results, it operates significantly slower than tfidf and doesn't guarantee improved outcomes.

##### Troubleshooting reranking results

An effective reranking strategy should ensure that only highly relevant statements appear in your final results. For reranking to work properly, the relevant statements must first be captured by your retrievers before the reranking process begins.

If your search results don't include content you expect to see, verify whether this content is present in the pre-ranked results by:

  1. Disabling the reranker by setting `reranker=None`
  2. Increasing the following parameters in your [search results configuration](#search-results-configuration):
    - [max_search_results](#max_search_results)
    - [max_statements_per_topic](#max_statements_per_topic)
    - [max_statements](#max_statements)

After making these adjustments, review the results returned by the `retrieve()` operation. If the expected content still doesn't appear, the issue isn't related to reranking. Instead, consider other tuning approaches described elsewhere in the documentation, such as:

  - Changing your retriever configuration
  - Adjusting pruning thresholds
  - Configuring entity network contexts

___

### Graph and vector search parameters

These settings govern how the system queries both the graph and vector stores. When a user submits a query, multiple searches run across both stores, with some executing in parallel. The vector store returns the most similar items based on a top K approach. Results can be diversified across different sources. Graph store queries return statement sets, grouped by their source. Graph queries use a two-phase process: initial statement identification followed by connection exploration.

##### `intermediate_limit`

Controls how many statements are identified in the first phase of a graph query, before exploring their connections (both local and global). The default value is `50`.

##### `query_limit`

Defines how many results each graph query returns. Each result consists of statements from a single source. The default value is `10`.

##### `vss_top_k`

Specifies how many top matching results are used to begin similarity-based traversals. The default value is `10`.

##### `vss_diversity_factor`

Ensures results come from a diverse range of sources. Queries to a vector store retrieve (`vss_top_k × vss_diversity_factor`) initial matches, and then iteratively select the most relevant result from previously unused sources. This process continues until reaching `vss_top_k` total results. If set to `None`, simply returns the first `vss_top_k` matches. The default value is `5`.

##### `num_workers`
 
Sets the number of threads available for running graph queries in parallel. The default value is `10`.

#### Example

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    intermediate_limit=25,
    num_workers=3
)
```

#### When to change the graph and vector search parameters

Whereas the [search results configuration](#search-results-configuration) parameters control the handling of the search results, the graph and vector store configuration parameters control the query processing used to generate the results. 

If your queries require finding highly diverse content from across multiple sources, increase the `vss_diversity_factor`. If your queries require content that derives directly from primary sources, reduce `vss_diversity_factor`, or set it to `None`.

If you experience out of memeory issue while running user queries, reduce the `intermediate_limit` and `num_workers`. This will reduce the size of the working set for each graph query, and reduce the number of graph queries running in parallel.

If your application requires a large number of search results, you should consider increasing the `intermediate_limit`, `query_limit` and/or `vss_top_k`. Note that increasing these parameters can increase query latencies, and require more memory.

___

### Entity network context selection

The system creates focused [entity network contexts](./traversal-based-search.md#entity-network-contexts) based on the user's query terms. These contextual networks guide both retrieval and response generation phases.

#### Entity network generation

The process for generating entity network contexts is as follows:

  1. **Initial entity discovery** Match query terms to entities using various search methods: lookup by id, exact match, partial match, full text search, or any other search technique offered by the graph store.
  2. **Entity prioritization**	Sort matched entities by relevance to the query. Calculate the degree centrality of the top entity: this will be used as a benchmark for subsequent filtering.
  3. **Network expansion** Starting from each root entity node, follow entity-to-entity relationships, expanding to a depth of 2-3 levels.
  4. **Network pruning** Apply filtering based on degree centrality thresholds derived from the benchmark created in step 2. Remove entities above and below these thresholds along each path.
  5. **Path selection** Rerank all valid paths and select the top N highest-ranking paths. These form the final set of entity network contexts.

You can configure entity network generation using the following parameters:

##### `ec_max_depth`

Determines the maximum path depth in entity networks. 

This value also controls the entity count per level according to depth. For a depth-2 traversal: 3 entities at depth 1, 2 entities at depth 2. For a depth-3 traversal: 4 entities at depth 1, 3 entities at depth 2, 2 entities at depth 3. 

The default value is `2`.

##### `ec_max_contexts`

Limits the number of entity contexts returned by providers. Note: Multiple entity contexts may originate from the same root entity. The default value is `2`.

##### `ec_max_score_factor`

Filters out entities whose degree centrality exceeds a threshold based on a percentage of the degree centrality of the top entity. The default value is `3` (300% of the top entity's score).

##### `ec_min_score_factor`

Filters out entities whose degree centrality falls below a threshold based on a percentage of the degree centrality of the top entity. The default value is `0.25` (25% of the top entity's score).

#### Example

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    ec_max_depth=3,
    ec_max_contexts=3
)
```

#### When to adjust entity network generation

The entity network context settings control how extensively the system searches for related content and how it filters results based on entity relationships. Increase the search scope to find structurally relevant but dissimilar content. Reduce the search scope to focus on content similar to the query.

A **broad but shallow search** – e.g. `ec_max_depth=1` and `ec_max_contexts=5` – helps explore diverse contexts focused on direct matches to the query. 

A **deep but narrow search** – e.g. `ec_max_depth=3` and `ec_max_contexts=2` – helps explore distantly related content through key entities.

The `ec_max_score_factor` and `ec_min_score_factor` parameters allow you to filter out 'whales' and 'minnows' in proportion to the significance of the top entity. 

`ec_max_score_factor` controls how prominently high-scoring distant entities appear in the search results. Higher values will include well-connected entities even if they're distantly related. Increase `ec_max_score_factor` when you want to see important entities that aren't directly connected.

`ec_min_score_factor` controls the inclusion of less significant distant entities. Lower values will result in the inclusion of rarely mentioned entities even if they're distantly related. Decrease `ec_min_score_factor` to find niche or uncommon connections.
