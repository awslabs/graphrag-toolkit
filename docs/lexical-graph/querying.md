[[Home](./)]

## Querying

### Topics

  - [Overview](#overview)
  - [TraversalBasedRetriever](#traversalbasedretriever)
    - [TraversalBasedRetriever results](#traversalbasedretriever-results)
    - [Configuring the TraversalBasedRetriever](#configuring-the-traversalbasedretriever)
    - [Statement reranking](#statement-reranking) 
  - [SemanticGuidedRetriever](#semanticguidedretriever)
    - [SemanticGuidedRetriever results](#semanticguidedretriever-results)
    - [Configuring the SemanticGuidedRetriever](#configuring-the-semanticguidedretriever)
    - [SemanticGuidedRetriever with a reranking beam search](#semanticguidedretriever-with-a-reranking-beam-search)
  - [Postprocessors](#postprocessors)
  
### Overview   

For the graphrag-toolkit, the primary unit of context presented to the LLM is the *statement*, which is a standalone assertion or proposition. Source documents are broken into chunks, and from these chunks are extracted statements. In the graphrag-toolkit's [graph model](./graph-model.md), statements are thematically grouped by topic, and supported by facts. At question-answering time, the lexical-graph retrieves groups of statements, and presents them in the context window to the LLM.

The lexical-graph contains two different retrievers: a `TraversalBasedRetriever`, and a `SemanticGuidedRetriever`. The `TraversalBasedRetriever` uses a combination of 'top down' search – finding chunks through vector similarity search, and then traversing from these chunks through topics to statements and facts – and 'bottom up' search, which performs keyword-based lookups of entities, and proceeds through facts to statements and topics. The `SemanticGuidedRetriever` integrates vector-based semantic search with structured graph traversal. It uses semantic and keyword-based searches to identify entry points, then intelligently explores the graph through beam search and path analysis, while employing reranking and diversity filtering to ensure quality results. This hybrid approach enables both precise matching and contextual exploration.

The `SemanticGuidedRetriever` uses a *statement* vector index, whereas the `TraversalBasedRetriever` uses a *chunk* vector index. The chunk vector index is much smaller than the statement index.

In the current release the output formats of the two retrievers are different. Future releases of the lexical-graph will seek to align the outputs.

> The [FalkorDB graph store](./storage-model.md#falkordb-graph-store) currently supports the `SemanticGuidedRetriever`. It does not support the [`TraversalBasedRetriever`](./querying.md#traversalbasedretriever)`.

Querying supports [multi-tenancy](multi-tenancy.md), whereby you can query different lexical graphs hosted in the same backend graph and vector stores.

#### Code examples

The code examples here are formatted to run in a Jupyter notebook. If you’re building an application with a main entry point, put your application logic inside a method, and add an [`if __name__ == '__main__'` block](./faq.md#runtimeerror-please-use-nest_asyncioapply-to-allow-nested-event-loops).

### TraversalBasedRetriever

The following example uses a `TraversalBasedRetriever` with all its default settings to query the graph:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

import nest_asyncio
nest_asyncio.apply()

graph_store = GraphStoreFactory.for_graph_store(
    'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
)

vector_store = VectorStoreFactory.for_vector_store(
    'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
)

query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store
)

response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.response)
```

By default, the `TraversalBasedRetriever` uses a composite search strategy using two subretrievers:

  - `EntityBasedSearch` – This retriever extracts keywords from the query and then looks up entities in the graph based on these keywords. From the entities, the retriever traverses facts, statements and topics. Entity-based search tends to return a broadly-scoped set of results, based on the neighbourhoods of individual entities and the facts that connect entities.
  - `ChunkBasedSearch` – This retriever finds chunks using vector similarity search. From the chunks, the retriever traverses topics, statements, and facts. Chunk-based search tends to return a narrowly-scoped set of results based on the statement and fact neighbourhoods of chunks that match the original query.
  
When combining these two retrievers, the `TraversalBasedRetriever` weights their contributions in favour of the chunk-based search: the chunk search provides a foundation of similarity-based results, which are then augmented by the broader-ranging entity-based results.

To configure the `TraversalBasedRetriever` with just one or other of these subretrievers, you can pass an instance of the subretriever, or the type of subretriever, to the factory method:

```python
from graphrag_toolkit.lexical_graph.retrieval.retrievers import ChunkBasedSearch

...

query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    retrievers=[ChunkBasedSearch]
)
```

#### TraversalBasedRetriever results

The `TraversalBasedRetriever` returns one or more search results, each of which comprises a source, topic, a set of statements, and a score:

```
{
  "source": "https://docs.aws.amazon.com/neptune/latest/userguide/intro.html",
  "topic": "Amazon Neptune",
  "statements": [
    "Amazon Neptune provides high performance for Property Graph (PG) model",
    "You can access Property Graph model in Amazon Neptune using openCypher query language",
    "You can access Property Graph model in Amazon Neptune using Gremlin query language",
    "Amazon Neptune provides high performance for Resource Description Framework (RDF) model",
    "Amazon Neptune is a fully managed graph database service",
    "Before designing a database, consult the GitHub repository \"AWS Reference Architectures for Using Graph Databases\" to inform choices about graph data models and query languages and browse examples of reference deployment architectures",
    "With Amazon Neptune, you can use graph query languages Gremlin, openCypher, and SPARQL",
    "You can access Resource Description Framework model in Amazon Neptune using SPARQL query language",
    "Neptune supports the Neo4j's openCypher property-graph query language",
    "To learn more about using Amazon Neptune, start with the section \"Overview of Amazon Neptune features\""
  ],
  "score": 0.53
}

...

{
  "source": "https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html",
  "topic": "Neptune Analytics",
  "statements": [
    "Neptune Analytics is a solution for quickly analyzing existing graph databases",
    "Neptune Analytics is a solution for quickly analyzing graph datasets stored in a data lake",
    "Neptune Analytics uses popular graph analytic algorithms",
    "Processing thousands of analytic queries per second using popular graph analytics algorithms becomes possible with Neptune Analytics",
    "Neptune Analytics makes it easy to apply powerful algorithms to the data in your Neptune Database",
    "Neptune Analytics provides a simple API for analyzing graph data",
    "You can use Neptune Analytics to analyze and query graphs in data science workflows that assist with fraud investigations",
    "You can use Neptune Analytics to analyze and query graphs in data science workflows that detect network threats",
    "Neptune Analytics uses low-latency analytic queries",
    "Neptune Analytics provides a simple API for loading graph data"
  ],
  "score": 0.22
}
```

#### Configuring the TraversalBasedRetriever

You can configure the `TraversalBasedRetriever` by passing named arguments to the `LexicalGraphQueryEngine` factory method, or to the retriever constructor.

| Parameter  | Description | Default Value |
| ------------- | ------------- | ------------- | 
| `max_search_results` | The maximum number of search results to return. A search result comprises one or more statements belonging to the same topic (and source). If set to `None`, all found search results will be returned. | `20` |
| `max_statements_per_topic` | The mazimum number of statements to include with a topic. Limits the size of each search result. If set to `None`, all the statements belonging to the topic that are found as part of the search will be included in the result. | `10` |
| `include_facts` | Determines whether facts, as well as statements, are included in the context returned to the LLM. | `False` |
| `expand_entities` | Used by `EntityBasedSearch` when identifying candidate entities to anchor the traversal. If set to `True`, the retriever considers not only keyword lookup entities, but also additional entities transitively connected to the keyword lookup entities. | `True` |
| `max_keywords` | Used by `EntityBasedSearch` when extracting keywords from the query. When extracting keywords, the retriever attempts to include alternative names, synonyms, abbreviations, and the definitions for any acronyms it recognizes. These all count towards the keyword limit. | `10` |
| `vss_top_k` | Used by `ChunkBasedSearch` when identifying candidate chunks to anchor the traversal. | `10` |
| `vss_diversity_factor` | Used by `ChunkBasedSearch` to identify the most relevant chunks across the broadest range of sources. The retriever does this by looking up `vss_top_k * vss_diversity_factor` chunks, and then iterating through the results looking for the next most relevant result from a previously unseen source until it has satisfied its `vss_top_k` quota. If set to `None`, `vss_top_k` chunks will be returned and used in order. | `5` |
| `derive_subqueries` | Used by `TraversalBasedRetriever`. If set to `True` the retriever will attempt to break complex queries into multiple, simpler queries. Candidates for query decomposition must be longer than 25 characters, and answering the original query must require details of more than one entity. | `False` |
| `max_subqueries` | The maximum number of subqueries into which a complex query will be decomposed. | `2` |
| `reranker` | Prior to returning the results to the query engine for post-processing, the retriever can rerank them based on a reranking of all statements in the results. Valid options here are `tfidf`, `model`, `none`, and the Python `None` keyword. See [Statement reranking](#statement-reranking) below for details. | `tfidf` |
| `max_statements` | Used by the traversal-based reranking strategy. Limits the number of reranked statements across the entire resultset to `max_statements`. If set to `None`, *all* the statements in the resultset will be reranked and returned to the query engine. | `100` |

The example below shows how to configure the `TraversalBasedRetriever` to return ten results:

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store, 
    vector_store,
    max_search_results=10
)
```

#### Statement reranking

At the end of the retrieval process, but prior to returning the results to the query engine for post-processing, the `TraversalBasedRetriever` can rerank all the statements in the results. There are two strategies available for doing this. If you set the `reranker` parameter to `model`, the retriever will use a `SentenceReranker` to rerank all the statements in the resultset. If you set the `reranker` parameter to `tfidf` (the default), the retriever uses a *term frequency-inverse document frequency* (TF-IDF) measure to rank all the statements in the resultset. `tfidf` tends to be faster than `model`. You can also turn off the reranking feature by setting `reranker` to `none`.

Besides reranking statements, you can also specify the `max_statements` to be returned by the reranker. This will truncate the number of statements subsequently passed to the query engine for post-processsing. `max_statements` is only applied if `reranker` has been set to `model` or `tfidf`.

This traversal-based reranking is performed on a per-statement basis. To facilitate the reranking, each statement is enriched with its topic and source metadata. This composite lexical unit is then reranked against a composite of the original query plus any entity names found in the keyword lookup step. 

You can use the traversal-based reranking *in combination* with any reranking applied during post-processing. Reranking in the post-processing stage will rerank *results* (i.e. collections of statements), whereas traversal-based reranking reranks individual *statements*. 

### SemanticGuidedRetriever

The following example uses a `SemanticGuidedRetriever` with all its default settings to query the graph:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

import nest_asyncio
nest_asyncio.apply()

graph_store = GraphStoreFactory.for_graph_store(
    'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
)

vector_store = VectorStoreFactory.for_vector_store(
    'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
)

query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
    graph_store, 
    vector_store
)

response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.response)
```

By default, the `SemanticGuidedRetriever` uses a composite search strategy using three subretrievers:

  - `StatementCosineSimilaritySearch` – Gets the top k statements using cosine similarity of statement embeddings to the query embedding.
  - `KeywordRankingSearch` – Gets the top k statements based on the number of matches to a specified number of keywords and synonyms extracted from the query. Statements with more keyword matches rank higher in the results.
  - `SemanticBeamGraphSearch` – A statement-based search that finds a statement's neighbouring statements based on shared entities, and retains the most promising based on the cosine similarity of the candidate statements' embeddings to the query embedding. The search is seeded with statements from other retrievers (e.g. `StatementCosineSimilaritySearch` and/or `KeywordRankingSearch`), or from an initial vector similarity search against the statement index.

#### SemanticGuidedRetriever results

The `SemanticGuidedRetriever` returns one or more search results, each of which comprises a source, and a set of statements:

```
<source_1>
<source_1_metadata>
	<url>https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html</url>
</source_1_metadata>
<statement_1.1>Neptune Database is a serverless graph database</statement_1.1>
<statement_1.2>Neptune Analytics is an analytics database engine</statement_1.2>
<statement_1.3>Neptune Analytics is a solution for quickly analyzing existing graph databases</statement_1.3>
<statement_1.4>Neptune Database provides a solution for graph database workloads that need Multi-AZ high availability</statement_1.4>
<statement_1.5>Neptune Analytics is a solution for quickly analyzing graph datasets stored in a data lake (details: Graph datasets LOCATION data lake)</statement_1.5>
<statement_1.6>Neptune Database provides a solution for graph database workloads that need to scale to 100,000 queries per second</statement_1.6>
<statement_1.7>Neptune Database is designed for optimal scalability</statement_1.7>
<statement_1.8>Neptune Database provides a solution for graph database workloads that need multi-Region deployments</statement_1.8>
<statement_1.9>Neptune Analytics removes the overhead of managing complex data-analytics pipelines (details: Overhead CONTEXT managing complex data-analytics pipelines)</statement_1.9>
...
</source_1>

...

<source_4>
<source_4_metadata>
	<url>https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html</url>
</source_4_metadata>
<statement_4.1>Neptune Analytics allows performing business intelligence queries using openCypher language</statement_4.1>
<statement_4.2>The text distinguishes between Neptune Analytics and Neptune Database</statement_4.2>
<statement_4.3>Neptune Analytics allows performing custom analytical queries using openCypher language</statement_4.3>
<statement_4.4>Neptune Analytics allows performing in-database analytics on large graphs</statement_4.4>
<statement_4.5>Neptune Analytics allows focusing on queries and workflows to solve problems</statement_4.5>
<statement_4.6>Neptune Analytics can load data extremely fast into memory</statement_4.6>
<statement_4.7>Neptune Analytics allows running graph analytics queries using pre-built or custom graph queries</statement_4.7>
<statement_4.8>Neptune Analytics manages graphs instead of infrastructure</statement_4.8>
<statement_4.9>Neptune Analytics allows loading graph data from Amazon S3 or a Neptune Database endpoint</statement_4.9>
...
</source_4>
```

#### Configuring the SemanticGuidedRetriever

The `SemanticGuidedRetriever` behaviour can be configured by copnfiguring individual subretrievers:

| Retriever  | Parameter  | Description | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| `StatementCosineSimilaritySearch` | `top_k` | Number of statements to include in the results | `100` |
| `KeywordRankingSearch` | `top_k` | Number of statements to include in the results | `100` |
|| `max_keywords` | The maximum number of keywords to extract from the query | `10` |
| `SemanticBeamGraphSearch` | `max_depth` | The maximum depth to follow promising candidates from the starting statements | `3` |
|| `beam_width` | The number of most promising candidates to return for each statement that is expanded | `10` |
| `RerankingBeamGraphSearch` | `max_depth` | The maximum depth to follow promising candidates from the starting statements | `3` |
|| `beam_width` | The number of most promising candidates to return for each statement that is expanded | `10` |
|| `reranker` | Reranker instance that will be used to rerank statements (see below) | `None` 
|| `initial_retrievers` | List of retrievers used to see the starting statements (see below) | `None` |

#### SemanticGuidedRetriever with a reranking beam search

Instead of using a `SemanticBeamGraphSearch` with the `SemanticGuidedRetriever`, you can use a `RerankingBeamGraphSearch` instead. Instead of using cosine similarity to determine which candidate statements to pursue, the `RerankingBeamGraphSearch` uses a reranker.

You must initialize a `RerankingBeamGraphSearch` instance with a reranker. The toolkit includes two different rerankers: `BGEReranker`, and `SentenceReranker`. If you're running on a CPU device, we recommend using the `SentenceReranker`. If you're running on a GPU device, you can choose either the `BGEReranker` or `SentenceReranker`.

The example below uses a `SentenceReranker` with a `RerankingBeamGraphSearch` to rerank statements while conducting the beam search:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.retrieval.retrievers import RerankingBeamGraphSearch, StatementCosineSimilaritySearch, KeywordRankingSearch
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker

import nest_asyncio
nest_asyncio.apply()

graph_store = GraphStoreFactory.for_graph_store(
    'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
)

vector_store = VectorStoreFactory.for_vector_store(
    'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
)

cosine_retriever = StatementCosineSimilaritySearch(
    vector_store=vector_store,
    graph_store=graph_store,
    top_k=50
)

keyword_retriever = KeywordRankingSearch(
    vector_store=vector_store,
    graph_store=graph_store,
    max_keywords=10
)

reranker = SentenceReranker(
    batch_size=128
)

beam_retriever = RerankingBeamGraphSearch(
    vector_store=vector_store,
    graph_store=graph_store,
    reranker=reranker,
    initial_retrievers=[cosine_retriever, keyword_retriever],
    max_depth=8,
    beam_width=100
)

query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
    graph_store, 
    vector_store,
    retrievers=[
        cosine_retriever,
        keyword_retriever,
        beam_retriever
    ]
)

response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.response)
```

The example below uses a `BGEReranker` with a `RerankingBeamGraphSearch` to rerank statements while conducting the beam search.

There will be a delay the first time this runs while the reranker downloads tensors.

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.retrieval.retrievers import RerankingBeamGraphSearch, StatementCosineSimilaritySearch, KeywordRankingSearch
from graphrag_toolkit.lexical_graph.retrieval.post_processors import BGEReranker

import nest_asyncio
nest_asyncio.apply()

graph_store = GraphStoreFactory.for_graph_store(
    'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
)

vector_store = VectorStoreFactory.for_vector_store(
    'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
)

cosine_retriever = StatementCosineSimilaritySearch(
    vector_store=vector_store,
    graph_store=graph_store,
    top_k=50
)

keyword_retriever = KeywordRankingSearch(
    vector_store=vector_store,
    graph_store=graph_store,
    max_keywords=10
)

reranker = BGEReranker(
    gpu_id=0, # Remove if running on CPU device,
    batch_size=128
)

beam_retriever = RerankingBeamGraphSearch(
    vector_store=vector_store,
    graph_store=graph_store,
    reranker=reranker,
    initial_retrievers=[cosine_retriever, keyword_retriever],
    max_depth=8,
    beam_width=100
)

query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
    graph_store, 
    vector_store,
    retrievers=[
        cosine_retriever,
        keyword_retriever,
        beam_retriever
    ]
)

response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.response)
```

### Postprocessors

There are a number of postprocessors you can use to further improve and format results:

| Postprocessor  | Use With  | Description |
| ------------- | ------------- | ------------- |
| `BGEReranker` | `TraversalBasedRetriever` or `SemanticGuidedRetriever` | Reranks (and limits) results using the `BAAI/bge-reranker-v2-minicpm-layerwise` model before returning them to the query engine. Use only if you have a GPU device. |
| `SentenceReranker` | `TraversalBasedRetriever` or `SemanticGuidedRetriever` | Reranks (and limits) results using the `mixedbread-ai/mxbai-rerank-xsmall-v1`. model before returning them to the query engine. |
| `StatementDiversityPostProcessor` | `TraversalBasedRetriever` or `SemanticGuidedRetriever` | Removes similar statements from the results using TF-IDF similarity. Before running `StatementDiversityPostProcessor` for the first time, load the following package: `python -m spacy download en_core_web_sm` |
| `EnrichSourceDetails` | `TraversalBasedRetriever` | Replaces the `sourceId` in the results with a string composed from source metadata. |
| `StatementEnhancementPostProcessor` | `SemanticGuidedRetriever` | Enhances statements by using chunk context and an LLM to improve content while preserving original metadata. (Requires an LLM call per statement.) |

The example below uses a `StatementDiversityPostProcessor`, `SentenceReranker` and `StatementEnhancementPostProcessor`. If you're running on a GPU device, you can replace the `SentenceReranker` with a `BGEReranker`.

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker, SentenceReranker, StatementDiversityPostProcessor, StatementEnhancementPostProcessor
import os

import nest_asyncio
nest_asyncio.apply()

graph_store = GraphStoreFactory.for_graph_store(
    'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
)

vector_store = VectorStoreFactory.for_vector_store(
    'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
)

query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
    graph_store, 
    vector_store,
    post_processors=[
        SentenceReranker(), 
        StatementDiversityPostProcessor(), 
        StatementEnhancementPostProcessor()
    ]
)

response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.response)
```