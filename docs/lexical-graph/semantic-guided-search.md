## Semantic-Guided Search

The recommended method for query and retrieval is to used the [traversal-based search](./traversal-based-search.md) operation. The lexical-graph does, however, also currently support semantic-guided search, but this approach has several drawbacks:

  - High storage costs due to requiring an embedding for each statement
  - Poor performance with large datasets, with queries often taking minutes to complete
  - Expected to be removed in future releases

This page contains the semantic-guided search documentation.

### Example

The following example uses semantic-guided search with all the default settings to query the graph:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

with (
    GraphStoreFactory.for_graph_store(
        'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
    ) as graph_store,
    VectorStoreFactory.for_vector_store(
        'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
    ) as vector_store
):

    query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
        graph_store, 
        vector_store,
        streaming=True
    )

    response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.print_response_stream())
```

By default, semantic-guided search uses a composite search strategy using three subretrievers:

  - `StatementCosineSimilaritySearch` – Gets the top k statements using cosine similarity of statement embeddings to the query embedding.
  - `KeywordRankingSearch` – Gets the top k statements based on the number of matches to a specified number of keywords and synonyms extracted from the query. Statements with more keyword matches rank higher in the results.
  - `SemanticBeamGraphSearch` – A statement-based search that finds a statement's neighbouring statements based on shared entities, and retains the most promising based on the cosine similarity of the candidate statements' embeddings to the query embedding. The search is seeded with statements from other retrievers (e.g. `StatementCosineSimilaritySearch` and/or `KeywordRankingSearch`), or from an initial vector similarity search against the statement index.

#### Semantic-guided search results

Semantic-guided search returns one or more search results, each of which comprises a source, and a set of statements:

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

Semantic-guided search behaviour can be configured by configuring individual subretrievers:

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

#### Semantic-guided search with a reranking beam search

Instead of using a `SemanticBeamGraphSearch` with the `SemanticGuidedRetriever`, you can use a `RerankingBeamGraphSearch` instead. Instead of using cosine similarity to determine which candidate statements to pursue, the `RerankingBeamGraphSearch` uses a reranker.

You must initialize a `RerankingBeamGraphSearch` instance with a reranker. The toolkit includes two different rerankers: `BGEReranker`, and `SentenceReranker`. If you're running on a CPU device, we recommend using the `SentenceReranker`. If you're running on a GPU device, you can choose either the `BGEReranker` or `SentenceReranker`.

The example below uses a `SentenceReranker` with a `RerankingBeamGraphSearch` to rerank statements while conducting the beam search:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.retrieval.retrievers import RerankingBeamGraphSearch, StatementCosineSimilaritySearch, KeywordRankingSearch
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker

with (
    GraphStoreFactory.for_graph_store(
        'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
    ) as graph_store,
    VectorStoreFactory.for_vector_store(
        'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
    ) as vector_store
):

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

with (
    GraphStoreFactory.for_graph_store(
        'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
    ) as graph_store,
    VectorStoreFactory.for_vector_store(
        'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
    ) as vector_store
):

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

| Postprocessor  | Description |
| ------------- | ------------- | ------------- |
| `BGEReranker` | Reranks (and limits) results using the `BAAI/bge-reranker-v2-minicpm-layerwise` model before returning them to the query engine. Use only if you have a GPU device. |
| `SentenceReranker` | Reranks (and limits) results using the `mixedbread-ai/mxbai-rerank-xsmall-v1`. model before returning them to the query engine. |
| `StatementDiversityPostProcessor` | Removes similar statements from the results using TF-IDF similarity. Before running `StatementDiversityPostProcessor` for the first time, load the following package: `python -m spacy download en_core_web_sm` |
| `StatementEnhancementPostProcessor` | Enhances statements by using chunk context and an LLM to improve content while preserving original metadata. (Requires an LLM call per statement.) |

The example below uses a `StatementDiversityPostProcessor`, `SentenceReranker` and `StatementEnhancementPostProcessor`. If you're running on a GPU device, you can replace the `SentenceReranker` with a `BGEReranker`.

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker, SentenceReranker, StatementDiversityPostProcessor, StatementEnhancementPostProcessor
import os

with (
    GraphStoreFactory.for_graph_store(
        'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
    ) as graph_store,
    VectorStoreFactory.for_vector_store(
        'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
    ) as vector_store
):

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