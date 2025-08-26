[[Home](./)]

## Traversal-Based Search

### Topics

  - [Overview](#overview)
  - [Example](#example)
  - [Basic concepts](#basic-concepts)
    - [Connectivity types](#connectivity-types)
    - [Entity network contexts](#entity-network-contexts)
  - [Retrievers](#retrievers)
  - [Search results](#search-results)

### Overview

The recommended method for query and retrieval is to used the traversal-based search operation. While the lexical-graph does include support for semantic-guided search, this alternative approach has several significant drawbacks:

  - High storage costs due to requiring an embedding for each statement
  - Poor performance with large datasets, with queries often taking minutes to complete
  - Expected to be removed in future releases

For optimal results, users should use traversal-based search in their applications.

Traversal-based search can be used in two ways: retrieval and querying. When you perform a retrieval operation, the system searches the graph and vector stores to find the most relevant information related to your query. It then returns these raw search results directly to you. With a query operation, the system takes an extra step. After finding the relevant information, it passes these results to a Large Language Model (LLM). The LLM processes this information and generates a natural language response that answers your query.

### Example

The following example performs a traversal-based search using the default settings:

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

    query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
        graph_store, 
        vector_store,
        streaming=True
    )

    response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

print(response.print_response_stream())
```

The parameters used to configure traversal-based search are described in [Configuring and Tuning Traversal-Based Search](./configuring-and-tuning-traversal-based-search.md).

### Basic concepts

Traversal-based search is a method that employs one or more retrievers to locate information within a lexical graph. This approach leverages two key features of the lexical graph structure: connectivity (both local and global) and entity network contexts.

#### Connectivity types

The lexical graph provides both local and global connectivity:

  - *Local Connectivity* Local connectivity enables traversal within a localized network, typically within a single source. This is primarily facilitated by topics, which connect relevant chunks of information within the same source material.
  - *Global Connectivity* Global connectivity allows navigation to related components that may be more distant in the graph structure. This is achieved through facts, which create connections across different sources.

Different retrievers emphasize these connectivity types in varying ways:

  - The `ChunkBasedSearch` retriever primarily utilizes local connectivity
  - The `EntityBasedSearch` retriever focuses more on global connectivity
	- The `EntityNetworkSearch` retriever balances local and global connectivity

#### Entity network contexts

An entity network context consists of a filtered and ranked network of entities that relate to search terms found in the user's query. These contexts serve multiple important functions:

  - *Search Initialization* Provides starting points for entity-based searches in the `EntityBasedSearch` retriever
  - *Similarity Searching* Entity network transcriptions – textual representations of the entity network contexts – help find content that differs from but relates to the original query in the `EntityNetworkSearch` retriever
  - *Reranking* Entity network transcriptions can be used to enhance the original search terms when reranking statements in search results
  - *LLM Integration* Entity network transcriptions can also be provided to Large Language Models (LLMs) during query operations to help focus responses on the most relevant search results

### Retrievers

Traversal-based search provides three different retrievers:

  - The `ChunkBasedSearch` retriever uses a vector similarity search to find information that is similar to the original query. The retriever first finds relevant chunks using vector similarity search. From these chunks, the retriever traverses topics, statements, and facts. Chunk-based search tends to return a narrowly-scoped set of results based on the statement and fact neighbourhoods of chunks that match the original query.
	- The `EntityBasedSearch` retriever uses as its starting points the entities in an entity network context. From these entities, the retriever traverses facts, statements and topics. Entity-based search tends to return a broadly-scoped set of results, based on the neighbourhoods of individual entities and the facts that connect entities.
	- The `EntityNetworkSearch` retriever uses textual transcriptions of an entity network context to drive vector searches for information that is dissimilar to the original query but nonetheless structurally relevant for creating an accurate and full response. These vector searches return chunks that are similar to 'something different from the question being asked'. From these chunks, the retriever traverses topics, statements, and facts to explore the structurally relevant space of dissimilar content.
	
By default, the traversal-based search is configured to use a combination of `ChunkBasedSearch` and `EntityNetworkSearch`. Together, these two retrievers provide access to content that is similar to the question being asked, plus content that is similar to 'something different from the question being asked'.

### Search results

When used with traversal-based search, the `retrieve()` operation of the `LexicalGraphQueryEngine` returns a collection of LlamaIndex scored nodes (`NodeWithScore`). Each node contains a single search result, comprising a source, topic, and a set of statements. For example,

```python
response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")

for n in response.source_nodes:
    print(n.text)
```

 – returns the following output:

```
{
  "source": "https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html",
  "topic": "Neptune Analytics Features",
  "statements": [
    "Neptune Analytics allows loading graph data from a Neptune Database endpoint.",
    "Neptune Analytics enables running graph analytics queries.",
    "Neptune Analytics allows loading graph data from Amazon S3.",
    "Neptune Analytics supports custom graph queries.",
    "Neptune Analytics supports pre-built graph queries."
  ]
}
{
  ...
}
```

The `metadata` property of each node contains a dictionary with a far more detailed breakdown of the search result. This includes the score for each statement, the facts that support each statement, the retrievers used to fetch each statement, and the entity network contexts used in the query. For example, 

```python
import json
for n in response.source_nodes:
    print(json.dumps(n.metadata, indent=2))
```

 – returns the following output:

```
{
  "result": {
    "source": {
      "sourceId": "aws::4510583f:e412",
      "metadata": {
        "url": "https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html"
      }
    },
    "topics": [
      {
        "topic": "Neptune Analytics Features",
        "topicId": "fbbde2f69acd195da90e578d0f9eeefe",
        "statements": [
          {
            "statementId": "810a8ac6943708e1584662b32431eb67",
            "statement": "Neptune Analytics allows loading graph data from a Neptune Database endpoint.",
            "facts": [
              "Neptune Analytics FEATURE loading graph data",
              "Neptune Analytics SUPPORTS LOADING FROM Neptune Database"
            ],
            "details": "",
            "chunkId": "aws::4510583f:e412:9f69cb6f",
            "score": 0.3187,
            "statement_str": "Neptune Analytics allows loading graph data from a Neptune Database endpoint. (details: Neptune Analytics FEATURE loading graph data, Neptune Analytics SUPPORTS LOADING FROM Neptune Database)",
            "retrievers": [
              "ChunkBasedSearch (3.12.0)"
            ]
          },
          {
            "statementId": "797021c7c33db8674fa0be42a1cdd9a6",
            "statement": "Neptune Analytics enables running graph analytics queries.",
            "facts": [
              "Neptune Analytics FEATURE running graph analytics queries"
            ],
            "details": "",
            "chunkId": "aws::4510583f:e412:9f69cb6f",
            "score": 0.2233,
            "statement_str": "Neptune Analytics enables running graph analytics queries. (details: Neptune Analytics FEATURE running graph analytics queries)",
            "retrievers": [
              "ChunkBasedSearch (3.12.0)"
            ]
          },
          {
            "statementId": "23deac383344021ed50e1c78448408a8",
            "statement": "Neptune Analytics allows loading graph data from Amazon S3.",
            "facts": [
              "Neptune Analytics FEATURE loading graph data",
              "Neptune Analytics SUPPORTS LOADING FROM Amazon S3"
            ],
            "details": "",
            "chunkId": "aws::4510583f:e412:9f69cb6f",
            "score": 0.2197,
            "statement_str": "Neptune Analytics allows loading graph data from Amazon S3. (details: Neptune Analytics FEATURE loading graph data, Neptune Analytics SUPPORTS LOADING FROM Amazon S3)",
            "retrievers": [
              "ChunkBasedSearch (3.12.0)"
            ]
          },
          {
            "statementId": "85a4ea712a9a83fb4ac7f441be72e694",
            "statement": "Neptune Analytics supports custom graph queries.",
            "facts": [
              "Neptune Analytics FEATURE custom graph queries"
            ],
            "details": "",
            "chunkId": "aws::4510583f:e412:9f69cb6f",
            "score": 0.199,
            "statement_str": "Neptune Analytics supports custom graph queries. (details: Neptune Analytics FEATURE custom graph queries)",
            "retrievers": [
              "ChunkBasedSearch (3.12.0)"
            ]
          },
          {
            "statementId": "3a480d6a686748a628009de3cd8238ed",
            "statement": "Neptune Analytics supports pre-built graph queries.",
            "facts": [
              "Neptune Analytics FEATURE pre-built graph queries"
            ],
            "details": "",
            "chunkId": "aws::4510583f:e412:9f69cb6f",
            "score": 0.1857,
            "statement_str": "Neptune Analytics supports pre-built graph queries. (details: Neptune Analytics FEATURE pre-built graph queries)",
            "retrievers": [
              "ChunkBasedSearch (3.12.0)"
            ]
          }
        ]
      }
    ]
  },
  "entity_contexts": {
    "contexts": [
      {
        "entities": [
          {
            "entity": {
              "entityId": "19ad98dc563a3a3c935d93723d3c9029",
              "value": "Neptune Analytics",
              "classification": "Software"
            },
            "score": 37.0,
            "reranking_score": 0.5025
          },
          {
            "entity": {
              "entityId": "ecc28e0aba278f8803bfbc5ae162831a",
              "value": "Neptune",
              "classification": "Software"
            },
            "score": 10.0,
            "reranking_score": 0.0
          }
        ]
      },
      {
        "entities": [
          {
            "entity": {
              "entityId": "51874c430e9cb1f5b09d790049d5380d",
              "value": "Neptune Database",
              "classification": "Software"
            },
            "score": 5.0,
            "reranking_score": 0.5025
          }
        ]
      }
    ]
  }
}
{
  ...
}
```