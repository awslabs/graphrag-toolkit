## Lexical Graph

The lexical-graph package provides a framework for automating the construction of a [hierarchical lexical graph](../docs/lexical-graph/graph-model.md) from unstructured data, and composing question-answering strategies that query this graph when answering user questions. 

### Features

  - Built-in graph store support for [Amazon Neptune Analytics](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html) and [Amazon Neptune Database](https://docs.aws.amazon.com/neptune/latest/userguide/intro.html) 
  - Built-in vector store support for Neptune Analytics, [Amazon OpenSearch Serverless](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless.html) and Postgres with the pgvector extension.
  - Built-in support for foundation models (LLMs and embedding models) on [Amazon Bedrock](https://docs.aws.amazon.com/bedrock/).
  - Easily extended to support additional graph and vector stores and model backends.
  - [Multi-tenancy](../docs/lexical-graph/multi-tenancy.md) – multiple separate lexical graphs in the same underlying graph and vector stores.
  - Continuous ingest and [batch extraction](../docs/lexical-graph/batch-extraction.md) (using [Bedrock batch inference](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html)) modes.
  - Quickstart [AWS CloudFormation templates](../examples/lexical-graph/cloudformation-templates/) for Neptune Database, OpenSearch Serverless, and Amazon Aurora Postgres.

## Installation

The lexical-graph requires Python and [pip](http://www.pip-installer.org/en/latest/) to install. You can install the lexical-graph using pip:

```
$ pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.2.0.zip#subdirectory=lexical-graph
```

If you're running on AWS, you must run your application in an AWS region containing the Amazon Bedrock foundation models used by the lexical graph (see the [configuration](../docs/lexical-graph/configuration.md#graphragconfig) section in the documentation for details on the default models used), and must [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to these models before running any part of the solution.

### Additional dependencies

You will need to install additional dependencies for specific vector store backends:

#### Amazon OpenSearch Serverless

```
$ pip install opensearch-py llama-index-vector-stores-opensearch
```

#### Postgres with pgvector

```
$ pip install psycopg2-binary pgvector
```

### Supported Python versions

The lexical-graph requires Python 3.10 or greater.

## Example of use

### Indexing

```python
import os

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

from llama_index.readers.web import SimpleWebPageReader

import nest_asyncio
nest_asyncio.apply()

def run_extract_and_build():

    graph_store = GraphStoreFactory.for_graph_store(
        'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
    )
    
    vector_store = VectorStoreFactory.for_vector_store(
        'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
    )

    graph_index = LexicalGraphIndex(
        graph_store, 
        vector_store
    )

    doc_urls = [
        'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
        'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
        'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
        'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
    ]

    docs = SimpleWebPageReader(
        html_to_text=True,
        metadata_fn=lambda url:{'url': url}
    ).load_data(doc_urls)

    graph_index.extract_and_build(docs, show_progress=True)

if __name__ == '__main__':
    run_extract_and_build()
```

### Querying

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

import nest_asyncio
nest_asyncio.apply()

def run_query():

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
  
  response = query_engine.query('''What are the differences between Neptune Database 
                                   and Neptune Analytics?''')
  
  print(response.response)
  
if __name__ == '__main__':
    run_query()
```

## Documentation

  - [Overview](../docs/lexical-graph/overview.md)
  - [Storage Model](../docs/lexical-graph/storage-model.md) 
  - [Indexing](../docs/lexical-graph/indexing.md) 
  - [Batch Extraction](../docs/lexical-graph/batch-extraction.md) 
  - [Querying](../docs/lexical-graph/querying.md) 
  - [Multi-Tenancy](../docs/lexical-graph/multi-tenancy.md) 
  - [Configuration](../docs/lexical-graph/configuration.md) 
  - [Graph Model](../docs/lexical-graph/graph-model.md)
  - [Security](../docs/lexical-graph/security.md)
  - [FAQ](../docs/lexical-graph/faq.md)


## License

This project is licensed under the Apache-2.0 License.

