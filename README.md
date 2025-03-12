## GraphRAG Toolkit

The graphrag-toolkit is a Python toolkit for building GraphRAG applications. It provides a framework for automating the construction of a graph from unstructured data, and composing question-answering strategies that query this graph when answering user questions. 

The toolkit uses low-level [LlamaIndex](https://docs.llamaindex.ai/en/stable/)  components – data connectors, metadata extractors, and transforms – to implement much of the graph construction process. By default, the toolkit uses [Amazon Neptune Analytics](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html) or [Amazon Neptune Database](https://docs.aws.amazon.com/neptune/latest/userguide/intro.html) (engine version 1.4.1.0 or later) for its graph store, and Neptune Analytics or [Amazon OpenSearch Serverless](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless.html) for its vector store, but it also provides extensibility points for adding alternative graph stores and vector stores. The default backend for LLMs and embedding models is [Amazon Bedrock](https://docs.aws.amazon.com/bedrock/); but, as with the stores, the toolkit can be configured for other LLM and embedding model backends using LlamaIndex abstractions.

If you're running on AWS, there's a quick start AWS CloudFormation template in the [examples](./examples) directory. Note that you must run your application in an AWS region containing the Amazon Bedrock foundation models used by the toolkit (see the [configuration](./docs/configuration.md#graphragconfig) section in the documentation for details on the default models used), and must [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to these models before running any part of the solution.

## v2.x

v2.x of the graphrag-toolkit includes a number of breaking changes. The [graph model](./docs/graph-model.md) has changed so that the lexical graph can better co-exist with domain-specific graph data. The `LexicalGraphIndex` [configuration](./docs/indexing.md#configuring-the-extract-and-build-stages) has also changed.

## Installation

The graphrag-toolkit requires Python and [pip](http://www.pip-installer.org/en/latest/) to install. You can install the graphrag-toolkit using pip:

```
$ pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v2.0.1.zip
```

### Supported Python versions

The graphrag-toolkit requires Python 3.10 or greater.

## Example of use

### Indexing

```python
import os

from graphrag_toolkit import LexicalGraphIndex
from graphrag_toolkit.storage import GraphStoreFactory
from graphrag_toolkit.storage import VectorStoreFactory

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
from graphrag_toolkit import LexicalGraphQueryEngine
from graphrag_toolkit.storage import GraphStoreFactory
from graphrag_toolkit.storage import VectorStoreFactory

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

  - [Storage Model](./docs/storage-model.md) 
  - [Indexing](./docs/indexing.md) 
  - [Batch Extraction](./docs/batch-extraction.md) 
  - [Querying](./docs/querying.md) 
  - [Configuration](./docs/configuration.md) 
  - [Graph Model](./docs/graph-model.md)
  - [Security](./docs/security.md)
  - [FAQ](./docs/faq.md)


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

