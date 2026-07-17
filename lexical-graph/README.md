## Lexical Graph

The lexical-graph package provides a framework for automating the construction of a [hierarchical lexical graph](https://awslabs.github.io/graphrag-toolkit/lexical-graph/graph-model/) from unstructured data, and composing question-answering strategies that query this graph when answering user questions.

### Features

  - Built-in graph store support for [Amazon Neptune Analytics](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html), [Amazon Neptune Database](https://docs.aws.amazon.com/neptune/latest/userguide/intro.html), and [Neo4j](https://neo4j.com/docs/).
  - Built-in vector store support for Neptune Analytics, [Amazon OpenSearch Serverless](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless.html), [Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) and Postgres with the pgvector extension.
  - Built-in support for foundation models (LLMs and embedding models) on [Amazon Bedrock](https://docs.aws.amazon.com/bedrock/).
  - Easily extended to support additional graph and vector stores and model backends.
  - [Multi-tenancy](https://awslabs.github.io/graphrag-toolkit/lexical-graph/multi-tenancy/) – multiple separate lexical graphs in the same underlying graph and vector stores.
  - Continuous ingest and [batch extraction](https://awslabs.github.io/graphrag-toolkit/lexical-graph/batch-extraction/) (using [Bedrock batch inference](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html)) modes.
  - [Versioned updates](https://awslabs.github.io/graphrag-toolkit/lexical-graph/versioned-updates/) for updating source documents and querying the state of the graph and vector stores at a point in time.
  - Quickstart [AWS CloudFormation templates](https://github.com/awslabs/graphrag-toolkit/tree/main/examples/lexical-graph/cloudformation-templates/) for Neptune Database, OpenSearch Serverless, and Amazon Aurora Postgres.

## Installation

The lexical-graph requires Python 3.10 or greater and [pip](http://www.pip-installer.org/en/latest/).

Install the latest stable release from PyPI:

```
$ pip install graphrag-lexical-graph
```

To install a specific version from PyPI:

```
$ pip install graphrag-lexical-graph==3.18.5
```

Or install from a release zip file:

```
$ pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/graphrag-lexical-graph/v3.18.5.zip#subdirectory=lexical-graph
```

If you're running on AWS, you must run your application in an AWS region containing the Amazon Bedrock foundation models used by the lexical graph (see the [configuration](https://awslabs.github.io/graphrag-toolkit/lexical-graph/configuration/#graphragconfig) section in the documentation for details on the default models used), and must [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to these models before running any part of the solution.

### Additional dependencies

You will need to install additional dependencies for specific graph and vector store backends:

#### OpenSearch and Amazon OpenSearch Serverless (AOSS)

```bash
$ pip install opensearch-py llama-index-vector-stores-opensearch
```

#### Postgres with pgvector

```bash
$ pip install psycopg2-binary pgvector
```

#### Neo4j

``` bash
$ pip install neo4j
```

### Connection strings

Pass a connection string to `GraphStoreFactory.for_graph_store()` or `VectorStoreFactory.for_vector_store()` to select a backend:

| Store | Connection string |
| --- | --- |
| Neptune Analytics (graph) | `neptune-graph://<graph-id>` |
| Neptune Database (graph) | `neptune-db://<hostname>` or any hostname ending `.neptune.amazonaws.com` |
| Neo4j (graph) | `bolt://`, `bolt+ssc://`, `bolt+s://`, `neo4j://`, `neo4j+ssc://`, or `neo4j+s://` URLs |
| Amazon OpenSearch Serverless / AOSS (vector) | `aoss://<url>` |
| OpenSearch (vector) | `opensearch://<url>` |
| Neptune Analytics (vector) | `neptune-graph://<graph-id>` |
| pgvector (vector) | constructed via `PGVectorIndexFactory` |
| S3 Vectors (vector) | constructed via `S3VectorIndexFactory` |
| Dummy / no-op | `None` or any unrecognised string — falls back to `DummyGraphStore` / `DummyVectorIndex` |

An `opensearch://<url>` endpoint connects with HTTP basic auth from the `OPENSEARCH_USERNAME` / `OPENSEARCH_PASSWORD` environment variables (no auth if unset), over SSL unless the endpoint has an explicit `http://` scheme. `client_kwargs` passed to `VectorStoreFactory.for_vector_store` are forwarded to the [opensearch-py](https://pypi.org/project/opensearch-py/) client. See [the vector store documentation](https://awslabs.github.io/graphrag-toolkit/lexical-graph/vector-store-opensearch-serverless/) for details and limitations.

## Example of use

### Indexing

```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

# requires pip install llama-index-readers-web
from llama_index.readers.web import SimpleWebPageReader

def run_extract_and_build():

    with (
        GraphStoreFactory.for_graph_store(
            'neptune-db://my-graph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com'
        ) as graph_store,
        VectorStoreFactory.for_vector_store(
            'aoss://https://abcdefghijkl.us-east-1.aoss.amazonaws.com'
        ) as vector_store
    ):

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

def run_query():

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
            vector_store
        )

        response = query_engine.query('''What are the differences between Neptune Database
                                         and Neptune Analytics?''')

        print(response.response)

if __name__ == '__main__':
    run_query()
```

## Documentation

  - [Overview](https://awslabs.github.io/graphrag-toolkit/lexical-graph/overview/)
  - [Graph Model](https://awslabs.github.io/graphrag-toolkit/lexical-graph/graph-model/)
  - [Storage Model](https://awslabs.github.io/graphrag-toolkit/lexical-graph/storage-model/)
  - [Indexing](https://awslabs.github.io/graphrag-toolkit/lexical-graph/indexing/)
    - [Batch Extraction](https://awslabs.github.io/graphrag-toolkit/lexical-graph/batch-extraction/)
    - [Configuring Batch Extraction](https://awslabs.github.io/graphrag-toolkit/lexical-graph/configuring-batch-extraction/)
    - [Versioned Updates](https://awslabs.github.io/graphrag-toolkit/lexical-graph/versioned-updates/)
  - [Querying](https://awslabs.github.io/graphrag-toolkit/lexical-graph/querying/)
    - [Traversal-Based Search](https://awslabs.github.io/graphrag-toolkit/lexical-graph/traversal-based-search/)
    - [Traversal-Based Search Configuration](https://awslabs.github.io/graphrag-toolkit/lexical-graph/traversal-based-search-configuration/)
  - [Configuration](https://awslabs.github.io/graphrag-toolkit/lexical-graph/configuration/)
  - [Security](https://awslabs.github.io/graphrag-toolkit/lexical-graph/security/)
  - [FAQ](https://awslabs.github.io/graphrag-toolkit/lexical-graph/faq/)


## Release

Release instructions are found in the [RELEASE.md](https://github.com/awslabs/graphrag-toolkit/tree/main/lexical-graph/RELEASE.md)

## License

This project is licensed under the Apache-2.0 License.
