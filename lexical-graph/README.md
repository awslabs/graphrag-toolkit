## Lexical Graph

The lexical-graph package provides a framework for automating the construction of a [hierarchical lexical graph](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/graph-model.md) from unstructured data, and composing question-answering strategies that query this graph when answering user questions.

### Features

  - Built-in graph store support for [Amazon Neptune Analytics](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html), [Amazon Neptune Database](https://docs.aws.amazon.com/neptune/latest/userguide/intro.html), and [Neo4j](https://neo4j.com/docs/).
  - Built-in vector store support for Neptune Analytics, [Amazon OpenSearch Serverless](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless.html), [Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) and Postgres with the pgvector extension.
  - Built-in support for foundation models (LLMs and embedding models) on [Amazon Bedrock](https://docs.aws.amazon.com/bedrock/).
  - Easily extended to support additional graph and vector stores and model backends.
  - [Multi-tenancy](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/multi-tenancy.md) – multiple separate lexical graphs in the same underlying graph and vector stores.
  - Continuous ingest and [batch extraction](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/batch-extraction.md) (using [Bedrock batch inference](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference.html)) modes.
  - [Versioned updates](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/versioned-updates.md) for updating source documents and querying the state of the graph and vector stores at a point in time.
  - Quickstart [AWS CloudFormation templates](https://github.com/awslabs/graphrag-toolkit/tree/main/examples/lexical-graph/cloudformation-templates/) for Neptune Database, OpenSearch Serverless, and Amazon Aurora Postgres.

## Installation

The lexical-graph requires Python 3.10 or greater and [pip](http://www.pip-installer.org/en/latest/).

Install the latest stable release from PyPI:

```
$ pip install graphrag-lexical-graph
```

To install a specific version from PyPI:

```
$ pip install graphrag-lexical-graph==3.18.3
```

Or install from a release zip file:

```
$ pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/graphrag-lexical-graph/v3.18.3.zip#subdirectory=lexical-graph
```

If you're running on AWS, you must run your application in an AWS region containing the Amazon Bedrock foundation models used by the lexical graph (see the [configuration](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/configuration.md#graphragconfig) section in the documentation for details on the default models used), and must [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to these models before running any part of the solution.

### Additional dependencies

You will need to install additional dependencies for specific graph and vector store backends:

#### Amazon OpenSearch Serverless

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
| OpenSearch Serverless (vector) | `aoss://<url>` |
| Neptune Analytics (vector) | `neptune-graph://<graph-id>` |
| pgvector (vector) | constructed via `PGVectorIndexFactory` |
| S3 Vectors (vector) | constructed via `S3VectorIndexFactory` |
| Dummy / no-op | `None` or any unrecognised string — falls back to `DummyGraphStore` / `DummyVectorIndex` |

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

### Ontology-guided extraction

You can constrain entity and relationship extraction to a user-provided OWL/Turtle ontology. An `OntologySchema` parses the ontology once, computes the `rdfs:subClassOf` closure, and surfaces two complementary behaviors:

  - **Suggestion mode** — the ontology reaches the LLM as prompt guidance. Wire `ontology.as_extraction_schema()` into `ExtractionConfig.from_stages(..., schema=...)` and the existing `LLMTopicExtractionStage` picks up the richer class hierarchy, domain/range, and datatype-property text automatically. No post-extraction filtering runs; the LLM is encouraged, not required, to draw from the declared vocabulary.
  - **Strict mode** — add `OntologyFilterStage(ontology)` after `LLMTopicExtractionStage` in the stage list. Post-extraction, entities whose class is not in the ontology are dropped, and facts whose (predicate, subject class, object class) tuple is not covered by a declared `owl:ObjectProperty` or `owl:DatatypeProperty` are dropped. Subclass closure is honored throughout.

Mode selection is pipeline composition, not configuration — the same `OntologySchema` instance serves both.

`rdflib` is a soft dependency. Callers that never load a Turtle file do not need it installed; install it when you do:

```
$ pip install rdflib
```

#### Suggestion mode

```python
from pathlib import Path

from graphrag_toolkit.lexical_graph import ExtractionConfig, LexicalGraphIndex
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LLMPropositionStage,
    LLMTopicExtractionStage,
    OntologySchema,
)
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

# requires pip install llama-index-readers-web
from llama_index.readers.web import SimpleWebPageReader

def run_extract_and_build_with_ontology():

    ontology = OntologySchema.from_turtle(Path('example_ontology.ttl'))

    extraction_config = ExtractionConfig.from_stages(
        stages=[
            LLMPropositionStage(),
            LLMTopicExtractionStage(),
        ],
        schema=ontology.as_extraction_schema(),
    )

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
            vector_store,
            indexing_config=extraction_config,
        )

        docs = SimpleWebPageReader(
            html_to_text=True,
            metadata_fn=lambda url:{'url': url}
        ).load_data([
            'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
        ])

        graph_index.extract_and_build(docs, show_progress=True)

if __name__ == '__main__':
    run_extract_and_build_with_ontology()
```

#### Strict mode

Append `OntologyFilterStage(ontology)` to the stage list. The rest of the pipeline is unchanged.

```python
from pathlib import Path

from graphrag_toolkit.lexical_graph import ExtractionConfig
from graphrag_toolkit.lexical_graph.indexing.extract import (
    LLMPropositionStage,
    LLMTopicExtractionStage,
    OntologyFilterStage,
    OntologySchema,
)

ontology = OntologySchema.from_turtle(Path('example_ontology.ttl'))

extraction_config = ExtractionConfig.from_stages(
    stages=[
        LLMPropositionStage(),
        LLMTopicExtractionStage(),
        OntologyFilterStage(ontology),  # strict mode
    ],
    schema=ontology.as_extraction_schema(),
)
```

#### Opting out of XSD literal validation

Strict mode validates SPC facts (entity-to-literal) against the XSD datatype declared on the matching `owl:DatatypeProperty`. If the LLM emits correctly-classed facts with mistyped literals (for example `"forty-two"` where `xsd:integer` is declared), those facts are dropped. Pass `validate_datatypes=False` to keep them while preserving every other strict-mode check:

```python
OntologyFilterStage(ontology, validate_datatypes=False)
```

The opt-out disables only the XSD literal check. Subject-class membership and predicate-in-ontology checks still apply, so facts whose predicate is not declared as a `DatatypeProperty`, or whose subject class is outside that property's domain, are still dropped.

## Documentation

  - [Overview](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/overview.md)
  - [Graph Model](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/graph-model.md)
  - [Storage Model](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/storage-model.md)
  - [Indexing](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/indexing.md)
    - [Batch Extraction](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/batch-extraction.md)
    - [Configuring Batch Extraction](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/configuring-batch-extraction.md)
    - [Composable Extraction Pipeline](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/composable-extraction-pipeline-usage.md)
    - [Versioned Updates](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/versioned-updates.md)
  - [Querying](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/querying.md)
    - [Traversal-Based Search](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/traversal-based-search.md)
    - [Traversal-Based Search Configuration](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/traversal-based-search-configuration.md)
  - [Configuration](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/configuration.md)
  - [Security](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/security.md)
  - [FAQ](https://github.com/awslabs/graphrag-toolkit/tree/main/docs/lexical-graph/faq.md)


## Release

Release instructions are found in the [RELEASE.md](https://github.com/awslabs/graphrag-toolkit/tree/main/lexical-graph/RELEASE.md)

## License

This project is licensed under the Apache-2.0 License.
