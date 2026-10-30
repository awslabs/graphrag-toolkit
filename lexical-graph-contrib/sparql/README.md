# RDF / SPARQL graph store

This contributor package stores the AWS GraphRAG Toolkit lexical graph in an
existing SPARQL 1.1 query/update endpoint. It is endpoint-neutral by default and
has an optional Amazon Neptune IAM transport.

Core graph builders and retrievers identify a backend-neutral `GraphQueryOperation`.
Property-graph stores execute their native queries, while this package provides
the native SPARQL implementation of each operation.

The update renderer batches related statements into a single request. Calling
RDFLib `Graph.add()` and `Graph.remove()` for every remote triple would be much
less efficient and would make multi-statement counters non-atomic.

## Install

Install the core and contributor packages from the same repository checkout.
The contributor uses the source-ID guard operations added alongside this package;
an older released core package is not sufficient.

```bash
pip install ./lexical-graph ./lexical-graph-contrib/sparql
```

Include the optional botocore dependency only when Neptune IAM authentication is
required:

```bash
pip install ./lexical-graph './lexical-graph-contrib/sparql[neptune]'
```

## Generic SPARQL endpoint

Register the contributor factory once before resolving a graph store:

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql import (
    SPARQLGraphStoreFactory,
)

GraphStoreFactory.register(SPARQLGraphStoreFactory)

graph_store = GraphStoreFactory.for_graph_store(
    'https://rdf.example.com/query',
    graph_store_type='sparql',
    update_endpoint='https://rdf.example.com/update',
)
```

Connect the graph store to an existing repository or dataset.

Use the endpoint's ordinary `http://` or `https://` URL without rewriting its
scheme. `graph_store_type='sparql'` selects this contributor. If
`update_endpoint` is omitted, the query endpoint is used for both operations.

### Generic authentication

HTTP Basic credentials can be supplied in the connection URL, as keyword
arguments, or through `SPARQL_USER` and `SPARQL_PASSWORD` when
`auth_type='basic'` is explicitly selected. IAM and bearer authentication ignore
these environment variables:

```python
graph_store = GraphStoreFactory.for_graph_store(
    'https://rdf.example.com/query',
    graph_store_type='sparql',
    username='service-user',
    password='secret',
    headers={'X-Request-Origin': 'graphrag-toolkit'},
)
```

Use `headers={'Authorization': 'Bearer ...'}` for bearer-token endpoints. Avoid
putting credentials directly in a URL when the URL may be logged by surrounding
application code.

## Amazon Neptune with IAM

IAM authentication changes only the transport. The RDF model, operations, and
SPARQL implementation remain the same as for every other endpoint.

```python
graph_store = GraphStoreFactory.for_graph_store(
    'https://my-cluster.cluster-abcdefghijkl.eu-central-1.neptune.amazonaws.com:8182/sparql',
    graph_store_type='sparql',
    auth_type='aws_iam',
    region_name='eu-central-1',
)
```

`NeptuneIAMAuth` uses botocore's standard credential provider chain and obtains
credentials for every request. This allows botocore to refresh temporary role,
web-identity, IAM Identity Center, ECS, and EC2 credentials. Requests are signed
for the `neptune-db` service with SigV4 and require HTTPS.

The transport can also be used as a plain RDFLib graph:

```python
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.neptune_iam import (
    neptune_iam_graph,
)

graph = neptune_iam_graph(
    'https://my-cluster.cluster-abcdefghijkl.eu-central-1.neptune.amazonaws.com:8182',
    region_name='eu-central-1',
)
try:
    rows = graph.query('SELECT * WHERE { ?s ?p ?o } LIMIT 10')
finally:
    graph.close()
```

## Namespaces and tenant isolation

The default schema namespace is
`https://awslabs.github.io/graphrag-toolkit/lexical#`; the default instance
namespace is `https://awslabs.github.io/graphrag-toolkit/lexical/`.

They can be changed when creating the store:

```python
graph_store = GraphStoreFactory.for_graph_store(
    'https://rdf.example.com/query',
    graph_store_type='sparql',
    update_endpoint='https://rdf.example.com/update',
    lexical_prefix='gt',
    lexical_schema_namespace='https://example.com/graph/schema#',
    lexical_instance_namespace='https://example.com/graph/data/',
)
```

Changing namespaces changes the IRIs written for new data. Use one configuration
consistently for a repository.

Every tenant, including the default tenant, uses a deterministic named graph
below the instance namespace. Writes use `GRAPH` and reads pass the tenant graph
as the SPARQL Protocol `default-graph-uri`, keeping tenant selection independent
of endpoint-specific union-default-graph behavior.

## RDF model

Lexical nodes receive deterministic IRIs, an RDF class, and the original id as
`lg:id`. Simple edges become RDF predicates. Predicates whose property-graph
name is ambiguous are specialized by domain, for example
`lg:statementMentionedIn` versus `lg:topicMentionedIn`.

Extracted facts are represented as statement resources:

```turtle
<fact/f1> a lg:Fact ;
    lg:subject <entity/amazon> ;
    lg:predicate <relation/produces> ;
    lg:object <entity/ec2> ;
    lg:supports <statement/s1> ;
    lg:value "Amazon PRODUCES EC2" .

<relation/produces> a lg:Relation ;
    lg:value "PRODUCES" .
```

This is the toolkit's fact model, with the fact resource carrying its subject,
predicate, object, and supporting statement.

Source and chunk metadata keys are percent-encoded in property IRIs and decoded
when read. The metadata key `id` uses `lg:metadata/id` to keep it separate from
the lexical node's `lg:id`. Source hashes and per-tenant source-ID width records
preserve the core ingestion collision and width checks.

## Indexing and retrieval

This package implements the typed operations used by graph construction and
the traversal-based `ChunkBasedSearch`, `EntityNetworkSearch`, and
`EntityBasedSearch` retrievers. Operations execute as native SPARQL, without
translating Cypher.

The standard retrieval metadata postprocessor can filter returned source
metadata. Vector-side filters use the configured vector backend.

Use the RDF graph store together with a vector store and configured extraction,
embedding, and response models. The setup notebook walks through connecting
these components, ingesting a PDF, and querying the resulting graph.

## Tests

```bash
pytest lexical-graph-contrib/sparql/tests
pytest lexical-graph/tests
```

The integration tests use an in-memory RDFLib dataset behind a test HTTP adapter
by default, exercising the production client and result parsing. To also run
against a SPARQL server and exercise the complete ingestion/query pipeline with
PostgreSQL, install `psycopg2-binary` and `pgvector`, enable the `vector` extension
in a test database, and set:

```bash
SPARQL_TEST_ENDPOINT=http://localhost:3030/test/query \
SPARQL_TEST_UPDATE_ENDPOINT=http://localhost:3030/test/update \
PGVECTOR_TEST_CONNECTION=postgresql://user:password@localhost/test \
pytest lexical-graph-contrib/sparql/tests
```

Use test services. The tests create unique named graphs and PostgreSQL schemas
and remove only those resources during teardown. The pipeline tests use local
deterministic models and do not require AWS credentials or make model API calls.
