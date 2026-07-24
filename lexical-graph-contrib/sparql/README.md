# RDF / SPARQL graph store

This contributor package stores the AWS GraphRAG Toolkit lexical graph in an
existing SPARQL 1.1 query/update endpoint. It is endpoint-neutral by default and
has an optional Amazon Neptune IAM transport.

Core graph builders and retrievers identify a backend-neutral `GraphOperation`.
Property-graph stores execute their native queries, while this package provides
the native SPARQL implementation of each operation.

The update renderer batches related statements into a single request. Calling
RDFLib `Graph.add()` and `Graph.remove()` for every remote triple would be much
less efficient and would make multi-statement counters non-atomic.

## Install

From a repository checkout:

```bash
pip install ./lexical-graph-contrib/sparql
```

Include the optional botocore dependency only when Neptune IAM authentication is
required:

```bash
pip install './lexical-graph-contrib/sparql[neptune]'
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
    'sparql+https://rdf.example.com/query',
    update_endpoint='https://rdf.example.com/update',
)
```

Connect the graph store to an existing repository or dataset.

Supported connection schemes are:

| Scheme | HTTP endpoint |
|---|---|
| `sparql://host/path` | `http://host/path` |
| `sparql+http://host/path` | `http://host/path` |
| `sparql+s://host/path` | `https://host/path` |
| `sparql+https://host/path` | `https://host/path` |
| `sparql+neptune://host:8182` | `https://host:8182/sparql`, signed with IAM |

If `update_endpoint` is omitted, the query endpoint is used for both operations.
It can also be supplied as an encoded `update_endpoint` query parameter.

### Generic authentication

HTTP Basic credentials can be supplied in the connection URL, as keyword
arguments, or through `SPARQL_USER` and `SPARQL_PASSWORD`:

```python
graph_store = GraphStoreFactory.for_graph_store(
    'sparql+https://rdf.example.com/query',
    username='service-user',
    password='secret',
    headers={'X-Request-Origin': 'graphrag-toolkit'},
)
```

Use `headers={'Authorization': 'Bearer ...'}` for bearer-token endpoints. Avoid
putting credentials directly in a URL when the URL may be logged by surrounding
application code.

## Amazon Neptune with IAM

The Neptune scheme changes only the transport. The RDF model, operations, and
SPARQL implementation remain the same as for every other endpoint.

```python
graph_store = GraphStoreFactory.for_graph_store(
    'sparql+neptune://my-cluster.cluster-abcdefghijkl.eu-central-1.neptune.amazonaws.com:8182',
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
    'sparql+https://rdf.example.com/query',
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

## Tests

```bash
pytest lexical-graph-contrib/sparql/tests
pytest lexical-graph/tests
```
