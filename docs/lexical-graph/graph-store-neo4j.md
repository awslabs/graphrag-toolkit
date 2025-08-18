[[Home](./)]

## Neo4j as a Graph Store

### Topics

  - [Overview](#overview)
  - [Creating a Neo4j graph store](#creating-a-neo4j-graph-store)

### Overview

You can use [Neo4j](https://neo4j.com/docs) as a graph store.

### Creating a Neo4j graph store

Use the `GraphStoreFactory.for_graph_store()` static factory method to create an instance of a Neo4j graph store.

To create a Neo4j graph store, supply a connection string that begins with one of the [Neo4j URI schemes](https://neo4j.com/docs/api/python-driver/5.28/api.html#uri) (e.g. `neo4j://`) in accordance with the following format:

```
[scheme]://[user[:password]@][host][:port][/dbname][?routing_context]
```

For example:

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory

neo4j_connection_info = 'neo4j://neo4j:!zfg%dGGh@example.com:7687'

with GraphStoreFactory.for_graph_store(neo4j_connection_info) as graph_store:
    ...
```
