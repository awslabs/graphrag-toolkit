[[Home](./)]

## FalkorDB as a Graph Store

### Topics

  - [Overview](#overview)
  - [Install package](#install-package)
  - [Registering FalkorDB as a graph store](#registering-falkordb-as-a-graph-store)
  - [Creating a FalkorDB graph store](#creating-a-falkordb-graph-store)

### Overview

You can use FalkorDB as a graph store.

### Install package

The FalkorDB graph store is contained in a separate contributor package. To install it:

```
!pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.15.5.zip#subdirectory=lexical-graph-contrib/falkordb
```

### Registering FalkorDB as a graph store

Before creating a FalkorDB graph store, you must register the `FalkorDBGraphStoreFactory` with the `GraphStoreFactory`:

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit_contrib.lexical_graph.storage.graph.falkordb import FalkorDBGraphStoreFactory

GraphStoreFactory.register(FalkorDBGraphStoreFactory)

```

### Creating a FalkorDB graph store

You can use the `GraphStoreFactory.for_graph_store()` static factory method to create an instance of a FalkorDB graph store.

The FalkorDB graph store currently supports [semantic-guided search](./semantic-guided-search.md). It does not support [traversal-based search](./traversal-based-search.md).

To create a [FalkorDB Cloud](https://app.falkordb.cloud/) graph store, supply a connection string that begins `falkordb://`, followed by the FalkorDB endpoint:

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit_contrib.lexical_graph.storage.graph.falkordb import FalkorDBGraphStoreFactory

falkordb_connection_info = 'falkordb://your-falkordb-endpoint'

GraphStoreFactory.register(FalkorDBGraphStoreFactory)

with GraphStoreFactory.for_graph_store(falkordb_connection_info) as graph_store:
  ...

```

You may also need to pass a username and password, and specify whether or not to use SSL:

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory

falkordb_connection_info = 'falkordb://<your-falkordb-endpoint>'

with GraphStoreFactory.for_graph_store(
      falkordb_connection_info,
      username='<username>',
      password='<password>',
      ssl=True
  ) as graph_store:

    ...
```

To create a local FalkorDB graph store, supply a connection string that has only `falkordb://`;

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory

falkordb_connection_info = 'falkordb://'

with GraphStoreFactory.for_graph_store(falkordb_connection_info) as graph_store:
  ...
```

