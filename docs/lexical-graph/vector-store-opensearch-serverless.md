[[Home](./)]

## Amazon OpenSearch Serverless as a Vector Store

### Topics

  - [Overview](#overview)
  - [Install dependencies](#install-dependencies)
  - [Creating an OpenSearch Serverless vector store](#creating-an-opensearch-serverless-vector-store)
  - [Amazon OpenSearch Serverless and custom document IDs](#amazon-opensearch-serverless-and-custom-document-ids)
    - [Verify and repair an Amazon OpenSearch Serverless vector store](#verify-and-repair-an-amazon-opensearch-serverless-vector-store)

### Overview

You can use an Amazon OpenSearch Serverless collection as a vector store.

### Install dependencies

The OpenSeacrh vector store requires both the `opensearch-py` and `llama-index-vector-stores-opensearch` packages:

```
pip install opensearch-py llama-index-vector-stores-opensearch
```

### Creating an OpenSearch Serverless vector store

Use the `VectorStoreFactory.for_vector_store()` static factory method to create an instance of an Amazon OpenSearch Serverless vector store.

To create an Amazon OpenSearch Serverless vector store, supply a connection string that begins `aoss://`, followed by the https endpoint of the OpenSearch Serverless collection:

```python
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

opensearch_connection_info = 'aoss://https://123456789012.us-east-1.aoss.amazonaws.com'

with VectorStoreFactory.for_vector_store(opensearch_connection_info) as vector_store:
    ...
```

### Amazon OpenSearch Serverless and custom document IDs

Amazon OpenSearch Serverless vector search collections do not allow documents to be indexed by a custom document ID, or updated by upsert requests. Internally, Amazon OpenSearch Serverless creates a unique document ID for each index action. This means that if the same document is indexed twice, there will be two separate entris in a collection.

Version 3.10.3 of the toolkit introduces a step into the bulk indexing process that checks whether a document has already been indexed. If it has, the process ignores the request to (re)index that particular document. Further, if the check determines that the document has already been indexed multiple times in the vector store, it deletes the redundant copies from teh store.

#### Verify and repair an Amazon OpenSearch Serverless vector store

3.10.3 introduces a [command-line tool](https://github.com/awslabs/graphrag-toolkit/blob/main/examples/lexical-graph/scripts/repair_opensearch_vector_store.py) that you can use to verify and repair an Amazon OpenSearch Serverless vector store. Download [repair_opensearch_vector_store.py](https://github.com/awslabs/graphrag-toolkit/blob/main/examples/lexical-graph/scripts/repair_opensearch_vector_store.py) and run the folliwng command: 

```
$ python repair_opensearch_vector_store.py --graph-store <graph store info> --vector-store <vector store info> --dry-run
```

The `--dry-run` flag above allows you to run the tool and see what repairs are necessary without actually modifying the indexes. Remove the `--dry-run` flag to repair (delete duplicate documents from) the vector store.

The tool has the following parameters:

| Parameter  | Description | Mandatory | Default |
| ------------- | ------------- | ------------- | ------------- |
| `--graph-store` | Graph store connection info (for example `neptune-db://mydbcluster.cluster-123456789012.us-east-1.neptune.amazonaws.com:8182`) | Yes | – |
| `--vector-store` | Vector store connection info (for example `aoss://https://123456789012.us-east-1.aoss.amazonaws.com`) | Yes | – |
| `--tenant-ids` | Space-separated list of tenant ids to check | No | All tenants |
| `--batch-size` | Number of OpenSearch documents to check with each request to OpenSearch | No | 1000 |
| `--dry-run` | Verify the store, but do not repair (delete any duplicates from) the store | No | Tool deletes duplicate documents from the vector store |

The tool returns results in the following format:

```
{
  "duration_seconds": 16,
  "dry_run": false,
  "totals": {
    "total_node_ids": 15354,
    "total_doc_ids": 15354,
    "total_deleted_doc_ids": 0,
    "total_unindexed": 0
  },
  "results": [
    {
      "tenant_id": "default_",
      "index": "chunk",
      "num_nodes": 17,
      "num_docs": 17,
      "num_deleted": 0,
      "num_unindexed": 0
    },
    {
      "tenant_id": "default_",
      "index": "statement",
      "num_nodes": 211,
      "num_docs": 211,
      "num_deleted": 0,
      "num_unindexed": 0
    },
    {
      "tenant_id": "local",
      "index": "chunk",
      "num_nodes": 1,
      "num_docs": 1,
      "num_deleted": 0,
      "num_unindexed": 0
    },
    {
      "tenant_id": "local",
      "index": "statement",
      "num_nodes": 26,
      "num_docs": 26,
      "num_deleted": 0,
      "num_unindexed": 0
    }
  ]
}
```

Field descriptions:

| Field  | Description |
| ------------- | ------------- |
| `dry_run` | `true` - Duplicate docs not actually deleted from vector store (the number of deleted docs in the results are indicative of the numbers that would have been deleted); `false` - Duplicate docs will have been deleted from the vector store. |
| `total_node_ids` | Total number of indexable nodes in the graph |
| `total_doc_ids` | Total number of documents in the vector store |
| `total_deleted_doc_ids` | Total number of documents deleted from vector store (indicative number only if `dry_run` is `true`) |
| `total_unindexed` | Total number of nodes that have not been indexed |
| `tenant_id` | Tenant id (the default tenant is `default_`) |
| `index` | Index name |
| `num_nodes` | Number of indexable nodes in a specific tenant graph |
| `num_docs` | Number of documents in a specific tenant vector index |
| `num_deleted` | Number of documents deleted from a specific tenant vector index (indicative number only if `dry_run` is `true`) |
| `num_unindexed` | Number of nodes that have not been indexed in a specific tenant vector index |