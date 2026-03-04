# Graph Stores

## Overview

Graph stores manage the knowledge graph data structure and provide interfaces for graph traversal, querying, and schema introspection. The byokg-rag library supports multiple graph store backends to accommodate different deployment scenarios, from local development to production AWS deployments.

A graph store serves as the foundation for Knowledge Graph Question Answering (KGQA). It stores nodes, edges, and their properties, and provides query execution capabilities that enable the retrieval system to explore relationships and extract relevant information.

## Supported Graph Stores

| Graph Store | Best For | Deployment | Query Language |
|-------------|----------|------------|----------------|
| Amazon Neptune Analytics | Production workloads, vector search, analytics | AWS managed service | openCypher |
| Amazon Neptune Database | Production workloads, transactional graphs | AWS managed service | openCypher, Gremlin |
| Local Graph Store | Development, testing, small datasets | Local CSV files | Limited (in-memory) |

## Amazon Neptune Analytics

### Service Summary

Amazon Neptune Analytics is a fully managed graph analytics service optimized for running complex graph queries and analytics on large datasets. It provides native support for vector embeddings, making it ideal for semantic search and entity linking in KGQA applications.

Choose Neptune Analytics when you need:

- Fast analytical queries over large knowledge graphs
- Native vector search capabilities for entity linking
- Serverless scaling without managing infrastructure
- Integration with other AWS analytics services

### Prerequisites

#### AWS Resources

- An Amazon Neptune Analytics graph instance
- Amazon S3 bucket for data loading and embedding storage
- VPC configuration if accessing from compute resources

#### IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "neptune-graph:ReadDataViaQuery",
        "neptune-graph:GetGraph",
        "neptune-graph:GetGraphSummary"
      ],
      "Resource": "arn:aws:neptune-graph:<region>:<account-id>:graph/<graph-id>"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<bucket-name>",
        "arn:aws:s3:::<bucket-name>/*"
      ]
    }
  ]
}
```

NOTE: Replace `<region>`, `<account-id>`, `<graph-id>`, and `<bucket-name>` with your specific values.

#### Network Requirements

If accessing Neptune Analytics from Amazon SageMaker, EC2, or other compute resources, ensure your compute environment is configured with appropriate VPC settings and security groups to reach the Neptune Analytics endpoint.

### Installation

The Neptune Analytics graph store is included in the base byokg-rag package:

```bash
pip install graphrag-toolkit-byokg-rag
```

### Connection Setup

```python
from graphrag_toolkit.byokg_rag.graphstore import NeptuneAnalyticsGraphStore

# Connect to Neptune Analytics graph
graph_store = NeptuneAnalyticsGraphStore(
    graph_identifier="<graph-id>",
    region="<region>"
)

# Verify connection by retrieving schema
schema = graph_store.get_schema()
print(schema)
```

The connection uses AWS credentials from your environment (AWS CLI configuration, IAM role, or environment variables).

### Configuration Options

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `graph_identifier` | str | Yes | None | The unique identifier of your Neptune Analytics graph |
| `region` | str | No | Auto-detected | AWS region where the graph is located (e.g., "us-west-2") |

The graph store automatically detects the region from environment variables (`AWS_REGION`, `AWS_DEFAULT_REGION`) or boto3 session configuration if not explicitly provided.

### Limitations

- **Query Complexity**: Very complex openCypher queries with deep traversals may timeout. Consider breaking them into smaller queries.
- **Regional Availability**: Neptune Analytics is available in specific AWS regions. Check the [AWS Regional Services List](https://aws.amazon.com/about-aws/global-infrastructure/regional-product-services/) for current availability.
- **Vector Search**: Vector search requires embeddings to be loaded into the graph as node properties. Use `NeptuneAnalyticsGraphStoreIndex` for this functionality.
- **Write Operations**: The graph store focuses on read operations for KGQA. Graph modifications should be performed through Neptune Analytics data loading APIs.

### See Also

- [Amazon Neptune Analytics Documentation](https://docs.aws.amazon.com/neptune-analytics/)
- [Neptune Analytics openCypher Support](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-opencypher.html)
- [Neptune Analytics Vector Search](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vector-search.html)

## Amazon Neptune Database

### Service Summary

Amazon Neptune Database is a fully managed graph database service that supports both property graph and RDF graph models. It provides ACID transactions, high availability, and read replicas for production workloads.

Choose Neptune Database when you need:

- Transactional graph operations with ACID guarantees
- High availability with automatic failover
- Read replicas for scaling query workloads
- Support for both openCypher and Gremlin query languages

### Prerequisites

#### AWS Resources

- An Amazon Neptune Database cluster with at least one instance
- Amazon S3 bucket for bulk data loading
- VPC with appropriate security groups and network configuration
- IAM role for Neptune to access S3 (for bulk loading)

#### IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "neptune-db:ReadDataViaQuery",
        "neptune-db:GetQueryStatus"
      ],
      "Resource": "arn:aws:neptune-db:<region>:<account-id>:cluster-<cluster-id>/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<bucket-name>",
        "arn:aws:s3:::<bucket-name>/*"
      ]
    }
  ]
}
```

NOTE: Replace `<region>`, `<account-id>`, `<cluster-id>`, and `<bucket-name>` with your specific values.

#### Network Requirements

Neptune Database clusters are deployed within a VPC. Your application must run within the same VPC or have network connectivity (VPC peering, Transit Gateway, or VPN) to access the cluster endpoint.

WARNING: Neptune Database does not support public endpoints. You must access it from within your VPC or through a bastion host.

### Installation

The Neptune Database graph store is included in the base byokg-rag package:

```bash
pip install graphrag-toolkit-byokg-rag
```

### Connection Setup

```python
from graphrag_toolkit.byokg_rag.graphstore import NeptuneDBGraphStore

# Connect to Neptune Database cluster
graph_store = NeptuneDBGraphStore(
    endpoint_url="https://<cluster-endpoint>:8182",
    region="<region>"
)

# Verify connection by retrieving schema
schema = graph_store.get_schema()
print(schema)
```

The endpoint URL should include the protocol (https), cluster endpoint, and port (8182 for openCypher).

### Configuration Options

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `endpoint_url` | str | Yes | None | The full endpoint URL including protocol and port (e.g., "https://my-cluster.cluster-xyz.us-west-2.neptune.amazonaws.com:8182") |
| `region` | str | Yes | None | AWS region where the Neptune cluster is located |

### Limitations

- **VPC Access Only**: Neptune Database requires VPC connectivity. You cannot access it from the public internet without a bastion host or VPN.
- **Query Timeout**: Long-running queries may timeout. The default timeout is configurable but should be balanced against resource usage.
- **Schema Refresh**: The graph store caches schema information. If you modify the graph structure, you may need to recreate the graph store instance to refresh the schema.
- **Concurrent Queries**: While Neptune supports concurrent queries, very high concurrency may require read replicas for optimal performance.

### See Also

- [Amazon Neptune Database Documentation](https://docs.aws.amazon.com/neptune/)
- [Neptune openCypher Support](https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-opencypher.html)
- [Neptune VPC Configuration](https://docs.aws.amazon.com/neptune/latest/userguide/security-vpc.html)

## Local Graph Store

### Service Summary

The local graph store provides a lightweight, in-memory graph implementation for development and testing. It loads graph data from CSV files and supports basic graph operations without requiring external infrastructure.

Choose the local graph store when you need:

- Rapid prototyping and development
- Testing with small datasets
- Local execution without AWS dependencies
- Learning and experimentation with byokg-rag

### Prerequisites

- Python 3.10 or higher
- Graph data in CSV format (nodes and edges files)

### Installation

The local graph store is included in the base byokg-rag package:

```bash
pip install graphrag-toolkit-byokg-rag
```

### Connection Setup

```python
from graphrag_toolkit.byokg_rag.graphstore import LocalKGStore

# Load graph from CSV files
graph_store = LocalKGStore()
graph_store.read_from_csv(
    nodes_file="path/to/nodes.csv",
    edges_file="path/to/edges.csv"
)

# Retrieve schema
schema = graph_store.get_schema()
print(schema)
```

The CSV files should follow this format:

**nodes.csv:**
```csv
~id,~label,name,description
1,Person,Albert Einstein,Theoretical physicist
2,Person,Marie Curie,Physicist and chemist
3,Award,Nobel Prize,Prestigious award
```

**edges.csv:**
```csv
~id,~from,~to,~label,year
e1,1,3,WON,1921
e2,2,3,WON,1903
```

### Configuration Options

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `nodes_file` | str | Yes | None | Path to CSV file containing node data |
| `edges_file` | str | Yes | None | Path to CSV file containing edge data |

### Limitations

- **In-Memory Only**: All graph data is stored in memory. Large graphs may exceed available RAM.
- **No Persistence**: Changes to the graph are not persisted. You must reload from CSV files on each restart.
- **Limited Query Support**: The local graph store does not support complex query languages like openCypher. It provides basic traversal operations only.
- **No Vector Search**: The local graph store does not support native vector search. Use separate dense indexes for semantic entity linking.
- **Single Process**: The local graph store is not designed for concurrent access from multiple processes.

TIP: Use the local graph store for initial development and switch to Neptune Analytics or Neptune Database for production deployments.

### See Also

- [Example Notebooks](../../examples/byokg-rag/) - Includes local graph store examples
- [CSV Format Specification](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html) - Neptune CSV format reference
