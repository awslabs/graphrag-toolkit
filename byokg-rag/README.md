# BYOKG-RAG: Bring Your Own Knowledge Graph for Retrieval Augmented Generation

![BYOKG-RAG Architecture](../images/byokg_rag.png)

BYOKG-RAG is a novel approach to Knowledge Graph Question Answering (KGQA) that combines the power of Large Language Models (LLMs) with structured knowledge graphs. The system allows users to bring their own knowledge graph and perform complex question answering over it.

## Key Features

- **Multi-strategy Retrieval**: Combines multiple retrieval strategies through iterative processing:
  - **Agentic triplet retrieval** for LLM-guided dynamic graph exploration
  - **Scoring-based triplet retrieval** for semantic-based triplet retrieval
  - **Path-based retrieval** for multi-hop reasoning through entity paths
  - **Query-based retrieval** for direct Cypher graph queries
- **Iterative Processing**: Uses iterative approach combining multi-strategy and Cypher-based retrieval
- **LLM-powered Reasoning**: Leverages state-of-the-art LLMs for question understanding and answer generation

## Prerequisites

### Python Version

Python 3.10 or higher is required.

### AWS Services

The byokg-rag library integrates with the following AWS services:

- **Amazon Bedrock** - Provides access to foundation models for LLM inference and embeddings
- **Amazon Neptune Analytics** - Graph analytics service with native vector search (optional)
- **Amazon Neptune Database** - Graph database service for transactional workloads (optional)
- **Amazon S3** - Object storage for data loading and embedding storage

NOTE: You can use the local graph store for development without AWS services. Production deployments typically use Neptune Analytics or Neptune Database.

### IAM Permissions

Minimum IAM permissions required for AWS integration:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:<region>::foundation-model/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "neptune-graph:ReadDataViaQuery",
        "neptune-graph:GetGraph"
      ],
      "Resource": "arn:aws:neptune-graph:<region>:<account-id>:graph/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::<bucket-name>/*"
    }
  ]
}
```

NOTE: Additional permissions may be required for Neptune Database (`neptune-db:*`) or specific Bedrock models. Adjust the policy based on your deployment.

## System Components

1. **ByoKGQueryEngine** ([src/graphrag_toolkit/byokg_rag/byokg_query_engine.py](src/graphrag_toolkit/byokg_rag/byokg_query_engine.py))
   - Core orchestrating component with dual-mode processing
   - Implements iterative retrieval with configurable iterations
   - Combines multi-strategy and Cypher-based approaches

2. **KG Linkers** ([src/graphrag_toolkit/byokg_rag/graph_connectors](src/graphrag_toolkit/byokg_rag/graph_connectors))
   - **KGLinker**: Base class for LLM-guided graph operations
   - **CypherKGLinker**: Specialized for Cypher query generation and execution
   - Links natural language queries to graph entities and relationships

3. **Graph Retrievers** ([src/graphrag_toolkit/byokg_rag/graph_retrievers](src/graphrag_toolkit/byokg_rag/graph_retrievers))
   - **AgenticRetriever**: LLM-guided iterative exploration with entity linking
   - **PathRetriever**: Multi-hop reasoning through entity relationship paths
   - **GraphQueryRetriever**: Direct Cypher query execution and result processing
   - **Rerankers**: BGE-based semantic reranking for improving retrieval relevance

4. **Graph Store** ([src/graphrag_toolkit/byokg_rag/graphstore](src/graphrag_toolkit/byokg_rag/graphstore))
   - Manages knowledge graph data structure and connectivity
   - Provides interfaces for graph traversal and querying
   - Supports multiple graph database backends

## Performance

Our results show that BYOKG-RAG outperforms existing approaches across multiple knowledge graph benchmarks:

| KGQA Hit (%) | Wiki-KG | Temp-KG | Med-KG |
|--------------|---------|---------|--------|
| Agent        | 77.8    | 57.3    | 59.2   |
| BYOKG-RAG    | 80.1    | 65.5    | 65.0   |

*See our [paper](https://arxiv.org/abs/2507.04127) for detailed methodology and results.*

## Getting Started

The byokg-rag toolkit requires Python and [pip](http://www.pip-installer.org/en/latest/) to install. You can install the byokg-rag using pip:

```bash
pip install .
```

Or install from GitHub:

```bash
pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.15.5.zip#subdirectory=byokg-rag
```

NOTE: The version number will vary based on the latest GitHub release.

## Quick Start

Run the demo notebooks:

- [Local Graph Demo](../examples/byokg-rag/byokg_rag_demo_local_graph.ipynb)
- [Neptune Analytics Demo](../examples/byokg-rag/byokg_rag_neptune_analytics_demo.ipynb)
- [Neptune Analytics with Cypher](../examples/byokg-rag/byokg_rag_neptune_analytics_demo_cypher.ipynb)
- [Neptune Database Demo](../examples/byokg-rag/byokg_rag_neptune_db_cluster_demo.ipynb)

## Configuration Reference

Complete documentation is available in the [docs/byokg-rag/](../docs/byokg-rag/) directory:

- [Overview](../docs/byokg-rag/overview.md) - Architecture, KGQA approach, and system components
- [Indexing](../docs/byokg-rag/indexing.md) - Dense index, fuzzy string index, and graph-store index setup
- [Graph Stores](../docs/byokg-rag/graph-stores.md) - Supported graph stores and connection setup
- [Configuration](../docs/byokg-rag/configuration.md) - Complete parameter documentation
- [FAQ](../docs/byokg-rag/faq.md) - Common questions and troubleshooting

## Examples

Additional examples are available in the [examples/byokg-rag/](../examples/byokg-rag/) directory.

## Citation

If you use BYOKG-RAG in your research, please cite our paper (to appear in EMNLP Main 2025):

**Paper**: [BYOKG-RAG: Multi-Strategy Graph Retrieval for Knowledge Graph Question Answering](https://arxiv.org/abs/2507.04127)

```bibtex
@article{mavromatis2025byokg,
  title={BYOKG-RAG: Multi-Strategy Graph Retrieval for Knowledge Graph Question Answering},
  author={Mavromatis, Costas and Adeshina, Soji and Ioannidis, Vassilis N and Han, Zhen and Zhu, Qi and Robinson, Ian and Thompson, Bryan and Rangwala, Huzefa and Karypis, George},
  journal={arXiv preprint arXiv:2507.04127},
  year={2025}
}
```

## License

This project is licensed under the Apache-2.0 License.
