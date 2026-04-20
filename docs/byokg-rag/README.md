# BYOKG-RAG Documentation

## Getting Started

- [Overview](./overview.md) - Architecture, KGQA approach, and system components
- [Indexing](./indexing.md) - Dense index, fuzzy string index, and graph-store index setup
- [Graph Stores](./graph-stores.md) - Supported graph stores and connection setup

## Configuration and Usage

- [Configuration Reference](./configuration.md) - Complete parameter documentation for all components
- [Query Engine](./query-engine.md) - Query engine details and usage patterns
- [Querying](./querying.md) - Entity linking, graph traversal, reranking, and verbalisation

## Retrieval Strategies

- [Graph Retrievers](./graph-retrievers.md) - Individual retriever implementations and configuration
- [Multi-Strategy Retrieval](./multi-strategy-retrieval.md) - Combined retrieval approach and orchestration

## Reference

- [FAQ](./faq.md) - Common questions, known limitations, and troubleshooting

## Examples

See the [examples/byokg-rag/](../../examples/byokg-rag/) directory for runnable notebooks demonstrating:

- Local graph store usage for development
- Neptune Analytics integration
- Neptune Database integration
- Cypher-based retrieval
- Vector embeddings and semantic search
