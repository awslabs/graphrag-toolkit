# BYOKG-RAG: Bring Your Own Knowledge Graph for Retrieval Augmented Generation 

![BYOKG-RAG Architecture](../images/byokg_rag.png)

BYOKG-RAG is a novel approach to Knowledge Graph Question Answering (KGQA) that combines the power of Large Language Models (LLMs) with structured knowledge graphs. The system allows users to bring their own knowledge graph and perform complex question answering over it.

## Key Features 🔑

- **Multi-strategy Retrieval**: Combines multiple retrieval strategies through iterative processing:
  - **Agentic triplet retrieval** for LLM-guided dynamic graph exploration
  - **Scoring-based triplet retrieval** for semantic-based triplet retrieval
  - **Path-based retrieval** for multi-hop reasoning through entity paths
  - **Query-based retrieval** for direct Cypher graph queries
- **Iterative Processing**: Uses iterative approach combining multi-strategy and Cypher-based retrieval
- **LLM-powered Reasoning**: Leverages state-of-the-art LLMs for question understanding and answer generation

## System Components ⚙️

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

## Performance 📈

Our results show that BYOKG-RAG outperforms existing approaches across multiple knowledge graph benchmarks:

| KGQA Hit (%) | Wiki-KG | Temp-KG | Med-KG |
|--------------|---------|---------|--------|
| Agent        | 77.8    | 57.3    | 59.2   |
| BYOKG-RAG    | 80.1    | 65.5    | 65.0   |

*See our [paper](https://arxiv.org/abs/2507.04127) for detailed methodology and results!* 📄

## Getting Started 🚀

The byokg-rag toolkit requires Python and [pip](http://www.pip-installer.org/en/latest/) to install. You can install the byokg-rag using pip:

1. Install dependencies:
```bash
pip install .
```
or 
```
pip install https://github.com/awslabs/graphrag-toolkit/archive/refs/tags/v3.15.5.zip#subdirectory=byokg-rag
```
(The version number will vary based on the latest GitHub release)

2. Run the demo notebooks:
   - [Local Graph Demo](../examples/byokg-rag/byokg_rag_demo_local_graph.ipynb)
   - [Neptune Analytics Demo](../examples/byokg-rag/byokg_rag_neptune_analytics_demo.ipynb)
   - [Neptune Analytics with Cypher](../examples/byokg-rag/byokg_rag_neptune_analytics_demo_cypher.ipynb)
   - [Neptune Database Demo](../examples/byokg-rag/byokg_rag_neptune_db_cluster_demo.ipynb)

## Citation 📚

If you use BYOKG-RAG in your research, please cite our paper (to appear in EMNLP Main 2025):

**Paper**: [BYOKG-RAG: Multi-Strategy Graph Retrieval for Knowledge Graph Question Answering](https://arxiv.org/abs/2507.04127)

```
@article{mavromatis2025byokg,
  title={BYOKG-RAG: Multi-Strategy Graph Retrieval for Knowledge Graph Question Answering},
  author={Mavromatis, Costas and Adeshina, Soji and Ioannidis, Vassilis N and Han, Zhen and Zhu, Qi and Robinson, Ian and Thompson, Bryan and Rangwala, Huzefa and Karypis, George},
  journal={arXiv preprint arXiv:2507.04127},
  year={2025}
}
```

## License ⚖️

This project is licensed under the Apache-2.0 License.
