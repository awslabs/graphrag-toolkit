# BYOKG-RAG Neptune Analytics Demo

This directory contains notebooks demonstrating Knowledge Graph Question Answering using BYOKG-RAG with Amazon Neptune Analytics and Neptune Database.

## Prerequisites

- AWS credentials configured with access to Bedrock and Neptune Analytics/Database
- Python 3.10+

## Environment Configuration

Copy the environment template and fill in your values:

```
cp .env.template .env
```

Edit `.env` and set your AWS region, graph identifier or endpoint, and model names. See `.env.template` for all available options.

## Notebooks

| Notebook | Description | Required Env Vars |
|----------|-------------|-------------------|
| `byokg_rag_neptune_analytics_demo.ipynb` | KGQA with Neptune Analytics (KGLinker + CypherKGLinker) | AWS_REGION, GRAPH_IDENTIFIER, MODEL_NAME |
| `byokg_rag_neptune_analytics_demo_cypher.ipynb` | Cypher-based KGQA with Neptune Analytics | AWS_REGION, GRAPH_IDENTIFIER, MODEL_NAME |
| `byokg_rag_neptune_analytics_embeddings.ipynb` | Embedding-based KGQA with Neptune Analytics | AWS_REGION, GRAPH_IDENTIFIER, MODEL_NAME, EMBEDDINGS_MODEL |
| `byokg_rag_demo_local_graph.ipynb` | KGQA with a local in-memory graph | AWS_REGION, MODEL_NAME |
| `byokg_rag_neptune_db_cluster_demo.ipynb` | KGQA with Neptune Database cluster | AWS_REGION, GRAPH_DB_ENDPOINT_URL, MODEL_NAME |
