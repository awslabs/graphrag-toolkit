# Docker Services Overview for GraphRAG Hybrid Development

This document describes the services defined in the `docker-compose.yml` file used for setting up a hybrid GraphRAG environment with local Docker services and AWS cloud integration.

---

## Services

### 1. `neo4j-hybrid`
- **Image**: `neo4j:5.25-community`
- **Description**: Neo4j graph database for storing the lexical graph structure
- **Ports**:
  - `7475:7474`: Neo4j Browser web interface
  - `7688:7687`: Bolt protocol for database connections
- **Environment Variables**:
  - `NEO4J_AUTH`: Authentication (neo4j/password)
  - `NEO4J_PLUGINS`: APOC plugin enabled
- **Volume**: Persists graph data using `neo4j_data`
- **Network**: Connected to `graphrag_network`

### 2. `jupyter-hybrid`
- **Build**: Custom Jupyter image with GraphRAG dependencies
- **Description**: Jupyter Lab environment for interactive development
- **Ports**:
  - `8889:8888`: Jupyter Lab web interface
- **Environment Variables**:
  - `JUPYTER_ENABLE_LAB`: Enables Jupyter Lab interface
- **Volumes**:
  - `../notebooks:/home/jovyan/work`: Notebook files
  - `../../../lexical-graph:/home/jovyan/lexical-graph-src`: Source code (dev mode)
  - `~/.aws:/home/jovyan/.aws`: AWS credentials
- **Network**: Connected to `graphrag_network`
- **Depends On**: `neo4j-hybrid`, `pgvector-hybrid`

### 3. `pgvector-hybrid`
- **Image**: `pgvector/pgvector:0.6.2-pg16`
- **Description**: PostgreSQL 16 with pgvector extension for vector embeddings
- **Ports**:
  - `5433:5432`: PostgreSQL connection (different port to avoid conflicts)
- **Environment Variables**:
  - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Database credentials
- **Volumes**:
  - `pgvector_data`: Data persistence
  - `./postgres/schema.sql`: Database initialization
- **Network**: Connected to `graphrag_network`

---

## Database Schema

The PostgreSQL container initializes with the following schema:

```sql
-- Enable pgvector extension for vector similarity search
CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;

-- Enable pg_trgm extension for trigram-based text search
CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;

-- Create schema for GraphRAG data
CREATE SCHEMA IF NOT EXISTS graphrag;
```

---

## Networks

- **graphrag_network**: Dedicated Docker bridge network for service communication

---

## Volumes

- **neo4j_data**: Persists Neo4j graph database
- **neo4j_logs**: Neo4j log files
- **pgvector_data**: PostgreSQL data including vector embeddings
- **jupyter_data**: Jupyter user data and configurations

---

## AWS Integration

The hybrid environment integrates with AWS services:

- **S3**: Document storage and batch processing
- **Bedrock**: LLM processing for extraction and generation
- **DynamoDB**: Batch job tracking (optional)

AWS credentials are mounted from the host system (`~/.aws`) into the Jupyter container for seamless cloud integration.

---

## Port Mapping

Services use different ports than local-dev to avoid conflicts:

| Service | Internal Port | External Port | Purpose |
|---------|---------------|---------------|---------|
| Neo4j HTTP | 7474 | 7475 | Web browser interface |
| Neo4j Bolt | 7687 | 7688 | Database connections |
| Jupyter Lab | 8888 | 8889 | Interactive development |
| PostgreSQL | 5432 | 5433 | Vector database |

This allows running both local-dev and hybrid-dev environments simultaneously.