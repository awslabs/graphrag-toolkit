# Docker Services Overview for GraphRAG Local Development

This document describes the services defined in the `docker-compose.yml` file used for setting up a local GraphRAG development environment with Neo4j, PostgreSQL, and Jupyter Lab.

---

## Services

### 1. `neo4j-local`
- **Image**: `neo4j:5.25-community`
- **Description**: Neo4j graph database for storing the lexical graph structure
- **Ports**:
  - `7476:7474`: Neo4j Browser web interface
  - `7689:7687`: Bolt protocol for database connections
- **Environment Variables**:
  - `NEO4J_AUTH`: Authentication (neo4j/password)
  - `NEO4J_PLUGINS`: APOC plugin enabled for advanced procedures
- **Volumes**:
  - `neo4j_local_data`: Persists graph database
  - `neo4j_local_logs`: Neo4j log files
- **Network**: Connected to `graphrag_local_network`

### 2. `jupyter-local`
- **Build**: Custom Jupyter image with GraphRAG dependencies
- **Description**: Jupyter Lab environment for interactive development
- **Ports**:
  - `8889:8888`: Jupyter Lab web interface (no password required)
- **Environment Variables**:
  - `JUPYTER_ENABLE_LAB`: Enables Jupyter Lab interface
- **Volumes**:
  - `../notebooks:/home/jovyan/notebooks`: Notebook files
  - `~/.aws:/home/jovyan/.aws`: AWS credentials
- **Network**: Connected to `graphrag_local_network`
- **Depends On**: `pgvector-local`, `neo4j-local`

### 3. `pgvector-local`
- **Image**: `pgvector/pgvector:0.6.2-pg16`
- **Description**: PostgreSQL 16 with pgvector extension for vector embeddings
- **Ports**:
  - `5432:5432`: PostgreSQL connection
- **Environment Variables**:
  - `POSTGRES_USER`: Database username (from .env)
  - `POSTGRES_PASSWORD`: Database password (from .env)
  - `POSTGRES_DB`: Database name (from .env)
- **Volumes**:
  - `pgvector_local_data`: Data persistence
  - `./postgres/schema.sql`: Database initialization script
- **Network**: Connected to `graphrag_local_network`

---

## Development Mode Services

The `docker-compose-dev.yml` provides a development variant with hot-code-injection support. Key differences from standard mode:

| Aspect | Standard (`docker-compose.yml`) | Dev (`docker-compose-dev.yml`) |
|--------|--------------------------------|-------------------------------|
| Neo4j ports | 7476, 7689 | 7477, 7690 |
| Jupyter port | 8889 | 8890 |
| PostgreSQL port | 5432 | 5434 |
| Jupyter Dockerfile | `jupyter/Dockerfile` (full) | `jupyter/Dockerfile.dev` (minimal) |
| Notebook mount | `/home/jovyan/notebooks` | `/home/jovyan/notebooks` |
| Source mounts | None | lexical-graph, lexical-graph-contrib |

Start dev mode with: `./start-containers.sh --dev`

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

## Service Communication

Services communicate using Docker internal networking:

| From Service | To Service | Connection String |
|--------------|------------|-------------------|
| Jupyter | Neo4j | `bolt://neo4j:password@neo4j-local:7687` |
| Jupyter | PostgreSQL | `postgresql://postgres:password@pgvector-local:5432/graphrag` |

---

## Data Persistence

All services use Docker volumes for data persistence. To reset all data:
```bash
./start-containers.sh --reset
```

---

## Service Access

After startup, services are available at:

| Service | Standard URL | Dev URL | Credentials | Purpose |
|---------|-------------|---------|-------------|---------|
| **Jupyter Lab** | http://localhost:8889 | http://localhost:8890 | None required | Interactive development |
| **Neo4j Browser** | http://localhost:7476 | http://localhost:7477 | neo4j/password | Graph database management |
| **PostgreSQL** | localhost:5432 | localhost:5434 | postgres/password | Vector database |
