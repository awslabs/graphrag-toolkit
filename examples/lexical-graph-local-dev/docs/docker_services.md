# Docker Services Overview for GraphRAG Local Development

This document describes the services defined in the `docker-compose.yml` file used for setting up a local GraphRAG development environment with Neo4j, PostgreSQL, and Jupyter Lab.

---

## Services

### 1. `neo4j`
- **Image**: `neo4j:5.25-community`
- **Description**: Neo4j graph database for storing the lexical graph structure
- **Ports**:
  - `7476:7474`: Neo4j Browser web interface
  - `7687:7687`: Bolt protocol for database connections
- **Environment Variables**:
  - `NEO4J_AUTH`: Authentication (neo4j/password)
  - `NEO4J_PLUGINS`: APOC plugin enabled for advanced procedures
- **Volumes**:
  - `neo4j_data`: Persists graph database
  - `neo4j_logs`: Neo4j log files
- **Network**: Connected to `lg_graphrag_network`

### 2. `jupyter-notebook`
- **Build**: Custom Jupyter image with GraphRAG dependencies
- **Description**: Jupyter Lab environment for interactive development
- **Ports**:
  - `8889:8888`: Jupyter Lab web interface (no password required)
- **Environment Variables**:
  - `JUPYTER_ENABLE_LAB`: Enables Jupyter Lab interface
- **Volumes**:
  - `../notebooks:/home/jovyan/work`: Notebook files
  - `../../../lexical-graph:/home/jovyan/lexical-graph-src`: Source code (dev mode only)
  - `jupyter_data`: Jupyter user data and configurations
- **Network**: Connected to `lg_graphrag_network`
- **Depends On**: `postgres`, `neo4j`

### 3. `postgres`
- **Image**: `pgvector/pgvector:0.6.2-pg16`
- **Description**: PostgreSQL 16 with pgvector extension for vector embeddings
- **Ports**:
  - `5432:5432`: PostgreSQL connection
- **Environment Variables**:
  - `POSTGRES_USER`: Database username (from .env)
  - `POSTGRES_PASSWORD`: Database password (from .env)
  - `POSTGRES_DB`: Database name (from .env)
- **Volumes**:
  - `pgvector_data`: Data persistence
  - `./postgres/schema.sql`: Database initialization script
- **Network**: Connected to `lg_graphrag_network`

---

## Development Mode Services

When using `--dev` flag, additional volume mounts are enabled:

### Enhanced Jupyter Service (Dev Mode)
- **Additional Volume**: `../../../lexical-graph:/home/jovyan/lexical-graph-src`
- **Hot-Code-Injection**: Changes to lexical-graph source reflected immediately
- **Editable Installation**: Package installed in development mode
- **Auto-Reload**: Jupyter notebooks automatically reload modules

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

These extensions are required for:
- **pgvector**: Vector similarity search and embeddings storage
- **pg_trgm**: Trigram-based fuzzy text matching
- **graphrag schema**: Organized storage for GraphRAG-specific data

---

## Networks

- **lg_graphrag_network**: Dedicated Docker bridge network for service communication

---

## Volumes

- **neo4j_data**: Persists Neo4j graph database and configurations
- **neo4j_logs**: Neo4j application and query logs
- **pgvector_data**: PostgreSQL data including vector embeddings and indexes
- **jupyter_data**: Jupyter user data, notebooks, and configurations

---

## Environment Variables

Services use environment variables from `docker/.env`:

```bash
# Database Configuration
POSTGRES_USER=graphrag
POSTGRES_PASSWORD=graphragpass
POSTGRES_DB=graphrag_db

# Neo4j Configuration
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Connection Strings (for notebooks)
VECTOR_STORE="postgresql://graphrag:graphragpass@postgres:5432/graphrag_db"
GRAPH_STORE="bolt://neo4j:password@neo4j:7687"
```

---

## Service Communication

Services communicate using Docker internal networking:

| From Service | To Service | Connection String |
|--------------|------------|-------------------|
| Jupyter | Neo4j | `bolt://neo4j:password@neo4j:7687` |
| Jupyter | PostgreSQL | `postgresql://graphrag:graphragpass@postgres:5432/graphrag_db` |

---

## Data Persistence

All services use Docker volumes for data persistence:

- **Database data** survives container restarts
- **Jupyter configurations** persist between sessions
- **Neo4j graph data** maintained across deployments

To reset all data, use:
```bash
./start-containers.sh --reset
```

---

## Service Access

After startup, services are available at:

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Jupyter Lab** | http://localhost:8889 | None required | Interactive development |
| **Neo4j Browser** | http://localhost:7476 | neo4j/password | Graph database management |
| **PostgreSQL** | localhost:5432 | graphrag/graphragpass | Vector database (internal) |

All development happens in Jupyter Lab, which provides pre-configured access to both databases.