# Lexical Graph Local Development

> **⚠️ IMPORTANT NOTICE**: FalkorDB support has been **removed** and replaced with **Neo4j** as the primary graph database. All examples and configurations now use Neo4j. If you have existing FalkorDB setups, please migrate to Neo4j.

## Overview

This example provides a complete local development environment for the GraphRAG Toolkit's lexical-graph functionality. The environment runs entirely in Docker with Jupyter Lab for interactive development, Neo4j for graph storage, and PostgreSQL with pgvector for vector embeddings.

## Notebooks

- [**00-Setup**](./notebooks/00-Setup.ipynb) – Environment setup, package installation, and development mode configuration
- [**01-Combined-Extract-and-Build**](./notebooks/01-Combined-Extract-and-Build.ipynb) – Complete extraction and building pipeline using `LexicalGraphIndex.extract_and_build()`
- [**02-Querying**](./notebooks/02-Querying.ipynb) – Graph querying examples using `LexicalGraphQueryEngine` with various retrievers
- [**03-Querying-with-Prompting**](./notebooks/03-Querying-with-Prompting.ipynb) – Advanced querying with custom prompts and prompt providers
- [**04-Advanced-Configuration-Examples**](./notebooks/04-Advanced-Configuration-Examples.ipynb) – Advanced reader configurations and metadata handling
- [**05-S3-Directory-Reader-Provider**](./notebooks/05-S3-Directory-Reader-Provider.ipynb) – S3-based document reading and processing

## Quick Start

> All commands below should be executed from the `lexical-graph-local-dev/` directory.

### 1. AWS Prerequisites

Before starting, ensure you have:
- [AWS CLI configured with credentials](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html) — verify with `aws sts get-caller-identity`
- Access to Amazon Bedrock models:
  - `us.anthropic.claude-sonnet-4-6` (extraction, response, evaluation)
  - `cohere.embed-english-v3` (embeddings)

### 2. Configure Environment

```bash
cp notebooks/.env.template notebooks/.env
```

Review `notebooks/.env` — defaults work for local Docker services. Set `S3_BUCKET_NAME` if using S3 features (notebooks 03, 04, 05).

### 3. Start the Environment

**Standard:**
```bash
cd docker
./start-containers.sh
```

**Development Mode (Hot-Code-Injection):**
```bash
cd docker
./start-containers.sh --dev
```

### 4. Access Jupyter Lab

Open your browser to: **http://localhost:8889** (or **http://localhost:8890** for dev mode)

- No password required
- Navigate to the `notebooks` folder to find notebooks
- All dependencies are pre-installed

### 5. Run the Setup Notebook

Start with `00-Setup.ipynb` to configure your environment and verify all services are working.

## Docker Scripts

### Available Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `start-containers.sh` | Unix/Linux/Mac | Main startup script with all options |

### Script Options

| Flag | Description |
|------|-------------|
| `--dev` | Enable development mode with hot-code-injection |
| `--reset` | Reset all data and rebuild containers |

### Examples

```bash
# Standard startup
./start-containers.sh

# Development mode with hot-reload
./start-containers.sh --dev

# Reset everything and start fresh
./start-containers.sh --reset

# Reset with dev mode
./start-containers.sh --dev --reset
```

## Services

After startup, the following services are available:

| Service | Standard URL | Dev URL | Credentials | Purpose |
|---------|-------------|---------|-------------|---------|
| **Jupyter Lab** | http://localhost:8889 | http://localhost:8890 | None required | Interactive development |
| **Neo4j Browser** | http://localhost:7476 | http://localhost:7477 | neo4j/password | Graph database management |
| **PostgreSQL** | localhost:5432 | localhost:5434 | postgres/password | Vector storage |

## Development Mode

Development mode enables hot-code-injection for active lexical-graph development:

```bash
./start-containers.sh --dev
```

**Features:**
- Mounts local `lexical-graph/` source code into Jupyter container
- Changes to source code are immediately reflected in notebooks
- No container rebuilds needed for code changes
- Auto-reload configured in notebooks

**When to use:**
- Contributing to lexical-graph package
- Testing local changes
- Debugging functionality
- Rapid prototyping

## Data Persistence

**Default behavior:** All data persists between container restarts
- Neo4j graph data in Docker volumes
- PostgreSQL vector data in Docker volumes
- Jupyter notebooks and user data

**To reset all data:**
```bash
./start-containers.sh --reset
```

## Database Configuration

### PostgreSQL Schema

The PostgreSQL container automatically applies this schema on initialization:

```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;

-- Create GraphRAG schema
CREATE SCHEMA IF NOT EXISTS graphrag;
```

### Neo4j Configuration

Neo4j is configured with:
- APOC plugin enabled
- Default credentials: neo4j/password
- Persistent data storage

## Reader Providers

The environment includes comprehensive document reader support:

### File-Based Readers
- **PDF**: PyMuPDF-based PDF processing
- **DOCX**: Word document processing
- **PPTX**: PowerPoint presentation processing
- **Markdown**: Markdown file processing
- **CSV/Excel**: Structured data with S3 streaming support
- **JSON/JSONL**: JSON document processing

### Web and API Readers
- **Web**: HTML page scraping and processing
- **YouTube**: Video transcript extraction
- **Wikipedia**: Wikipedia article processing
- **GitHub**: Repository and file processing

### Cloud Storage Readers
- **S3 Directory**: AWS S3 bucket and object processing
- **Directory**: Local directory traversal

### Universal S3 Support

Most file-based readers support both local files and S3 URLs:

```python
# Works with local files
docs = reader.read('/local/path/file.pdf')

# Also works with S3 URLs
docs = reader.read('s3://my-bucket/documents/file.pdf')
```

## Environment Variables

Key environment variables (configured in `notebooks/.env`):

```bash
# Database connections (Docker internal names)
VECTOR_STORE="postgresql://postgres:password@pgvector-local:5432/graphrag"
GRAPH_STORE="bolt://neo4j:password@neo4j-local:7687"

# AWS Configuration (optional)
AWS_REGION="us-east-1"
AWS_PROFILE="your-profile"

# Model Configuration
EMBEDDINGS_MODEL="cohere.embed-english-v3"
EXTRACTION_MODEL="us.anthropic.claude-sonnet-4-6"
```

## Automated Testing

Run all notebooks end-to-end with a single command:

```bash
bash tests/test-local-dev-notebooks.sh
```

This handles the full lifecycle: environment setup, Docker containers, notebook execution, reporting, and cleanup.

Configuration options (environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_GITHUB` | `true` | Skip GitHub reader cells (requires token) |
| `SKIP_PPTX` | `true` | Skip PPTX reader cells (slow, requires torch) |
| `SKIP_LONG_RUNNING` | `true` | Skip JSON/Wikipedia extract_and_build cells |
| `CLEANUP` | `true` | Clean up all resources after run |
| `REPORT_DIR` | `test-results/` | Output directory for reports |

Reports are generated in `test-results/` (execution_report.json + execution_report.md).

## Troubleshooting

### Common Issues

**Port conflicts:**
- Jupyter: 8889 (not 8888)
- Neo4j HTTP: 7476 (not 7474)
- Neo4j Bolt: 7689 (not 7687)
- PostgreSQL: 5432

**Container networking:**
- Use container names in connection strings (e.g., `neo4j-local:7687`, not `localhost:7687`)
- The `.env` file uses Docker internal networking

**Development mode:**
- Restart Jupyter kernel after enabling hot-reload
- Check that lexical-graph source is mounted at `/home/jovyan/lexical-graph`

### Reset Environment

If you encounter persistent issues:

```bash
# Stop and remove everything
docker compose down -v

# Start fresh
./start-containers.sh --reset
```

## AWS Foundation Model Access (Optional)

For AWS Bedrock integration, ensure your AWS account has access to:
- `us.anthropic.claude-sonnet-4-6`
- `cohere.embed-english-v3`

Enable model access via the [Bedrock model access console](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html).

## Migration from FalkorDB

If you have existing FalkorDB configurations:

1. **Update connection strings** to use Neo4j format:
   ```bash
   # Old FalkorDB
   GRAPH_STORE="falkordb://localhost:6379"
   
   # New Neo4j
   GRAPH_STORE="bolt://neo4j:password@neo4j-local:7687"
   ```

2. **Update imports** in your code:
   ```python
   # Replace FalkorDB imports with Neo4j
   from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
   GraphStoreFactory.register(Neo4jGraphStoreFactory)
   ```

3. **Migrate data** if needed (contact support for migration tools)