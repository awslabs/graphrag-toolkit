# Lexical Graph Local Development

> **⚠️ IMPORTANT NOTICE**: FalkorDB support has been **removed** and replaced with **Neo4j** as the primary graph database. All examples and configurations now use Neo4j. If you have existing FalkorDB setups, please migrate to Neo4j.

## Overview

This example provides a complete local development environment for the GraphRAG Toolkit's lexical-graph functionality. The environment runs entirely in Docker with Jupyter Lab for interactive development, Neo4j for graph storage, and PostgreSQL with pgvector for vector embeddings.

## Notebooks

- [**00-Setup**](./notebooks/00-Setup.ipynb) – Environment setup, package installation, and development mode configuration
- [**01-Combined-Extract-and-Build**](./notebooks/01-Combined-Extract-and-Build.ipynb) – Complete extraction and building pipeline using `LexicalGraphIndex.extract_and_build()`
- [**02-Querying**](./notebooks/02-Querying.ipynb) – Graph querying examples using `LexicalGraphQueryEngine` with various retrievers
- [**03-Querying with prompting**](./notebooks/03-Querying%20with%20prompting.ipynb) – Advanced querying with custom prompts and prompt providers
- [**04-Advanced-Configuration-Examples**](./notebooks/04-Advanced-Configuration-Examples.ipynb) – Advanced reader configurations and metadata handling
- [**05-S3-Directory-Reader-Provider**](./notebooks/05-S3-Directory-Reader-Provider.ipynb) – S3-based document reading and processing

## Quick Start

### 1. Start the Environment

**Standard (x86/Intel):**
```bash
cd docker
./start-containers.sh
```

**Mac/ARM (Apple Silicon):**
```bash
cd docker
./start-containers.sh --mac
```

**Development Mode (Hot-Code-Injection):**
```bash
cd docker
./start-containers.sh --dev --mac   # Enable live code editing
```

### 2. Access Jupyter Lab

Open your browser to: **http://localhost:8889**

- No password required
- Navigate to the `work` folder to find notebooks
- All dependencies are pre-installed

### 3. Run the Setup Notebook

Start with `00-Setup.ipynb` to configure your environment and verify all services are working.

## Docker Scripts

### Available Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `start-containers.sh` | Unix/Linux/Mac | Main startup script with all options |
| `start-containers.ps1` | Windows PowerShell | PowerShell version with same functionality |
| `start-containers.bat` | Windows CMD | Command prompt version |
| `dev-start.sh` | Unix/Linux/Mac | Development mode startup |
| `dev-reset.sh` | Unix/Linux/Mac | Reset development environment |

### Script Options

| Flag | Description |
|------|-------------|
| `--mac` | Use ARM/Apple Silicon optimized containers |
| `--dev` | Enable development mode with hot-code-injection |
| `--reset` | Reset all data and rebuild containers |

### Examples

```bash
# Standard startup
./start-containers.sh

# Apple Silicon Mac
./start-containers.sh --mac

# Development mode with hot-reload
./start-containers.sh --dev --mac

# Reset everything and start fresh
./start-containers.sh --reset --mac

# Windows PowerShell
.\start-containers.ps1 -Mac -Dev

# Windows Command Prompt
start-containers.bat --mac --dev
```

## Services

After startup, the following services are available:

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Jupyter Lab** | http://localhost:8889 | None required | Interactive development |
| **Neo4j Browser** | http://localhost:7476 | neo4j/password | Graph database management |
| **PostgreSQL** | localhost:5432 | graphrag/graphragpass | Vector storage |

## Development Mode

Development mode enables hot-code-injection for active lexical-graph development:

```bash
./start-containers.sh --dev --mac
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
./start-containers.sh --reset --mac
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

Key environment variables (configured in `docker/.env`):

```bash
# Database connections (Docker internal names)
VECTOR_STORE="postgresql://graphrag:graphragpass@postgres:5432/graphrag_db"
GRAPH_STORE="bolt://neo4j:password@neo4j:7687"

# AWS Configuration (optional)
AWS_REGION="us-east-1"
AWS_PROFILE="your-profile"

# Model Configuration
EMBEDDINGS_MODEL="cohere.embed-english-v3"
EXTRACTION_MODEL="us.anthropic.claude-3-5-sonnet-20240620-v1:0"
```

## Troubleshooting

### Common Issues

**Port conflicts:**
- Jupyter: 8889 (not 8888)
- Neo4j HTTP: 7476 (not 7474)
- Neo4j Bolt: 7687
- PostgreSQL: 5432

**Container networking:**
- Use container names in connection strings (e.g., `neo4j:7687`, not `localhost:7687`)
- The `.env` file uses Docker internal networking

**Development mode:**
- Restart Jupyter kernel after enabling hot-reload
- Check that lexical-graph source is mounted at `/home/jovyan/lexical-graph-src`

### Reset Environment

If you encounter persistent issues:

```bash
# Stop and remove everything
docker-compose down -v

# Start fresh
./start-containers.sh --reset --mac
```

## AWS Foundation Model Access (Optional)

For AWS Bedrock integration, ensure your AWS account has access to:
- `anthropic.claude-3-5-sonnet-20240620-v1:0`
- `cohere.embed-english-v3`

Enable model access via the [Bedrock model access console](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html).

## Migration from FalkorDB

If you have existing FalkorDB configurations:

1. **Update connection strings** to use Neo4j format:
   ```bash
   # Old FalkorDB
   GRAPH_STORE="falkordb://localhost:6379"
   
   # New Neo4j
   GRAPH_STORE="bolt://neo4j:password@neo4j:7687"
   ```

2. **Update imports** in your code:
   ```python
   # Replace FalkorDB imports with Neo4j
   from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
   GraphStoreFactory.register(Neo4jGraphStoreFactory)
   ```

3. **Migrate data** if needed (contact support for migration tools)

---

This local development environment provides everything needed to develop, test, and experiment with GraphRAG lexical-graph functionality without requiring AWS infrastructure.