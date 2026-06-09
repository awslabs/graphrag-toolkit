# Lexical Graph Hybrid Development

> **⚠️ IMPORTANT NOTICE**: FalkorDB support has been **removed** and replaced with **Neo4j** as the primary graph database. All examples and configurations now use Neo4j.

## Overview

This example provides a hybrid development environment that combines local Docker-based development with AWS cloud services. It's designed for developers who want to test lexical-graph functionality locally while leveraging AWS Bedrock for LLM processing and S3 for data storage.

## Notebooks

- [**00-Setup**](./notebooks/00-Setup.ipynb) – Environment setup, package installation, and development mode configuration
- [**01-Local-Extract-Batch**](./notebooks/01-Local-Extract-Batch.ipynb) – Local batch extraction with S3 storage integration
- [**02-Cloud-Setup**](./notebooks/02-Cloud-Setup.ipynb) – AWS cloud infrastructure setup and configuration
- [**03-Cloud-Build**](./notebooks/03-Cloud-Build.ipynb) – Cloud-based graph building with Bedrock batch processing
- [**04-Cloud-Querying**](./notebooks/04-Cloud-Querying.ipynb) – Advanced querying with cloud-based prompt management

## Quick Start

> All commands below should be executed from the `lexical-graph-hybrid-dev/` directory.

### 1. AWS Prerequisites

Before starting, ensure you have:
- [AWS CLI configured with credentials](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html) — verify with `aws sts get-caller-identity`
- Access to Amazon Bedrock models:
  - `us.anthropic.claude-sonnet-4-6` (extraction, response, evaluation)
  - `cohere.embed-english-v3` (embeddings)

### 2. Create AWS Resources (Optional — for batch inference)

Run the setup script to create the S3 bucket, DynamoDB table, and IAM role:

```bash
cd aws
bash setup-bedrock-batch.sh [your-profile]
```

This creates `graphrag-toolkit-<ACCOUNT_ID>` (S3), `graphrag-toolkit-batch-table` (DynamoDB), and `bedrock-batch-inference-role` (IAM).

### 3. Configure Environment

```bash
cp notebooks/.env.template notebooks/.env
```

Edit `notebooks/.env` — set your account ID and S3 bucket name:
```bash
AWS_ACCOUNT=123456789012
S3_BUCKET_NAME=graphrag-toolkit-123456789012
```

All other values (models, DynamoDB, IAM role) match the setup script defaults.

### 4. Start the Environment

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

### 5. Access Jupyter Lab

Open your browser to: **http://localhost:8889** (or **http://localhost:8890** for dev mode)

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

# Development mode
./start-containers.sh --dev

# Reset everything
./start-containers.sh --reset

# Reset with dev mode
./start-containers.sh --dev --reset
```

## Services

After startup, the following services are available:

| Service | Standard URL | Dev URL | Credentials | Purpose |
|---------|-------------|---------|-------------|---------|
| **Jupyter Lab** | http://localhost:8889 | http://localhost:8890 | None required | Interactive development |
| **Neo4j Browser** | http://localhost:7475 | http://localhost:7476 | neo4j/password | Graph database management |
| **PostgreSQL** | localhost:5433 | localhost:5434 | postgres/password | Vector storage |

> **Note**: Ports are different from local-dev to avoid conflicts when running both environments simultaneously. Dev mode uses separate ports to allow running standard and dev containers side by side.

## AWS Integration

### Setup Scripts

The `aws/` directory contains setup scripts for cloud infrastructure:

- `setup-bedrock-batch.sh` - Creates S3 bucket, DynamoDB table, and IAM role
- `create_custom_prompt.sh` - Sets up Bedrock prompt management
- `create_prompt_role.sh` - Creates IAM roles for prompt access

See [`notebooks/.env.template`](./notebooks/.env.template) for all available configuration options.

### S3 Integration

The hybrid environment uses S3 for:
- **Document storage**: Extracted documents and metadata
- **Batch processing**: Input/output files for Bedrock batch jobs
- **Checkpointing**: Progress tracking and resume capabilities

## Development Mode

Enable development mode for active lexical-graph development:

```bash
./start-containers.sh --dev
```

**Features:**
- Mounts local `lexical-graph/` source code
- Hot-code-injection for immediate changes
- Auto-reload in notebooks
- No container rebuilds needed

## Database Configuration

### Neo4j (Graph Store)
- **Container**: `neo4j-hybrid`
- **URL**: `bolt://neo4j:password@neo4j-hybrid:7687`
- **Browser**: http://localhost:7475
- **Features**: APOC plugin enabled

### PostgreSQL (Vector Store)
- **Container**: `pgvector-hybrid`
- **URL**: `postgresql://postgres:password@pgvector-hybrid:5432/graphrag`
- **Extensions**: pgvector, pg_trgm enabled

## Reader Providers

The environment supports all GraphRAG reader providers with enhanced AWS integration:

### File-Based Readers with S3 Support
- **PDF, DOCX, PPTX**: Document processing with S3 streaming
- **CSV/Excel**: Structured data with large file streaming
- **Markdown, JSON**: Text-based document processing

### Cloud-Native Readers
- **S3 Directory**: Direct S3 bucket processing
- **Web**: URL-based document ingestion
- **GitHub**: Repository processing

### Example S3 Usage
```python
# Works with local files
docs = reader.read('/local/path/file.pdf')

# Also works with S3 URLs
docs = reader.read('s3://my-bucket/documents/file.pdf')

# Automatic streaming for large files
config = StructuredDataReaderConfig(
    stream_s3=True,
    stream_threshold_mb=100
)
```

## Batch Processing

The hybrid environment supports AWS Bedrock batch processing for large-scale operations:

### Configuration
```python
batch_config = BatchConfig(
    region=os.environ["AWS_REGION"],
    bucket_name=os.environ["S3_BUCKET_NAME"],
    key_prefix=os.environ["BATCH_PREFIX"],
    role_arn=f'arn:aws:iam::{os.environ["AWS_ACCOUNT"]}:role/{os.environ["BATCH_ROLE_NAME"]}'
)
```

### Features
- **Automatic batching**: Groups documents for efficient processing
- **S3 integration**: Stores batch inputs/outputs in S3
- **Progress tracking**: DynamoDB-based job monitoring
- **Error handling**: Retry logic and failure recovery

## Automated Testing

Run all notebooks end-to-end with a single command:

```bash
bash tests/test-hybrid-dev-notebooks.sh
```

This handles the full lifecycle: environment setup, AWS resource creation, Docker containers, notebook execution, reporting, and cleanup.

Configuration options (environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_CUDA` | `true` | Skip GPU/CUDA cells |
| `SKIP_BATCH` | `true` | Skip batch processing cells |
| `CLEANUP` | `true` | Clean up all resources after run |
| `REPORT_DIR` | `test-results/` | Output directory for reports |

Reports are generated in `test-results/` (execution_report.json + execution_report.md).

## Troubleshooting

### Common Issues

**AWS Credentials:**
- Ensure AWS CLI is configured: `aws configure`
- Check profile access: `aws sts get-caller-identity --profile your-profile`

**S3 Bucket Access:**
- Verify bucket exists: `aws s3 ls s3://your-bucket-name`
- Check permissions for read/write access

**Bedrock Model Access:**
- Enable models in [Bedrock console](https://console.aws.amazon.com/bedrock/home#/modelaccess)
- Verify region availability for models

**Container Networking:**
- Use container names in connection strings (e.g., `neo4j-hybrid:7687`)
- Check port conflicts with local-dev environment

### Reset Environment

If you encounter persistent issues:

```bash
# Stop and remove everything
docker compose down -v

# Start fresh
./start-containers.sh --reset
```

## Migration from FalkorDB

If you have existing FalkorDB configurations:

1. **Update connection strings**:
   ```bash
   # Old FalkorDB
   GRAPH_STORE="falkordb://localhost:6379"
   
   # New Neo4j
   GRAPH_STORE="bolt://neo4j:password@neo4j-hybrid:7687"
   ```

2. **Update imports**:
   ```python
   from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
   GraphStoreFactory.register(Neo4jGraphStoreFactory)
   ```

## Cost Considerations

**AWS Services Used:**
- **Bedrock**: Pay-per-token for LLM processing
- **S3**: Storage and data transfer costs
- **DynamoDB**: Batch job tracking (minimal cost)

**Cost Optimization:**
- Use batch processing for large datasets
- Enable S3 streaming for large files
- Monitor Bedrock token usage
- Use appropriate instance types for compute