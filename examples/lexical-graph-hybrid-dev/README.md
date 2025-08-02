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

### 1. AWS Prerequisites

Before starting, ensure you have:
- AWS CLI configured with appropriate credentials
- Access to Amazon Bedrock models:
  - `anthropic.claude-3-5-sonnet-20240620-v1:0`
  - `cohere.embed-english-v3`
- S3 bucket for data storage
- IAM roles for batch processing (optional)

### 2. Configure Environment

Update `notebooks/.env` with your AWS settings:
```bash
AWS_REGION="us-east-1"
AWS_PROFILE="your-profile"
AWS_ACCOUNT="123456789012"
S3_BUCKET_EXTRACK_BUILD_BATCH_NAME="your-bucket-name"
```

### 3. Start the Environment

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
./start-containers.sh --dev --mac
```

### 4. Access Jupyter Lab

Open your browser to: **http://localhost:8889**

## Docker Scripts

### Available Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `start-containers.sh` | Unix/Linux/Mac | Main startup script with all options |
| `start-containers.ps1` | Windows PowerShell | PowerShell version |
| `start-containers.bat` | Windows CMD | Command prompt version |
| `dev-start.sh` | Unix/Linux/Mac | Development mode startup |
| `dev-reset.sh` | Unix/Linux/Mac | Reset development environment |
| `reset.sh` | Unix/Linux/Mac | Reset all containers and data |

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

# Development mode
./start-containers.sh --dev --mac

# Reset everything
./start-containers.sh --reset --mac

# Windows PowerShell
.\start-containers.ps1 -Mac -Dev
```

## Services

After startup, the following services are available:

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Jupyter Lab** | http://localhost:8889 | None required | Interactive development |
| **Neo4j Browser** | http://localhost:7475 | neo4j/password | Graph database management |
| **PostgreSQL** | localhost:5433 | postgres/password | Vector storage |

> **Note**: Ports are different from local-dev to avoid conflicts when running both environments simultaneously.

## AWS Integration

### Setup Scripts

The `aws/` directory contains setup scripts for cloud infrastructure:

- `setup-bedrock-batch.sh` - Creates S3 buckets, DynamoDB tables, and IAM roles
- `create_custom_prompt.sh` - Sets up Bedrock prompt management
- `create_prompt_role.sh` - Creates IAM roles for prompt access

### Environment Variables

Key AWS configuration variables in `notebooks/.env`:

```bash
# AWS Configuration
AWS_REGION="us-east-1"
AWS_PROFILE="your-profile"
AWS_ACCOUNT="123456789012"

# S3 Storage
S3_BUCKET_EXTRACK_BUILD_BATCH_NAME="your-bucket-name"
S3_BATCH_BUCKET_NAME="your-bucket-name"

# Bedrock Models
EXTRACTION_MODEL="us.anthropic.claude-3-5-sonnet-20240620-v1:0"
EMBEDDINGS_MODEL="cohere.embed-english-v3"

# Batch Processing
BATCH_ROLE_NAME="GraphRAGBatchRole"
DYNAMODB_NAME="graphrag-batch-table"
```

### S3 Integration

The hybrid environment uses S3 for:
- **Document storage**: Extracted documents and metadata
- **Batch processing**: Input/output files for Bedrock batch jobs
- **Checkpointing**: Progress tracking and resume capabilities

## Development Mode

Enable development mode for active lexical-graph development:

```bash
./start-containers.sh --dev --mac
```

**Features:**
- Mounts local `lexical-graph/` source code
- Hot-code-injection for immediate changes
- Auto-reload in notebooks
- No container rebuilds needed

## Database Configuration

### Neo4j (Graph Store)
- **Container**: `neo4j-hybrid`
- **URL**: `neo4j://neo4j:password@neo4j-hybrid:7687`
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
    bucket_name=os.environ["S3_BUCKET_EXTRACK_BUILD_BATCH_NAME"],
    key_prefix=os.environ["BATCH_PREFIX"],
    role_arn=f'arn:aws:iam::{os.environ["AWS_ACCOUNT"]}:role/{os.environ["BATCH_ROLE_NAME"]}'
)
```

### Features
- **Automatic batching**: Groups documents for efficient processing
- **S3 integration**: Stores batch inputs/outputs in S3
- **Progress tracking**: DynamoDB-based job monitoring
- **Error handling**: Retry logic and failure recovery

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
docker-compose down -v

# Start fresh
./start-containers.sh --reset --mac
```

## Migration from FalkorDB

If you have existing FalkorDB configurations:

1. **Update connection strings**:
   ```bash
   # Old FalkorDB
   GRAPH_STORE="falkordb://localhost:6379"
   
   # New Neo4j
   GRAPH_STORE="neo4j://neo4j:password@neo4j-hybrid:7687"
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

---

This hybrid environment provides the best of both worlds: local development speed with cloud-scale processing capabilities.