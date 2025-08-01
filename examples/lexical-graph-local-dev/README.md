## Lexical Graph Local Development

### Notebooks

  - [**00-Setup**](./notebooks/00-Setup.ipynb) – Installs the lexical-graph package and additional dependencies.
  - [**01-Combined Extract and Build**](./notebooks/01-Combined-Extract-and-Build.ipynb) – An example of [performing continuous ingest](../../docs/lexical-graph/indexing.md#continous-ingest) using the `LexicalGraphIndex.extract_and_build()` method.
  - [**02-Querying**](./notebooks/02-Querying.ipynb) – Examples of [querying the graph](../../docs/lexical-graph/querying.md) using the `LexicalGraphQueryEngine` with `TraversalBasedRetriever`.
  - [**03-Querying with prompting**](./notebooks/03-Querying%20with%20prompting.ipynb) – Advanced querying examples with custom prompts.
  - [**05-Reader-Providers**](./notebooks/05-Reader-Providers.ipynb) – Examples of different document reader providers.
  - [**06-Additional-Reader-Providers**](notebooks/04-Advanced-Configuration-Examples.ipynb) – Additional reader provider implementations.
  - [**07-Directory-Reader-Provider**](./notebooks/07-Directory-Reader-Provider.ipynb) – Directory-based document reading examples.
  - [**08-S3-Directory-Reader-Provider**](notebooks/05-S3-Directory-Reader-Provider.ipynb) – S3-based directory reading examples.
  - [**09-Extra-Readers**](./notebooks/09-Extra-Readers.ipynb) – Additional specialized reader implementations.
  
## Environment Setup

The development environment runs entirely in Jupyter Lab with Docker services. All code execution happens within the Jupyter container. This is the recommended environment for testing lexical-graph functionality.

### Starting the Environment

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
./start-containers.sh --dev        # Standard with dev mode
./start-containers.sh --dev --mac   # ARM with dev mode
```

**Reset Data and Rebuild:**
```bash
cd docker
./start-containers.sh --reset --mac  # Reset everything and start fresh
```

**Windows PowerShell:**
```powershell
cd docker
.\start-containers.ps1           # Standard
.\start-containers.ps1 -Mac      # ARM/Mac
.\start-containers.ps1 -Dev -Mac # Development mode
.\start-containers.ps1 -Reset    # Reset data
```

**Windows Command Prompt:**
```cmd
cd docker
start-containers.bat              # Standard
start-containers.bat --mac        # ARM/Mac
start-containers.bat --dev --mac  # Development mode
start-containers.bat --reset      # Reset data
```

This will start the following services:

- **Neo4j** for graph storage (accessible at `http://localhost:7476`, credentials: neo4j/password)
- **PostgreSQL with pgvector** for vector embeddings
- **Jupyter Lab** at `http://localhost:8889` for interactive development

### Accessing Jupyter Lab

After starting the containers, access Jupyter Lab at:
- **Jupyter Lab**: `http://localhost:8889`

The notebooks are automatically mounted in the `/home/jovyan/work` directory within the Jupyter container. All required packages are pre-installed in the Jupyter environment.

### Development Mode (Hot-Code-Injection)

For active development of the lexical-graph package, use the `--dev` flag to enable hot-code-injection:

```bash
cd docker
./start-containers.sh --dev --mac  # or just --dev for x86
```

**Development mode features:**
- Mounts the local `lexical-graph/` source code into the Jupyter container
- Changes to lexical-graph source code are immediately reflected in notebooks
- No need to rebuild containers or reinstall packages when modifying lexical-graph
- The `00-Dev-Setup.ipynb` notebook automatically detects and configures hot-code-injection

**When to use development mode:**
- Contributing to the lexical-graph package
- Testing local changes before submitting PRs
- Debugging lexical-graph functionality
- Rapid prototyping with lexical-graph modifications

### Data Persistence

By default, the environment preserves data between container restarts:
- Neo4j graph data persists in Docker volumes
- PostgreSQL vector data persists in Docker volumes
- Jupyter notebooks and user data persist

**To reset all data and start fresh:**
```bash
./start-containers.sh --reset --mac  # Unix/Mac
.\start-containers.ps1 -Reset -Mac   # PowerShell
start-containers.bat --reset --mac   # Windows CMD
```

### Database Configuration

The Postgres container auto-applies the following schema on initialization via `./postgres/schema.sql`:

```sql
-- Enable pgvector extension in public schema
CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;

-- Enable pg_trgm extension in public schema
CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;

-- Create schema for GraphRAG
CREATE SCHEMA IF NOT EXISTS graphrag;
```

These extensions are necessary for similarity search and fuzzy matching in GraphRAG.

## AWS Foundation Model Access (Optional)

If you intend to run the CloudFormation templates instead of using Docker:

- Ensure your AWS account has access to the following Amazon Bedrock foundation models:
  - `anthropic.claude-3-5-sonnet-20240620-v1:0`
  - `cohere.embed-english-v3`

Enable model access via the [Bedrock model access console](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html).

You must deploy to an AWS region where these models are available.

## Optional: CloudFormation Stacks

If you want to deploy infrastructure in AWS, CloudFormation templates are available:

- `graphrag-toolkit-neptune-db-opensearch-serverless.json`
- `graphrag-toolkit-neptune-db-aurora-postgres.json`

These templates create:

- A Neptune serverless DB cluster
- Either OpenSearch Serverless or Aurora PostgreSQL
- A SageMaker notebook instance
- IAM roles with optional policies via the `IamPolicyArn` parameter
- An optional `ExampleNotebooksURL` parameter to auto-load the examples

> ⚠️ AWS charges apply for cloud resources.

---

Use this guide if you prefer to develop and test locally before migrating to AWS-based deployments.