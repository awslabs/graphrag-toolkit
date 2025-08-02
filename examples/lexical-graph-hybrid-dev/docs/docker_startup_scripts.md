# Docker Startup Scripts

This document describes the Docker startup scripts available in the hybrid development environment.

---

## Available Scripts

### `start-containers.sh` (Primary Script)

Main startup script with comprehensive options:

```bash
./start-containers.sh [OPTIONS]
```

**Options:**
- `--mac`: Use ARM/Apple Silicon optimized containers
- `--dev`: Enable development mode with hot-code-injection
- `--reset`: Reset all data and rebuild containers

**Examples:**
```bash
# Standard startup
./start-containers.sh

# Apple Silicon Mac
./start-containers.sh --mac

# Development mode with hot-reload
./start-containers.sh --dev --mac

# Reset everything and start fresh
./start-containers.sh --reset --mac
```

### `build.sh`

Simple build and start script for initial deployments:

```bash
./build.sh
```

**What it does:**
- Executes `docker compose up -d --build`
- Builds Docker images from Dockerfiles
- Starts services in detached mode
- Does not remove existing data or volumes

### `reset.sh`

Full environment reset script:

```bash
./reset.sh
```

**What it does:**
- Stops and removes all containers
- Removes all volumes and data
- Cleans up networks and orphaned containers
- Rebuilds everything from scratch

**⚠️ Warning:** This script removes all persistent data

### Development Mode Scripts

#### `dev-start.sh`

Starts the environment in development mode:

```bash
./dev-start.sh
```

**Features:**
- Mounts local lexical-graph source code
- Enables hot-code-injection
- Configures auto-reload in Jupyter

#### `dev-reset.sh`

Resets the development environment:

```bash
./dev-reset.sh
```

**Features:**
- Preserves development mode configuration
- Cleans up development-specific volumes
- Rebuilds with source code mounting

---

## Windows Scripts

### PowerShell (`start-containers.ps1`)

```powershell
.\start-containers.ps1 [OPTIONS]
```

**Options:**
- `-Mac`: Use ARM/Apple Silicon containers
- `-Dev`: Enable development mode
- `-Reset`: Reset all data

### Command Prompt (`start-containers.bat`)

```cmd
start-containers.bat [OPTIONS]
```

**Options:**
- `--mac`: ARM/Apple Silicon support
- `--dev`: Development mode
- `--reset`: Full reset

---

## Development Mode

Development mode enables hot-code-injection for active lexical-graph development:

### Features
- **Source Code Mounting**: Local `lexical-graph/` directory mounted into containers
- **Hot-Reload**: Changes reflected immediately without rebuilds
- **Editable Installation**: Package installed in development mode
- **Auto-Reload**: Jupyter notebooks automatically reload modules

### Usage
```bash
# Enable development mode
./start-containers.sh --dev --mac

# Check if dev mode is active (in Jupyter)
import os
dev_mode = os.path.exists('/home/jovyan/lexical-graph-src')
print(f"Development mode: {dev_mode}")
```

### When to Use
- Contributing to lexical-graph package
- Testing local changes before commits
- Debugging lexical-graph functionality
- Rapid prototyping with modifications

---

## Environment Variables

Scripts use environment variables from `docker/.env`:

```bash
# Database connections (Docker internal names)
VECTOR_STORE="postgresql://postgres:password@pgvector-hybrid:5432/graphrag"
GRAPH_STORE="neo4j://neo4j:password@neo4j-hybrid:7687"

# AWS Configuration
AWS_REGION="us-east-1"
AWS_PROFILE="your-profile"

# Container Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=graphrag
```

---

## Troubleshooting

### Common Issues

**Port Conflicts:**
- Hybrid-dev uses ports 7475, 7688, 8889, 5433
- Local-dev uses ports 7476, 7687, 8889, 5432
- Use `--reset` flag if containers are in inconsistent state

**Development Mode Not Working:**
- Ensure lexical-graph source is available at `../../../lexical-graph`
- Check that containers have proper volume mounts
- Restart Jupyter kernel after enabling dev mode

**AWS Integration Issues:**
- Verify AWS credentials are mounted: `~/.aws:/home/jovyan/.aws`
- Check AWS profile configuration in `.env` file
- Ensure S3 bucket and IAM roles exist

### Reset Commands

```bash
# Full reset (removes all data)
./start-containers.sh --reset --mac

# Docker cleanup (if scripts fail)
docker-compose down -v --remove-orphans
docker system prune -f

# Restart fresh
./start-containers.sh --mac
```

---

## Service Access

After startup, services are available at:

| Service | URL | Credentials |
|---------|-----|-------------|
| Jupyter Lab | http://localhost:8889 | None required |
| Neo4j Browser | http://localhost:7475 | neo4j/password |
| PostgreSQL | localhost:5433 | postgres/password |

All development happens in Jupyter Lab at http://localhost:8889.