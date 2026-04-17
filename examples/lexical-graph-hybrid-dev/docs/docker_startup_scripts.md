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

Scripts use environment variables from [`notebooks/.env`](../notebooks/.env.template):

```bash
# Database connections (Docker internal names)
VECTOR_STORE="postgresql://postgres:password@pgvector-hybrid:5432/graphrag"
GRAPH_STORE="bolt://neo4j:password@neo4j-hybrid:7687"

# AWS Configuration
AWS_REGION=us-east-1
# AWS_PROFILE=default  # Optional — uncomment to use a specific profile

# Container Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=graphrag
```

---

## Troubleshooting

### Common Issues

**Port Conflicts:**
- Standard mode uses ports 7475, 7688, 8889, 5433
- Dev mode uses ports 7476, 7689, 8890, 5434
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