# Troubleshooting Guide

This document provides solutions to common issues in the local GraphRAG development environment.

---

## Docker Issues

### Container Startup Problems

**Issue: Port already in use**
```
Error: bind: address already in use
```

**Solution:**
```bash
# Check what's using the ports
lsof -i :8889  # Jupyter
lsof -i :7476  # Neo4j
lsof -i :5432  # PostgreSQL

# Stop conflicting services or use different ports
./start-containers.sh --reset
```

**Issue: Containers won't start**
```
Error: failed to create network
```

**Solution:**
```bash
# Clean up Docker resources
docker system prune -f
docker network prune -f
docker volume prune -f

# Restart Docker daemon (macOS)
# Docker Desktop → Restart

# Try again
./start-containers.sh --mac
```

### Volume and Data Issues

**Issue: Data not persisting**
```
Neo4j database empty after restart
```

**Solution:**
```bash
# Check volume status
docker volume ls | grep neo4j

# Ensure proper shutdown
docker-compose down  # Don't use -v flag

# Restart normally
./start-containers.sh --mac
```

**Issue: Permission denied errors**
```
Permission denied: '/home/jovyan/work'
```

**Solution:**
```bash
# Fix ownership on host
sudo chown -R $USER:$USER notebooks/

# Rebuild containers
./start-containers.sh --reset --mac
```

---

## Development Mode Issues

### Source Code Not Mounted

**Issue: Development mode not detected**
```python
dev_mode = os.path.exists('/home/jovyan/lexical-graph-src')
print(dev_mode)  # False
```

**Solution:**
```bash
# Ensure correct directory structure
# graphrag-toolkit/examples/lexical-graph-local-dev/
ls -la ../../../lexical-graph  # Should exist

# Start with --dev flag
./start-containers.sh --dev --mac
```

### Hot-Reload Not Working

**Issue: Code changes not reflected**
```python
# Changes to source code don't appear in notebooks
```

**Solution:**
```python
# In Jupyter notebook:
# 1. Restart kernel (Kernel → Restart Kernel)
# 2. Re-run setup cell
%load_ext autoreload
%autoreload 2

# 3. Verify editable installation
import graphrag_toolkit
print(graphrag_toolkit.__file__)
# Should show: /home/jovyan/lexical-graph-src/...
```

### Installation Issues

**Issue: Editable install fails**
```
ERROR: Could not install packages due to an EnvironmentError
```

**Solution:**
```python
# In Jupyter, try manual installation
!pip install -e /home/jovyan/lexical-graph-src --user

# Or reinstall from scratch
!pip uninstall graphrag-toolkit-lexical-graph -y
!pip install -e /home/jovyan/lexical-graph-src

# Restart kernel after installation
```

---

## Database Connection Issues

### Neo4j Connection Problems

**Issue: Cannot connect to Neo4j**
```
ServiceUnavailable: Failed to establish connection
```

**Solution:**
```python
# Check connection string in notebook
import os
print(os.environ.get('GRAPH_STORE'))
# Should be: bolt://neo4j:password@neo4j:7687

# Test connection
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
with driver.session() as session:
    result = session.run("RETURN 1")
    print(result.single()[0])  # Should print: 1
```

**Issue: Neo4j browser not accessible**
```
http://localhost:7476 not loading
```

**Solution:**
```bash
# Check container status
docker ps | grep neo4j

# Check logs
docker logs neo4j

# Restart if needed
docker restart neo4j
```

### PostgreSQL Connection Problems

**Issue: Cannot connect to PostgreSQL**
```
psycopg2.OperationalError: could not connect to server
```

**Solution:**
```python
# Check connection string
import os
print(os.environ.get('VECTOR_STORE'))
# Should be: postgresql://graphrag:graphragpass@postgres:5432/graphrag_db

# Test connection
import psycopg2
conn = psycopg2.connect(
    host="postgres",
    database="graphrag_db",
    user="graphrag",
    password="graphragpass"
)
print("PostgreSQL connection successful")
conn.close()
```

---

## Reader Provider Issues

### Import Errors

**Issue: Reader providers not found**
```python
ImportError: No module named 'graphrag_toolkit.lexical_graph.indexing.load.readers'
```

**Solution:**
```python
# Install missing dependencies
!pip install llama-index-readers-file pymupdf
!pip install llama-index-readers-structured-data pandas openpyxl
!pip install llama-index-readers-web requests beautifulsoup4

# Restart kernel after installation
```

### File Processing Errors

**Issue: PDF processing fails**
```
Error: Could not read PDF file
```

**Solution:**
```python
# Check file exists and is readable
import os
file_path = 'notebooks/artifacts/sample.pdf'
print(f"File exists: {os.path.exists(file_path)}")
print(f"File size: {os.path.getsize(file_path)} bytes")

# Try alternative PDF reader
from graphrag_toolkit.lexical_graph.indexing.load.readers import PDFReaderProvider, PDFReaderConfig

config = PDFReaderConfig(
    return_full_document=True,  # Try different settings
    metadata_fn=lambda path: {'source': 'pdf', 'file_path': path}
)
```

**Issue: CSV parsing errors**
```
pandas.errors.ParserError: Error tokenizing data
```

**Solution:**
```python
# Adjust pandas configuration
from graphrag_toolkit.lexical_graph.indexing.load.readers import StructuredDataReaderConfig

config = StructuredDataReaderConfig(
    pandas_config={
        "sep": ",",
        "encoding": "utf-8",
        "error_bad_lines": False,  # Skip bad lines
        "warn_bad_lines": True
    }
)
```

---

## Memory and Performance Issues

### Out of Memory Errors

**Issue: Jupyter kernel dies during processing**
```
The kernel appears to have died. It will restart automatically.
```

**Solution:**
```python
# Process files in smaller batches
def process_in_batches(files, batch_size=3):
    for i in range(0, len(files), batch_size):
        batch = files[i:i+batch_size]
        # Process batch
        docs = reader.read(batch)
        graph_index.extract_and_build(docs)
        
        # Clear memory
        import gc
        gc.collect()

# Monitor memory usage
import psutil
print(f"Memory usage: {psutil.virtual_memory().percent}%")
```

### Slow Processing

**Issue: Document processing is very slow**

**Solution:**
```python
# Reduce batch sizes
GraphRAGConfig.extraction_batch_size = 2  # Default is 4

# Use fewer workers
GraphRAGConfig.extraction_num_workers = 1  # Default is 2

# Process smaller documents first
files.sort(key=lambda x: os.path.getsize(x))
```

---

## Jupyter Lab Issues

### Notebook Not Loading

**Issue: Jupyter Lab shows blank page**
```
http://localhost:8889 loads but shows nothing
```

**Solution:**
```bash
# Check Jupyter logs
docker logs jupyter-notebook

# Try different browser or incognito mode
# Clear browser cache

# Restart Jupyter container
docker restart jupyter-notebook
```

### Kernel Issues

**Issue: Kernel won't start or keeps dying**

**Solution:**
```bash
# Check Jupyter container resources
docker stats jupyter-notebook

# Restart with more memory (if needed)
# Edit docker-compose.yml to add memory limits

# Clear Jupyter cache
docker exec -it jupyter-notebook rm -rf /home/jovyan/.jupyter/runtime/*
```

---

## Environment Variable Issues

### Missing Configuration

**Issue: Environment variables not loaded**
```python
import os
print(os.environ.get('GRAPH_STORE'))  # None
```

**Solution:**
```python
# In Jupyter notebook, reload environment
%reload_ext dotenv
%dotenv

# Check .env file exists
!ls -la .env

# Manually load if needed
import os
from dotenv import load_dotenv
load_dotenv('.env')
```

---

## Network and Connectivity Issues

### Container Communication

**Issue: Services can't communicate**
```
Connection refused when connecting between containers
```

**Solution:**
```bash
# Check network status
docker network ls | grep lg_graphrag

# Inspect network
docker network inspect lg_graphrag_network

# Ensure all containers are on same network
docker ps --format "table {{.Names}}\t{{.Networks}}"
```

### External Network Access

**Issue: Cannot access external URLs from Jupyter**
```
requests.exceptions.ConnectionError
```

**Solution:**
```python
# Test basic connectivity
import requests
try:
    response = requests.get('https://httpbin.org/get', timeout=10)
    print("External connectivity OK")
except Exception as e:
    print(f"Connectivity issue: {e}")

# Check DNS resolution
import socket
try:
    socket.gethostbyname('google.com')
    print("DNS resolution OK")
except Exception as e:
    print(f"DNS issue: {e}")
```

---

## Complete Reset Procedures

### Full Environment Reset

When all else fails, perform a complete reset:

```bash
# 1. Stop everything
docker-compose down -v --remove-orphans

# 2. Clean up Docker
docker system prune -f
docker volume prune -f
docker network prune -f

# 3. Remove any conflicting containers
docker rm -f $(docker ps -aq) 2>/dev/null

# 4. Clean local directories
rm -rf notebooks/extracted/
rm -rf notebooks/output/

# 5. Restart fresh
./start-containers.sh --reset --mac
```

### Selective Reset

Reset only specific components:

```bash
# Reset only databases (keep Jupyter)
docker stop neo4j postgres
docker rm neo4j postgres
docker volume rm neo4j_data pgvector_data

# Restart databases
docker-compose up -d neo4j postgres
```

---

## Getting Help

### Log Collection

When reporting issues, collect relevant logs:

```bash
# Container logs
docker logs neo4j > neo4j.log
docker logs postgres > postgres.log
docker logs jupyter-notebook > jupyter.log

# System information
docker version > system_info.txt
docker-compose version >> system_info.txt
uname -a >> system_info.txt
```

### Debug Information

Include this information when seeking help:

```python
# In Jupyter notebook
import sys, os, platform
print(f"Python version: {sys.version}")
print(f"Platform: {platform.platform()}")
print(f"Working directory: {os.getcwd()}")
print(f"Environment variables:")
for key in ['GRAPH_STORE', 'VECTOR_STORE']:
    print(f"  {key}: {os.environ.get(key)}")

# Package versions
!pip list | grep -E "(graphrag|llama|neo4j|psycopg)"
```

This troubleshooting guide covers the most common issues in the local development environment. For additional help, check the main README.md or create an issue in the repository.