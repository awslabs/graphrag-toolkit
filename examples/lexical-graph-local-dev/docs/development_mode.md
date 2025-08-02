# Development Mode Guide

This document explains how to use development mode (`--dev`) for active lexical-graph development in the local environment.

---

## Overview

Development mode enables hot-code-injection, allowing you to modify the lexical-graph source code and see changes immediately in Jupyter notebooks without rebuilding containers.

---

## Enabling Development Mode

### Standard Startup
```bash
cd docker
./start-containers.sh --dev
```

### Apple Silicon Mac
```bash
cd docker
./start-containers.sh --dev --mac
```

### Windows PowerShell
```powershell
cd docker
.\start-containers.ps1 -Dev -Mac
```

---

## What Development Mode Does

### 1. Source Code Mounting
- Mounts `../../../lexical-graph` to `/home/jovyan/lexical-graph-src` in Jupyter container
- Provides direct access to lexical-graph source code
- Changes to source files are immediately visible

### 2. Editable Installation
- Installs lexical-graph package in editable mode (`pip install -e`)
- Python imports use the mounted source code
- No need to reinstall after code changes

### 3. Auto-Reload Configuration
- Enables IPython autoreload extension
- Automatically reloads modules when source changes
- Jupyter notebooks reflect changes immediately

---

## Development Workflow

### 1. Start Development Environment
```bash
./start-containers.sh --dev --mac
```

### 2. Access Jupyter Lab
Open http://localhost:8889 in your browser

### 3. Run Setup Notebook
Execute `00-Setup.ipynb` which will:
- Detect development mode automatically
- Install lexical-graph in editable mode
- Configure auto-reload
- **Important**: Restart kernel after setup

### 4. Verify Development Mode
```python
import os
dev_mode = os.path.exists('/home/jovyan/lexical-graph-src')
print(f"Development mode: {dev_mode}")

# Check if auto-reload is active
%load_ext autoreload
%autoreload 2
```

### 5. Make Changes
- Edit files in your local `lexical-graph/` directory
- Changes are immediately reflected in Jupyter
- No container rebuilds needed

---

## Directory Structure

```
lexical-graph-local-dev/
├── docker/
│   └── start-containers.sh --dev    # Enables dev mode
├── notebooks/                       # Your notebooks
└── ../../../lexical-graph/         # Source code (mounted in dev mode)
    ├── src/
    │   └── graphrag_toolkit/
    │       └── lexical_graph/       # Main package
    └── pyproject.toml
```

In Jupyter container:
```
/home/jovyan/
├── work/                           # notebooks/ mounted here
└── lexical-graph-src/             # lexical-graph/ mounted here (dev mode only)
    ├── src/
    │   └── graphrag_toolkit/
    └── pyproject.toml
```

---

## Development Mode Features

### Hot-Code-Injection
```python
# Edit lexical-graph source code in your IDE
# Changes are immediately available in Jupyter

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
# Uses your modified source code automatically
```

### Auto-Reload
```python
# In Jupyter notebook
%load_ext autoreload
%autoreload 2

# Now imports will automatically reload when source changes
from graphrag_toolkit.lexical_graph.indexing.load.readers import PDFReaderProvider
```

### Debugging
```python
# Add print statements or breakpoints to source code
# They work immediately in notebooks

# Example: Add debug print to a reader provider
# Edit: lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/load/readers/providers/pdf_reader_provider.py
# Add: print("DEBUG: Reading PDF file")
# Use in notebook immediately
```

---

## When to Use Development Mode

### ✅ Use Development Mode When:
- Contributing to lexical-graph package
- Testing new features or bug fixes
- Debugging lexical-graph functionality
- Rapid prototyping with modifications
- Developing new reader providers
- Experimenting with core functionality

### ❌ Don't Use Development Mode When:
- Just using lexical-graph as-is
- Running production workloads
- Following tutorials without modifications
- Performance testing (slight overhead)

---

## Troubleshooting Development Mode

### Issue: Development Mode Not Detected
```bash
# Check if source directory exists
ls -la ../../../lexical-graph

# Ensure proper directory structure
# lexical-graph-local-dev should be inside graphrag-toolkit/examples/
```

### Issue: Changes Not Reflected
```python
# In Jupyter, restart kernel and re-run setup
# Kernel → Restart Kernel

# Verify auto-reload is active
%load_ext autoreload
%autoreload 2

# Check installation mode
import graphrag_toolkit
print(graphrag_toolkit.__file__)
# Should show path to /home/jovyan/lexical-graph-src/...
```

### Issue: Import Errors
```python
# Reinstall in editable mode
!pip install -e /home/jovyan/lexical-graph-src

# Restart kernel after installation
```

### Issue: Permission Problems
```bash
# Ensure proper ownership of mounted directories
# On host system:
sudo chown -R $USER:$USER lexical-graph/
```

---

## Development Mode vs Standard Mode

| Feature | Standard Mode | Development Mode |
|---------|---------------|------------------|
| **Installation** | PyPI package | Editable source |
| **Code Changes** | Requires reinstall | Immediate |
| **Source Access** | No | Yes |
| **Performance** | Optimal | Slight overhead |
| **Use Case** | Production/Learning | Development |
| **Container Rebuild** | Sometimes needed | Never needed |

---

## Best Practices

### 1. Always Restart Kernel
After enabling development mode, restart the Jupyter kernel before continuing.

### 2. Use Auto-Reload
Enable auto-reload in notebooks that import lexical-graph modules:
```python
%load_ext autoreload
%autoreload 2
```

### 3. Verify Installation
Check that editable installation worked:
```python
import graphrag_toolkit
print(graphrag_toolkit.__file__)
# Should show mounted source path
```

### 4. Test Changes
Create simple test notebooks to verify your changes work as expected.

### 5. Clean Shutdown
Use proper shutdown to avoid container state issues:
```bash
./start-containers.sh --reset  # If problems occur
```

---

## Contributing Workflow

1. **Fork and Clone**: Fork graphrag-toolkit, clone locally
2. **Start Dev Mode**: `./start-containers.sh --dev --mac`
3. **Make Changes**: Edit source code in your IDE
4. **Test in Jupyter**: Verify changes work in notebooks
5. **Commit and Push**: Standard git workflow
6. **Create PR**: Submit pull request with changes

Development mode makes this workflow seamless by eliminating the need for package reinstallation during development.