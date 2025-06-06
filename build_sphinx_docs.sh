#!/bin/bash

# Exit immediately if any command fails
set -e

# -------------------------------
# Rebuild Sphinx Documentation
# -------------------------------
# This script prepares and builds the HTML documentation for the GraphRAG Toolkit,
# including both the core and FalkorDB-contrib source trees.
# It ensures all Python packages are recognized, removes old generated files,
# generates new API reference `.rst` files using `sphinx-apidoc`, and builds the docs.
# -------------------------------

# Base project directory (root of repository)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Paths to source code
SRC_MAIN="$PROJECT_ROOT/lexical-graph/src"
SRC_CONTRIB="$PROJECT_ROOT/lexical-graph-contrib/falkordb/src"

# Path to Sphinx documentation (unified doc site for both main and contrib modules)
DOCS_DIR="$PROJECT_ROOT/sphinx_docs"
DOCS_SOURCE="$DOCS_DIR/source"
DOCS_BUILD="$DOCS_DIR/build"

echo "Ensuring __init__.py files exist..."
# Make sure all Python directories are proper packages so Sphinx can import them
find "$SRC_MAIN/graphrag_toolkit" -type d -exec touch {}/__init__.py \;
find "$SRC_CONTRIB/graphrag_toolkit" -type d -exec touch {}/__init__.py \;

echo "Cleaning old .rst files..."
# Delete all previously auto-generated .rst files, except index.rst
find "$DOCS_SOURCE" -name "*.rst" ! -name "index.rst" -delete || true

echo "Generating autodoc .rst files..."
# Auto-generate API documentation .rst files for both main and contrib packages
sphinx-apidoc -f -o "$DOCS_SOURCE" "$SRC_MAIN/graphrag_toolkit"
sphinx-apidoc -f -o "$DOCS_SOURCE/falkordb" "$SRC_CONTRIB/graphrag_toolkit/lexical_graph/storage/graph/falkordb"

echo "Building HTML docs..."
# Build the HTML documentation using Sphinx
# PYTHONPATH includes both source trees to resolve imports
PYTHONPATH="$SRC_MAIN:$SRC_CONTRIB/graphrag_toolkit/lexical_graph/storage/graph" \
sphinx-build -b html -d "$DOCS_BUILD/doctrees" -j auto -v "$DOCS_SOURCE" "$DOCS_BUILD/html"

echo "Docs built successfully at: $DOCS_BUILD/html"