# Changelog

All notable changes to the GraphRAG Toolkit will be documented in this file.

## [Unreleased] - 2025-08-02

> **Major Release**: This release introduces a complete reader provider system, migrates all examples to Jupyter Lab environments, and adds revolutionary development mode capabilities.

### 🚨 Breaking Changes

- **REMOVED FalkorDB Support**: FalkorDB has been completely removed and replaced with Neo4j as the primary graph database
  - All connection strings now use Neo4j format: `neo4j://neo4j:password@host:7687`
  - Updated all examples and documentation to use Neo4j
  - Migration guide provided in example READMEs

### ✨ New Features

#### Complete Reader Provider System (NEW)
> **All reader providers are completely new additions to the GraphRAG Toolkit**

- **StructuredDataReaderProvider**: Comprehensive reader for CSV, Excel, JSON, and JSONL files
  - File-type-specific pandas configuration filtering
  - S3 streaming support for large files (configurable threshold)
  - Universal S3 URL support alongside local files
  - Enhanced metadata extraction with file type detection

- **PDFReaderProvider**: PDF document processing with S3 streaming capabilities
- **DocxReaderProvider**: Word document processing with S3 integration
- **PPTXReaderProvider**: PowerPoint presentation processing with S3 support
- **MarkdownReaderProvider**: Markdown file processing with S3 compatibility
- **CSVReaderProvider**: CSV processing with S3 streaming
- **JSONReaderProvider**: JSON/JSONL processing with S3 support
- **WebReaderProvider**: Web page scraping and processing
- **YouTubeReaderProvider**: YouTube transcript extraction
- **WikipediaReaderProvider**: Wikipedia article processing
- **GitHubReaderProvider**: GitHub repository and file processing
- **S3DirectoryReaderProvider**: Direct S3 bucket and object processing
- **DirectoryReaderProvider**: Local directory traversal and processing
- **DatabaseReaderProvider**: SQL database integration
- **DocumentGraphReaderProvider**: Document graph processing

#### Universal S3 Support (NEW)
- **S3FileMixin**: Universal S3 integration for all file-based readers
  - Automatic S3 URL detection and temporary file handling
  - Streaming support for large files to avoid local storage issues
  - Presigned URL generation for secure streaming access
  - File size-based streaming decisions (configurable threshold)
  - Seamless local/S3 file mixing in single operations

#### Advanced Metadata System (NEW)
- **File-type-specific metadata extraction**: Tailored metadata for each document type
- **S3 metadata detection**: Automatic source type identification
- **Advanced file metadata**: File size, timestamps, and processing metadata
- **YouTube metadata extraction**: Video ID and platform information
- **Universal metadata patterns**: Consistent metadata across all readers
- **Custom metadata functions**: User-defined metadata extraction

### 🔧 Improvements

#### Complete Environment Migration to Jupyter (NEW)
> **Complete redesign of example environments from scratch**

- **Jupyter Lab Integration**: All examples now run in containerized Jupyter Lab environments
  - Interactive notebook-based development
  - Pre-installed dependencies and configurations
  - Seamless integration with graph and vector stores
  - No password authentication for development ease

- **Development Mode (`--dev`) (NEW)**: Hot-code-injection for live lexical-graph development
  - Mounts local lexical-graph source code into containers
  - Automatic module reloading on code changes
  - Editable package installation for immediate testing
  - No container rebuilds needed for code modifications
  - Perfect for contributing to lexical-graph development

#### Docker Environment Architecture (NEW)
- **Multi-architecture support**: Native ARM (Apple Silicon) and x86 container images
- **Port conflict resolution**: Separate port ranges for local-dev vs hybrid-dev
- **Enhanced startup scripts**: Comprehensive options with user guidance
- **Container orchestration**: Coordinated Neo4j, PostgreSQL, and Jupyter services

#### Example Environment Redesign
- **lexical-graph-local-dev (REDESIGNED)**: Complete local development environment
  - Jupyter Lab with hot-code-injection support
  - Neo4j 5.25-community with APOC plugin
  - PostgreSQL with pgvector for embeddings
  - Development mode for live coding
  - Comprehensive setup and reader examples
  
- **lexical-graph-hybrid-dev (NEW)**: Hybrid local/cloud development environment
  - Local Jupyter development with AWS cloud integration
  - AWS Bedrock batch processing capabilities
  - S3-based document storage and processing
  - Cloud-native prompt management
  - Comprehensive AWS setup automation

#### Documentation Updates
- **Reader documentation**: Complete guide to all reader providers with S3 support
- **Setup notebooks**: Enhanced 00-Setup.ipynb with development mode detection
- **Migration guides**: Detailed FalkorDB to Neo4j migration instructions
- **Troubleshooting sections**: Common issues and solutions for both environments

### 🐛 Bug Fixes

#### Reader Provider Fixes
- **Pandas configuration filtering**: Prevents CSV-specific parameters from being passed to Excel readers
- **Path object conversion**: Fixed StructuredDataReader compatibility with pathlib.Path objects
- **Import error handling**: Better error messages for missing dependencies
- **File extension detection**: Improved file type detection for S3 URLs

#### Docker Environment Fixes
- **Container networking**: Fixed connection strings to use Docker internal names
- **Port conflicts**: Resolved conflicts between local-dev and hybrid-dev environments
- **Neo4j warnings suppression**: Added configuration to reduce verbose logging
- **Environment variable handling**: Proper defaults and validation

### 📚 Documentation

#### New Documentation
- **docs/lexical-graph/readers.md**: Comprehensive reader provider documentation
  - Universal S3 support explanation
  - Configuration examples for all readers
  - Installation requirements and dependencies
  - Custom reader development guide

#### Updated Documentation
- **examples/lexical-graph-local-dev/README.md**: Complete rewrite with current features
- **examples/lexical-graph-hybrid-dev/README.md**: New comprehensive hybrid environment guide
- **Migration guides**: FalkorDB to Neo4j migration instructions

### 🏗️ Infrastructure

#### Database Migration
- **Neo4j Integration**: Complete replacement of FalkorDB with Neo4j
  - Neo4j 5.25-community with APOC plugin
  - Updated connection strings and factory registrations
  - Enhanced graph store capabilities

#### Development Environment (COMPLETELY NEW)
- **Hot-code-injection (`--dev` flag)**: Revolutionary live development experience
  - Mount local lexical-graph source directly into Jupyter containers
  - Immediate reflection of code changes without restarts
  - Automatic module reloading and dependency management
  - Perfect for lexical-graph contributors and advanced users
- **Multi-platform support**: Native ARM and x86 Docker images
- **Enhanced debugging**: Better error messages and logging configuration
- **Jupyter-first approach**: All development happens in interactive notebooks

### 🔄 Migration Guide

#### From FalkorDB to Neo4j
1. **Update connection strings**:
   ```bash
   # Old
   GRAPH_STORE="falkordb://localhost:6379"
   # New
   GRAPH_STORE="neo4j://neo4j:password@neo4j:7687"
   ```

2. **Update imports**:
   ```python
   # Replace FalkorDB imports
   from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
   GraphStoreFactory.register(Neo4jGraphStoreFactory)
   ```

3. **Update Docker configurations**: Use new Neo4j-based compose files

#### Reader Provider Adoption (NEW SYSTEM)
> **Note: Reader providers are entirely new - no migration needed from previous versions**

- **Replace direct LlamaIndex usage**: Use GraphRAG reader providers for better integration
- **Adopt S3 support**: Utilize both local and S3 URLs in file paths
- **Use configuration classes**: Implement reader-specific configuration patterns
- **Leverage universal metadata**: Take advantage of consistent metadata across readers

### 📦 Dependencies

#### New Dependencies
- **llama-index-readers-structured-data**: For enhanced structured data processing
- **llama-index-readers-s3**: For S3 directory reading capabilities
- **pandas**: Enhanced structured data processing
- **openpyxl**: Excel file processing support

#### Updated Dependencies
- **Neo4j**: Updated to version 5.25-community
- **Docker base images**: Multi-architecture support

---

### Notes

- **Breaking Change**: This release contains breaking changes due to FalkorDB removal
- **Migration Required**: Existing FalkorDB configurations must be migrated to Neo4j
- **All Readers Are New**: The entire reader provider system is a new addition to GraphRAG Toolkit
- **Environment Redesign**: Example environments completely redesigned around Jupyter Lab
- **Development Revolution**: `--dev` mode enables unprecedented live development experience
- **Enhanced Capabilities**: S3 streaming and universal file support significantly improves scalability
- **Developer Experience**: Hot-code-injection and Jupyter-first approach dramatically improves productivity

For detailed setup instructions and development mode usage, see the README files in the respective example directories.