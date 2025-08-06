# GraphRAG Reader Providers

## Overview
The GraphRAG Toolkit provides a unified, extensible system for reading documents from a wide variety of sources. Reader providers abstract the details of document ingestion, allowing you to work with files, databases, APIs, cloud storage, and more using a consistent interface.

## Architecture

### Core Abstractions
- **ReaderProvider**: The abstract base class for all document readers. Every concrete reader implements the `read(input_source)` method, returning a list of `Document` objects.
- **BaseReaderProvider**: Implements both the GraphRAG `ReaderProvider` and LlamaIndex `BaseReader` interfaces, providing compatibility and a standard pattern for new readers.
- **LlamaIndexReaderProviderBase**: A simple wrapper for LlamaIndex readers, making it easy to adapt existing LlamaIndex readers to the GraphRAG system.
- **ValidatedReaderProviderBase**: Extends `LlamaIndexReaderProviderBase` with input, output, and configuration validation.

### Configuration Classes
Each reader provider is paired with a configuration class (e.g., `PDFReaderConfig`, `WebReaderConfig`). These classes define the parameters required for each data source and use Python dataclasses for validation.

## How to Use

1. **Choose a provider and config** for your data source
2. **Instantiate the config** with the required parameters
3. **Create the provider** with the config
4. **Call `.read(input_source)`** to extract documents

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import PDFReaderProvider, PDFReaderConfig

config = PDFReaderConfig(
    return_full_document=False,
    metadata_fn=lambda path: {'source': 'pdf', 'file_path': path}
)
reader = PDFReaderProvider(config)
documents = reader.read("/path/to/file.pdf")
```

## Using Metadata with Readers

Many reader providers support attaching custom metadata to each document via the `metadata_fn` parameter in the configuration class. The function should accept an input and return a dictionary of metadata.

```python
def custom_metadata(path):
    return {
        "source": path,
        "document_type": "technical_doc",
        "project": "GraphRAG"
    }

config = PDFReaderConfig(
    return_full_document=False,
    metadata_fn=custom_metadata
)
```

## Built-in Providers

### Document Readers
| Provider | Config | Description | Dependencies |
|----------|--------|-------------|--------------|
| `PDFReaderProvider` | `PDFReaderConfig` | PDF documents | `pymupdf`, `llama-index-readers-file` |
| `DocxReaderProvider` | `DocxReaderConfig` | Word documents | `python-docx` |
| `PPTXReaderProvider` | `PPTXReaderConfig` | PowerPoint files | `python-pptx` |
| `MarkdownReaderProvider` | `MarkdownReaderConfig` | Markdown files | Built-in |
| `CSVReaderProvider` | `CSVReaderConfig` | CSV files | Built-in |
| `JSONReaderProvider` | `JSONReaderConfig` | JSON/JSONL files | Built-in |
| `StructuredDataReaderProvider` | `StructuredDataReaderConfig` | CSV/Excel files with streaming | `pandas`, `openpyxl`, `llama-index-readers-structured-data` |

### Web and Knowledge Base Readers
| Provider | Config | Description | Dependencies |
|----------|--------|-------------|--------------|
| `WebReaderProvider` | `WebReaderConfig` | Web pages | `requests`, `beautifulsoup4` |
| `WikipediaReaderProvider` | `WikipediaReaderConfig` | Wikipedia articles | `wikipedia` |
| `YouTubeReaderProvider` | `YouTubeReaderConfig` | YouTube transcripts | `youtube-transcript-api` |

### Cloud Storage Readers
| Provider | Config | Description | Dependencies |
|----------|--------|-------------|--------------|
| `S3DirectoryReaderProvider` | `S3DirectoryReaderConfig` | AWS S3 buckets | `boto3` |
| `DirectoryReaderProvider` | `DirectoryReaderConfig` | Local directories | Built-in |

### Database Readers
| Provider | Config | Description | Dependencies |
|----------|--------|-------------|--------------|
| `DatabaseReaderProvider` | `DatabaseReaderConfig` | SQL databases | Database-specific drivers |


### Code and Repository Readers
| Provider | Config | Description | Dependencies |
|----------|--------|-------------|--------------|
| `GitHubReaderProvider` | `GitHubReaderConfig` | GitHub repositories | `PyGithub` |


### Specialized Readers
| Provider | Config | Description | Dependencies |
|----------|--------|-------------|--------------|
| `DocumentGraphReaderProvider` | `DocumentGraphReaderConfig` | Document graphs | Built-in |


## S3 Support

The GraphRAG Toolkit provides two approaches for S3 integration:

### 1. S3DirectoryReaderProvider (Recommended)
Modern S3 reader using LlamaIndex's S3Reader for direct S3 access:

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import S3DirectoryReaderProvider, S3DirectoryReaderConfig

# For a single file
config = S3DirectoryReaderConfig(
    bucket="my-bucket",
    key="documents/file.pdf",  # Use 'key' for single file
    metadata_fn=lambda path: {'source': 's3'}
)

# For a directory/prefix
config = S3DirectoryReaderConfig(
    bucket="my-bucket",
    prefix="documents/",  # Use 'prefix' for directory
    metadata_fn=lambda path: {'source': 's3'}
)

# Note: Use either 'key' OR 'prefix', not both
reader = S3DirectoryReaderProvider(config)
docs = reader.read()
```

### 2. Legacy S3BasedDocs
Legacy system for S3 document storage and retrieval (still supported):

```python
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs

s3_docs = S3BasedDocs(
    region="us-east-1",
    bucket_name="my-bucket",
    key_prefix="documents/",
    collection_id="my-collection"
)

# Iterate through stored documents
for doc in s3_docs:
    # Process document
    pass
```

### S3 Authentication
S3 access uses `GraphRAGConfig.session` for AWS credentials. Configure via:
- AWS credentials file (`~/.aws/credentials`)
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- IAM roles (when running on AWS)
- AWS SSO profiles

### S3 Streaming for Large Files
The `StructuredDataReaderProvider` supports streaming large S3 files to avoid downloading:

```python
config = StructuredDataReaderConfig(
    stream_s3=True,  # Enable streaming
    stream_threshold_mb=100,  # Stream files > 100MB
    pandas_config={"sep": ","}
)
```

## Configuration Examples

### PDF Reader
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import PDFReaderProvider, PDFReaderConfig

config = PDFReaderConfig(
    return_full_document=False,
    metadata_fn=lambda path: {'source': 'pdf', 'file_path': path}
)
reader = PDFReaderProvider(config)
docs = reader.read('document.pdf')
```

### Web Reader
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import WebReaderProvider, WebReaderConfig

config = WebReaderConfig(
    html_to_text=True,
    metadata_fn=lambda url: {'source': 'web', 'url': url}
)
reader = WebReaderProvider(config)
docs = reader.read('https://example.com')
```

### YouTube Reader
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import YouTubeReaderProvider, YouTubeReaderConfig

config = YouTubeReaderConfig(
    language="en",
    metadata_fn=lambda url: {'source': 'youtube', 'url': url}
)
reader = YouTubeReaderProvider(config)
docs = reader.read('https://www.youtube.com/watch?v=VIDEO_ID')
```

### Structured Data Reader (CSV/Excel)
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import StructuredDataReaderProvider, StructuredDataReaderConfig

config = StructuredDataReaderConfig(
    col_index=0,  # Column to use as index
    col_joiner=', ',  # How to join columns
    pandas_config={"sep": ","},  # Pandas options
    stream_s3=True,  # Enable S3 streaming
    stream_threshold_mb=50,  # Stream files > 50MB
    metadata_fn=lambda path: {'source': 'structured', 'file': path}
)
reader = StructuredDataReaderProvider(config)

# Works with local and S3 files
docs = reader.read(['data.csv', 's3://bucket/large-file.xlsx'])
```

### S3 Directory Reader
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import S3DirectoryReaderProvider, S3DirectoryReaderConfig

# Reading from a directory/prefix
config = S3DirectoryReaderConfig(
    bucket="my-bucket",
    prefix="documents/",  # For directory access
    metadata_fn=lambda path: {'source': 's3', 'path': path}
)
reader = S3DirectoryReaderProvider(config)
docs = reader.read()  # No parameter needed

# Reading a single file
config = S3DirectoryReaderConfig(
    bucket="my-bucket",
    key="documents/specific-file.pdf",  # For single file
    metadata_fn=lambda path: {'source': 's3', 'path': path}
)
reader = S3DirectoryReaderProvider(config)
docs = reader.read()  # No parameter needed
```

### Database Reader
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import DatabaseReaderProvider, DatabaseReaderConfig

config = DatabaseReaderConfig(
    connection_string="postgresql://user:pass@localhost/db",
    query="SELECT id, content FROM documents",
    metadata_fn=lambda row: {'source': 'database', 'id': row.get('id')}
)
reader = DatabaseReaderProvider(config)
docs = reader.read(config.query)
```

## Installation Requirements

Different readers require different dependencies. Install as needed:

```bash
# PDF processing
pip install pymupdf llama-index-readers-file

# Web scraping
pip install requests beautifulsoup4 llama-index-readers-web

# YouTube transcripts
pip install youtube-transcript-api

# AWS services
pip install boto3

# Structured data processing
pip install pandas openpyxl llama-index-readers-structured-data

# Office documents
pip install python-docx python-pptx

# GitHub integration
pip install PyGithub

# Notion integration
pip install notion-client

# Wikipedia
pip install wikipedia
```

## Extending: Writing a Custom Reader

To add a new data source:

1. **Create a config class** as a dataclass:
```python
from dataclasses import dataclass
from typing import Optional, Callable, Dict, Any
from .reader_provider_config_base import ReaderProviderConfig

@dataclass
class MyReaderConfig(ReaderProviderConfig):
    api_key: str = ""
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None
```

2. **Subclass a base provider**:
```python
from .base_reader_provider import BaseReaderProvider

class MyReaderProvider(BaseReaderProvider):
    def __init__(self, config: MyReaderConfig):
        self.config = config
    
    def read(self, input_source):
        # Implement your reading logic
        documents = []
        # ... process input_source ...
        return documents
```

3. **Register in `__init__.py`** for easy importing.

## References
- [Base Classes](../../lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/load/readers/)
- [Configuration Classes](../../lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/load/readers/reader_provider_config.py)
- [Provider Implementations](../../lexical-graph/src/graphrag_toolkit/lexical_graph/indexing/load/readers/providers/)