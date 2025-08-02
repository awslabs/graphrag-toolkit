# Reader Providers in Local Development

This document explains how to use the GraphRAG reader providers in the local development environment.

---

## Overview

The local development environment includes comprehensive support for all GraphRAG reader providers, allowing you to process various document types seamlessly within Jupyter notebooks.

---

## Available Reader Providers

### Document Readers
- **PDFReaderProvider**: PDF documents with text extraction
- **DocxReaderProvider**: Microsoft Word documents
- **PPTXReaderProvider**: PowerPoint presentations
- **MarkdownReaderProvider**: Markdown files
- **CSVReaderProvider**: CSV files with configurable parsing
- **JSONReaderProvider**: JSON and JSONL files
- **StructuredDataReaderProvider**: Advanced CSV/Excel processing

### Web and API Readers
- **WebReaderProvider**: Web page scraping
- **YouTubeReaderProvider**: YouTube transcript extraction
- **WikipediaReaderProvider**: Wikipedia articles
- **GitHubReaderProvider**: GitHub repositories and files

### Directory Readers
- **DirectoryReaderProvider**: Local directory traversal
- **S3DirectoryReaderProvider**: AWS S3 bucket processing (requires AWS credentials)

---

## Basic Usage Pattern

All reader providers follow a consistent pattern:

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import (
    ReaderProvider, ReaderConfig
)

# 1. Configure the reader
config = ReaderConfig(
    metadata_fn=lambda path: {'source': 'type', 'file_path': path}
)

# 2. Create the provider
reader = ReaderProvider(config)

# 3. Read documents
docs = reader.read('path/to/file')

# 4. Use with GraphRAG
graph_index.extract_and_build(docs, show_progress=True)
```

---

## Example Configurations

### PDF Processing
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import PDFReaderProvider, PDFReaderConfig

config = PDFReaderConfig(
    return_full_document=False,  # Split into chunks
    metadata_fn=lambda path: {
        'source': 'pdf',
        'file_path': path,
        'document_type': 'research_paper'
    }
)

pdf_reader = PDFReaderProvider(config)
docs = pdf_reader.read('notebooks/artifacts/sample.pdf')
```

### Structured Data (CSV/Excel)
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import (
    StructuredDataReaderProvider, StructuredDataReaderConfig
)

config = StructuredDataReaderConfig(
    pandas_config={"sep": ","},  # CSV-specific options
    col_index=0,  # Column to use as content
    col_joiner=', ',  # How to join multiple columns
    metadata_fn=lambda path: {
        'source': 'structured_data',
        'file_path': path,
        'data_type': 'tabular'
    }
)

structured_reader = StructuredDataReaderProvider(config)
docs = structured_reader.read('notebooks/artifacts/sample.csv')
```

### Web Content
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import WebReaderProvider, WebReaderConfig

config = WebReaderConfig(
    html_to_text=True,
    metadata_fn=lambda url: {
        'source': 'web',
        'url': url,
        'domain': url.split('/')[2] if '/' in url else 'unknown'
    }
)

web_reader = WebReaderProvider(config)
docs = web_reader.read([
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://example.com/article'
])
```

### Directory Processing
```python
from graphrag_toolkit.lexical_graph.indexing.load.readers import (
    DirectoryReaderProvider, DirectoryReaderConfig
)

config = DirectoryReaderConfig(
    input_dir='notebooks/dir_reader',
    recursive=True,
    required_exts=['.txt', '.md'],  # Only process these file types
    metadata_fn=lambda path: {
        'source': 'directory',
        'file_path': path,
        'directory': 'dir_reader'
    }
)

dir_reader = DirectoryReaderProvider(config)
docs = dir_reader.read('notebooks/dir_reader')
```

---

## Advanced Metadata Functions

### File-Based Metadata
```python
import datetime
from pathlib import Path

def advanced_file_metadata(file_path):
    """Extract detailed metadata from file."""
    path = Path(file_path)
    
    return {
        'source': 'file',
        'file_path': str(path),
        'file_name': path.name,
        'file_extension': path.suffix,
        'file_size': path.stat().st_size if path.exists() else 0,
        'created_date': datetime.datetime.fromtimestamp(
            path.stat().st_ctime
        ).isoformat() if path.exists() else None,
        'processing_date': datetime.datetime.now().isoformat()
    }

# Use with any file-based reader
config = PDFReaderConfig(metadata_fn=advanced_file_metadata)
```

### Content-Based Metadata
```python
def content_metadata(file_path):
    """Add content-specific metadata."""
    metadata = {'file_path': file_path}
    
    if 'research' in file_path.lower():
        metadata['category'] = 'research'
        metadata['priority'] = 'high'
    elif 'tutorial' in file_path.lower():
        metadata['category'] = 'educational'
        metadata['priority'] = 'medium'
    
    return metadata
```

---

## Working with Sample Data

The local environment includes sample files in `notebooks/artifacts/`:

```python
# Process all sample files
from pathlib import Path

artifacts_dir = Path('notebooks/artifacts')
readers = {
    '.pdf': PDFReaderProvider(PDFReaderConfig()),
    '.docx': DocxReaderProvider(DocxReaderConfig()),
    '.csv': StructuredDataReaderProvider(StructuredDataReaderConfig()),
    '.json': JSONReaderProvider(JSONReaderConfig()),
    '.md': MarkdownReaderProvider(MarkdownReaderConfig())
}

all_docs = []
for file_path in artifacts_dir.iterdir():
    if file_path.suffix in readers:
        reader = readers[file_path.suffix]
        docs = reader.read(str(file_path))
        all_docs.extend(docs)
        print(f"Processed {file_path.name}: {len(docs)} documents")

print(f"Total documents: {len(all_docs)}")
```

---

## Integration with GraphRAG

### Extract and Build Pipeline
```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint

# Setup extraction handler
extracted_docs = FileBasedDocs(docs_directory='extracted')
checkpoint = Checkpoint('reader-extraction-checkpoint')

# Create graph index
graph_index = LexicalGraphIndex(graph_store, vector_store)

# Process documents
docs = reader.read('path/to/documents')
graph_index.extract_and_build(
    docs, 
    handler=extracted_docs, 
    checkpoint=checkpoint, 
    show_progress=True
)
```

### Batch Processing Multiple Readers
```python
# Process different document types together
readers_config = {
    'pdf': (PDFReaderProvider, PDFReaderConfig()),
    'web': (WebReaderProvider, WebReaderConfig(html_to_text=True)),
    'structured': (StructuredDataReaderProvider, StructuredDataReaderConfig())
}

all_documents = []

# PDF documents
pdf_reader = PDFReaderProvider(PDFReaderConfig())
pdf_docs = pdf_reader.read('notebooks/artifacts/sample.pdf')
all_documents.extend(pdf_docs)

# Web content
web_reader = WebReaderProvider(WebReaderConfig(html_to_text=True))
web_docs = web_reader.read(['https://example.com'])
all_documents.extend(web_docs)

# Process all together
graph_index.extract_and_build(all_documents, show_progress=True)
```

---

## Troubleshooting

### Common Issues

**Import Errors:**
```python
# Ensure all dependencies are installed
!pip install llama-index-readers-file pymupdf
!pip install llama-index-readers-web requests beautifulsoup4
!pip install llama-index-readers-structured-data pandas openpyxl
```

**File Not Found:**
```python
# Check file paths relative to notebook location
import os
print("Current directory:", os.getcwd())
print("Available files:", os.listdir('notebooks/artifacts'))
```

**Memory Issues with Large Files:**
```python
# Process files in smaller batches
def process_in_batches(files, batch_size=5):
    for i in range(0, len(files), batch_size):
        batch = files[i:i+batch_size]
        docs = reader.read(batch)
        graph_index.extract_and_build(docs)
        print(f"Processed batch {i//batch_size + 1}")
```

### Performance Tips

1. **Use appropriate chunk sizes** for large documents
2. **Filter file types** to process only what you need
3. **Batch process** multiple files together
4. **Use metadata functions** to add context for better retrieval
5. **Monitor memory usage** with large document sets

---

## Best Practices

### 1. Consistent Metadata
Use consistent metadata schemas across different readers:

```python
def standard_metadata(source_path, doc_type):
    return {
        'source_path': source_path,
        'document_type': doc_type,
        'processing_timestamp': datetime.now().isoformat(),
        'environment': 'local-dev'
    }
```

### 2. Error Handling
Wrap reader operations in try-catch blocks:

```python
def safe_read(reader, path):
    try:
        return reader.read(path)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return []
```

### 3. Progress Tracking
Monitor processing progress for large document sets:

```python
from tqdm import tqdm

for file_path in tqdm(file_paths, desc="Processing files"):
    docs = reader.read(file_path)
    all_docs.extend(docs)
```

### 4. Validation
Validate documents before processing:

```python
def validate_docs(docs):
    valid_docs = []
    for doc in docs:
        if doc.text and len(doc.text.strip()) > 10:
            valid_docs.append(doc)
    return valid_docs
```

This comprehensive reader system enables flexible document processing in your local GraphRAG development environment.