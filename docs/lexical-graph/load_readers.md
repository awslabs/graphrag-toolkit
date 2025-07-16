# Using Custom Extract Providers

The GraphRAG Toolkit supports pluggable **extract providers** to allow structured ingestion of content from multiple source types such as S3, web, PDFs, GitHub, and more.

Each provider implements the `ExtractProvider` interface and can be dynamically loaded via the control plane or instantiated directly in code.

---

## Provider Categories

Extract providers are grouped into two categories:

* **Llama Providers** – Based on LlamaIndex readers (e.g., PDF, web, docx)

---

## Llama Providers

### 1. DirectoryReaderProvider

Use this provider to extract from a local directory with mixed file types (`.pdf`, `.docx`, `.html`, etc.).

```python
from graphrag_toolkit.lexical_graph.extract.llama_providers.directory_reader_provider import DirectoryReaderProvider

# Directory input path
directory_path = "soup/"  # This folder should contain mixed .pdf, .pptx, .docx, etc.

# Extract and build
provider = DirectoryReaderProvider(data_dir=directory_path)
docs = provider.extract()

graph_index.extract_and_build(docs, show_progress=True)
```

---

### 2. WebReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.web_reader_provider import WebReaderProvider

doc_urls = [
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
]

provider = WebReaderProvider()
docs = provider.read(doc_urls)

graph_index.extract_and_build(docs, show_progress=True)
```

---

### 3. PDFReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.pdf_reader_provider import PDFReaderProvider

# PDF Input
pdf_path = "pdf/sample.pdf"

# Extract and build
provider = PDFReaderProvider()
docs = provider.read(pdf_path)
```

---

### 4. YouTubeReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.youtube_reader_provider import YouTubeReaderProvider

# YouTube URL
youtube_url = "https://www.youtube.com/watch?v=YmR2_zlQO5w"

# Extract and build
provider = YouTubeReaderProvider()
docs = provider.read(youtube_url)
```

---

### 5. S3DirectoryReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import S3DirectoryReaderConfig

# S3 configuration
s3_config = S3DirectoryReaderConfig(
    bucket="rag-extract-188967239867",  # Replace it with your actual bucket
    prefix="soup/",                          # Folder inside the bucket
    region="us-east-1",                      # Optional, defaults to us-east-1
    profile=None                             # Optional, use None to rely on default credentials
)

# Extract and build
provider = S3DirectoryReaderProvider(config=s3_config)
docs = provider.read()
```

---

### 6. GitHubReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.github_repo_provider import GitHubReaderProvider

# GitHub repo (public)
github_repo = "https://github.com/awslabs/graphrag-toolkit"
branch = "main"

# Load optional GitHub token
github_token = os.getenv("GITHUB_TOKEN")

if github_token:
    print("Using authenticated GitHub access with token.")
else:
    print("No GITHUB_TOKEN found — using unauthenticated access. You may be rate-limited.")
```

---

### 7. DocxReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.docx_reader_provider import DocxReaderProvider

# DOCX input path
docx_path = "docs/story.docx"

# Extract and build
provider = DocxReaderProvider()
docs = provider.read(docx_path)
```

---

## 8. PPTXReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.pptx_reader_provider import PPTXReaderProvider

# PPTX input path
pptx_path = "pptx/sample.pptx"

# Extract and build
provider = PPTXReaderProvider()
docs = provider.read(pptx_path)
```

---

## Configuration-Driven Instantiation

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.web_reader_provider import WebReaderProvider

doc_urls = [
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
]

provider = WebReaderProvider()
docs = provider.read(doc_urls)
```

---

## Available Extract Provider Types

| Type             | Class                     |
|------------------| ------------------------- |
| `directory`      | DirectoryReaderProvider   |
| `s3_directory`   | S3DirectoryReaderProvider |
| `pdf`            | PDFReaderProvider         |
| `web`            | WebReaderProvider         |
| `youtube`        | YouTubeReaderProvider     |
| `github`         | GitHubRepoReaderProvider  |
| `docx`           | DocxReaderProvider        |
| `pptx`           | PPTXReaderProvider        |


---

## Full Example: Web Extraction + GraphRAG Indexing

```python
%reload_ext dotenv
%dotenv

import os

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, set_logging_config
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph.falkordb import FalkorDBGraphStoreFactory
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.web_reader_provider import WebReaderProvider

GraphStoreFactory.register(FalkorDBGraphStoreFactory)



graph_store = GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE'])
vector_store = VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE'])

graph_index = LexicalGraphIndex(
    graph_store,
    vector_store
)

doc_urls = [
    'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
    'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
]

provider = WebReaderProvider()
docs = provider.read(doc_urls)

graph_index.extract_and_build(docs, show_progress=True)

print('Complete')
```
