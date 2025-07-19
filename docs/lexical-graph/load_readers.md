# Using Custom Extract Providers

The GraphRAG Toolkit supports pluggable **extract providers** to allow structured ingestion of content from sources like S3, GitHub, PDFs, DOCX, PowerPoint, web pages, and more.

Each provider implements the `ExtractProvider` interface and can be dynamically loaded or used directly.

---

## Available Provider Types

| Type             | Class                     |
|------------------|---------------------------|
| `directory`      | DirectoryReaderProvider   |
| `s3_directory`   | S3DirectoryReaderProvider |
| `pdf`            | PDFReaderProvider         |
| `web`            | WebReaderProvider         |
| `youtube`        | YouTubeReaderProvider     |
| `github`         | GitHubReaderProvider      |
| `docx`           | DocxReaderProvider        |
| `pptx`           | PPTXReaderProvider        |

---

## DirectoryReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.directory_reader_provider import DirectoryReaderProvider

provider = DirectoryReaderProvider(
    data_dir="data/",
    recursive=True,
    required_exts=[".pdf", ".docx"],
    file_metadata=lambda path: {"source": path},
    filename_as_id=True,
    num_files_limit=10,
    exclude_hidden=True,
    exclude_empty=True,
    encoding="utf-8",
    errors="ignore"
)

docs = provider.read()
```

---

## S3DirectoryReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import S3DirectoryReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.s3_directory_reader_provider import S3DirectoryReaderProvider

config = S3DirectoryReaderConfig(
    bucket="my-bucket",
    prefix="documents/",
    region="us-east-1",
    profile="default",
    recursive=True,
    required_exts=[".pdf", ".pptx"],
    file_metadata=lambda path: {"source": f"s3://my-bucket/{path}"},
    filename_as_id=True,
    num_files_limit=100,
    exclude_hidden=True,
    exclude_empty=True,
    encoding="utf-8",
    errors="ignore"
)

provider = S3DirectoryReaderProvider(config=config)
docs = provider.read()
```

---

## PDFReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.pdf_reader_provider import PDFReaderProvider

provider = PDFReaderProvider(
    extract_images=True,
    extract_metadata=True,
    infer_table_struct=True,
    metadata_filename="pdf_metadata.json"
)

docs = provider.read(["docs/sample1.pdf", "docs/sample2.pdf"])
```

---

## WebReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.web_reader_provider import WebReaderProvider

urls = [
    "https://docs.aws.amazon.com/neptune/latest/userguide/intro.html",
    "https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html"
]

provider = WebReaderProvider(
    html_to_text=True,
    metadata_fn=lambda url: {"url": url},
    max_num_tokens=4096,
    continue_on_failure=True
)

docs = provider.read(urls)
```

---

## YouTubeReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.youtube_reader_provider import YouTubeReaderProvider

provider = YouTubeReaderProvider()
docs = provider.read("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
```

---

## GitHubReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.github_repo_provider import GitHubReaderProvider

provider = GitHubReaderProvider(
    github_token=os.environ.get("GITHUB_TOKEN"),
    default_branch="main",
    filter_directories=["src", "docs"],
    filter_file_extensions=[".py", ".md"],
    verbose=True
)

docs = provider.read("https://github.com/awslabs/graphrag-toolkit")
```

---

## DocxReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.docx_reader_provider import DocxReaderProvider

provider = DocxReaderProvider()
docs = provider.read(["docs/story1.docx", "docs/story2.docx"])
```

---

## PPTXReaderProvider

```python
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_providers.pptx_reader_provider import PPTXReaderProvider

provider = PPTXReaderProvider()
docs = provider.read(["slides/intro.pptx", "slides/overview.pptx"])
```