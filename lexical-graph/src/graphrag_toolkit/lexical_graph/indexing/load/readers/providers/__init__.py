"""
Core reader providers following AWS Ask requirements.
All providers inherit from LlamaIndexReaderProviderBase and use config-based initialization.
"""

# Lazy import mapping to avoid import failures when dependencies are missing
_PROVIDER_MODULES = {
    # Document readers
    "PDFReaderProvider": ".pdf_reader_provider",
    "DocxReaderProvider": ".docx_reader_provider",
    "PPTXReaderProvider": ".pptx_reader_provider",
    "MarkdownReaderProvider": ".markdown_reader_provider",
    "CSVReaderProvider": ".csv_reader_provider",
    "JSONReaderProvider": ".json_reader_provider",
    "DocumentGraphReaderProvider": ".document_graph_reader_provider",
    # Web readers
    "WebReaderProvider": ".web_reader_provider",
    # Knowledge base readers
    "WikipediaReaderProvider": ".wikipedia_reader_provider",
    "YouTubeReaderProvider": ".youtube_reader_provider",
    "StructuredDataReaderProvider": ".structured_data_reader_provider",
    # Code readers
    "GitHubReaderProvider": ".github_reader_provider",
    "DirectoryReaderProvider": ".directory_reader_provider",
    # Cloud storage readers
    "S3DirectoryReaderProvider": ".s3_directory_reader_provider",


    # Database readers
    "DatabaseReaderProvider": ".database_reader_provider",
}

def __getattr__(name):
    """Lazy import providers only when accessed."""
    if name in _PROVIDER_MODULES:
        from importlib import import_module
        module = import_module(_PROVIDER_MODULES[name], package=__name__)
        return getattr(module, name)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = list(_PROVIDER_MODULES.keys())