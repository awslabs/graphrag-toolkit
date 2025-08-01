"""
GraphRAG Toolkit Reader Providers

Provides a unified interface for reading documents from various sources.
All providers inherit from ReaderProvider and use ReaderProviderConfig.
"""

from .reader_provider_base import ReaderProvider
from .reader_provider_config_base import ReaderProviderConfig, AWSReaderConfigBase
from .llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from .s3_file_mixin import S3FileMixin
from .reader_provider_config import *
from .providers import *

__all__ = [
    "ReaderProvider",
    "ReaderProviderConfig", 
    "AWSReaderConfigBase",
    "LlamaIndexReaderProviderBase",
    "S3FileMixin",
    # Config classes - Documents
    "PDFReaderConfig",
    "DocxReaderConfig",
    "PPTXReaderConfig",
    "MarkdownReaderConfig",
    "HTMLReaderConfig",
    "CSVReaderConfig",
    "JSONReaderConfig",
    "XMLReaderConfig",
    "DocumentGraphReaderConfig",
    # Config classes - Web
    "WebReaderConfig",
    "RSSReaderConfig",
    # Config classes - Knowledge base
    "WikipediaReaderConfig",
    "YouTubeReaderConfig",
    "StructuredDataReaderConfig",
    # Config classes - Code
    "GitHubReaderConfig", 
    "DirectoryReaderConfig",
    # Config classes - Cloud storage
    "S3ReaderConfig",
    "S3DirectoryReaderConfig",
    "AthenaReaderConfig",
    "GCSReaderConfig",
    # Config classes - API
    "NotionReaderConfig",
    "SlackReaderConfig",
    "DiscordReaderConfig",
    "TwitterReaderConfig",
    # Config classes - Database
    "DatabaseReaderConfig",
    "MongoReaderConfig",
    # Config classes - Email
    "GmailReaderConfig",
    "OutlookReaderConfig",
    # Provider classes - Documents
    "PDFReaderProvider",
    "DocxReaderProvider",
    "PPTXReaderProvider",
    "MarkdownReaderProvider",
    "CSVReaderProvider",
    "JSONReaderProvider",
    "DocumentGraphReaderProvider",
    # Provider classes - Web
    "WebReaderProvider",
    # Provider classes - Knowledge base
    "WikipediaReaderProvider",
    "YouTubeReaderProvider",
    "StructuredDataReaderProvider",
    # Provider classes - Code
    "GitHubReaderProvider",
    "DirectoryReaderProvider",
    # Provider classes - Cloud storage
    "S3DirectoryReaderProvider",
    "AthenaReaderProvider",
    # Provider classes - API
    "NotionReaderProvider",
    # Provider classes - Database
    "DatabaseReaderProvider"
]