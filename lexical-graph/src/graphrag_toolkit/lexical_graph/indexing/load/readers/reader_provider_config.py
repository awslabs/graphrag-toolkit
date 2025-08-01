from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Callable
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig, AWSReaderConfigBase

# Document readers
@dataclass
class PDFReaderConfig(ReaderProviderConfig):
    return_full_document: bool = False
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class DocxReaderConfig(ReaderProviderConfig):
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class PPTXReaderConfig(ReaderProviderConfig):
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class MarkdownReaderConfig(ReaderProviderConfig):
    remove_hyperlinks: bool = True
    remove_images: bool = True
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class HTMLReaderConfig(ReaderProviderConfig):
    tag_to_ignore: Optional[List[str]] = None

@dataclass
class CSVReaderConfig(ReaderProviderConfig):
    concat_rows: bool = True
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class JSONReaderConfig(ReaderProviderConfig):
    is_jsonl: bool = False
    clean_json: bool = True
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class XMLReaderConfig(ReaderProviderConfig):
    tree_level_split: int = 0

# Document Graph reader
@dataclass
class DocumentGraphReaderConfig(ReaderProviderConfig):
    """Configuration for the Document Graph reader provider."""
    metadata_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None

# Web readers
@dataclass
class WebReaderConfig(ReaderProviderConfig):
    html_to_text: bool = False
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class RSSReaderConfig(ReaderProviderConfig):
    html_to_text: bool = True

# Database readers
@dataclass
class DatabaseReaderConfig(ReaderProviderConfig):
    connection_string: str = ""
    query: str = ""
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class MongoReaderConfig(ReaderProviderConfig):
    host: str = "localhost"
    port: int = 27017
    db_name: str = ""
    collection_name: str = ""

# Cloud storage readers
@dataclass
class S3DirectoryReaderConfig(AWSReaderConfigBase):
    bucket: str = ""
    key: Optional[str] = None       # for single file
    prefix: Optional[str] = None    # for directory
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

    def __post_init__(self):
        if not self.key and not self.prefix:
            raise ValueError("You must specify either `key` or `prefix`.")
        if self.key and self.prefix:
            raise ValueError("Only one of `key` or `prefix` may be set, not both.")


@dataclass
class GCSReaderConfig(ReaderProviderConfig):
    bucket: str = ""
    key: Optional[str] = None



@dataclass
class SlackReaderConfig(ReaderProviderConfig):
    slack_token: str = ""
    channel_ids: Optional[List[str]] = None

@dataclass
class DiscordReaderConfig(ReaderProviderConfig):
    discord_token: str = ""
    channel_ids: Optional[List[str]] = None

@dataclass
class TwitterReaderConfig(ReaderProviderConfig):
    bearer_token: str = ""
    num_tweets: int = 100

# Knowledge base readers
@dataclass
class WikipediaReaderConfig(ReaderProviderConfig):
    lang: str = "en"
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class YouTubeReaderConfig(ReaderProviderConfig):
    language: str = "en"
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class StructuredDataReaderConfig(ReaderProviderConfig):
    col_index: int = 0
    col_joiner: str = ', '
    col_metadata: Optional[Any] = None
    pandas_config: Optional[Dict[str, Any]] = None
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None
    stream_s3: bool = False  # Default to download, set True to stream
    stream_threshold_mb: int = 100  # Auto-stream files larger than this
    # S3 support - file_path can be local path or s3:// URL
    # AWS credentials handled via GraphRAGConfig.session

# Code readers
@dataclass
class GitHubReaderConfig(ReaderProviderConfig):
    github_token: Optional[str] = None
    verbose: bool = False
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

@dataclass
class DirectoryReaderConfig(ReaderProviderConfig):
    input_dir: str = ""
    exclude_hidden: bool = True
    recursive: bool = True
    required_exts: Optional[List[str]] = None
    metadata_fn: Optional[Callable[[str], Dict[str, Any]]] = None

# Email readers
@dataclass
class GmailReaderConfig(ReaderProviderConfig):
    credentials_path: str = ""
    token_path: str = ""
    use_iterative_parser: bool = False

@dataclass
class OutlookReaderConfig(ReaderProviderConfig):
    client_id: str = ""
    client_secret: str = ""
    tenant_id: str = ""