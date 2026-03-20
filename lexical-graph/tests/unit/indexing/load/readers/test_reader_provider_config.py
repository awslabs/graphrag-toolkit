# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import sys
from dataclasses import fields
from unittest.mock import Mock

# Mock the providers module to avoid loading optional dependencies
sys.modules['graphrag_toolkit.lexical_graph.indexing.load.readers.providers'] = Mock()

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
    PDFReaderConfig,
    DocxReaderConfig,
    PPTXReaderConfig,
    MarkdownReaderConfig,
    HTMLReaderConfig,
    CSVReaderConfig,
    JSONReaderConfig,
    XMLReaderConfig,
    DocumentGraphReaderConfig,
    WebReaderConfig,
    RSSReaderConfig,
    DatabaseReaderConfig,
    MongoReaderConfig,
    S3DirectoryReaderConfig,
    GCSReaderConfig,
    SlackReaderConfig,
    DiscordReaderConfig,
    TwitterReaderConfig,
    WikipediaReaderConfig,
    YouTubeReaderConfig,
    StructuredDataReaderConfig,
    GitHubReaderConfig,
    DirectoryReaderConfig,
    GmailReaderConfig,
    OutlookReaderConfig,
    UniversalDirectoryReaderConfig
)


class TestPDFReaderConfig:
    """Tests for PDFReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify PDFReaderConfig initializes with defaults."""
        config = PDFReaderConfig()
        
        assert config.return_full_document is False
        assert config.metadata_fn is None
    
    def test_initialization_with_custom_values(self):
        """Verify initialization with custom values."""
        def custom_fn(path):
            return {"source": path}
        
        config = PDFReaderConfig(
            return_full_document=True,
            metadata_fn=custom_fn
        )
        
        assert config.return_full_document is True
        assert config.metadata_fn == custom_fn


class TestDocxReaderConfig:
    """Tests for DocxReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify DocxReaderConfig initializes with defaults."""
        config = DocxReaderConfig()
        
        assert config.metadata_fn is None
    
    def test_initialization_with_metadata_fn(self):
        """Verify initialization with metadata function."""
        def metadata_fn(path):
            return {"type": "docx"}
        
        config = DocxReaderConfig(metadata_fn=metadata_fn)
        
        assert config.metadata_fn == metadata_fn


class TestMarkdownReaderConfig:
    """Tests for MarkdownReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify MarkdownReaderConfig initializes with defaults."""
        config = MarkdownReaderConfig()
        
        assert config.remove_hyperlinks is True
        assert config.remove_images is True
        assert config.metadata_fn is None
    
    def test_initialization_with_custom_values(self):
        """Verify initialization with custom values."""
        config = MarkdownReaderConfig(
            remove_hyperlinks=False,
            remove_images=False
        )
        
        assert config.remove_hyperlinks is False
        assert config.remove_images is False


class TestHTMLReaderConfig:
    """Tests for HTMLReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify HTMLReaderConfig initializes with defaults."""
        config = HTMLReaderConfig()
        
        assert config.tag_to_ignore is None
    
    def test_initialization_with_tags_to_ignore(self):
        """Verify initialization with tags to ignore."""
        config = HTMLReaderConfig(tag_to_ignore=["script", "style"])
        
        assert config.tag_to_ignore == ["script", "style"]


class TestCSVReaderConfig:
    """Tests for CSVReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify CSVReaderConfig initializes with defaults."""
        config = CSVReaderConfig()
        
        assert config.concat_rows is True
        assert config.metadata_fn is None
    
    def test_initialization_with_custom_values(self):
        """Verify initialization with custom values."""
        config = CSVReaderConfig(concat_rows=False)
        
        assert config.concat_rows is False


class TestJSONReaderConfig:
    """Tests for JSONReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify JSONReaderConfig initializes with defaults."""
        config = JSONReaderConfig()
        
        assert config.is_jsonl is False
        assert config.clean_json is True
        assert config.metadata_fn is None
    
    def test_initialization_with_jsonl_format(self):
        """Verify initialization with JSONL format."""
        config = JSONReaderConfig(is_jsonl=True, clean_json=False)
        
        assert config.is_jsonl is True
        assert config.clean_json is False


class TestXMLReaderConfig:
    """Tests for XMLReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify XMLReaderConfig initializes with defaults."""
        config = XMLReaderConfig()
        
        assert config.tree_level_split == 0
    
    def test_initialization_with_tree_level(self):
        """Verify initialization with tree level split."""
        config = XMLReaderConfig(tree_level_split=2)
        
        assert config.tree_level_split == 2


class TestWebReaderConfig:
    """Tests for WebReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify WebReaderConfig initializes with defaults."""
        config = WebReaderConfig()
        
        assert config.html_to_text is False
        assert config.metadata_fn is None
    
    def test_initialization_with_html_to_text(self):
        """Verify initialization with html_to_text option."""
        config = WebReaderConfig(html_to_text=True)
        
        assert config.html_to_text is True


class TestRSSReaderConfig:
    """Tests for RSSReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify RSSReaderConfig initializes with defaults."""
        config = RSSReaderConfig()
        
        assert config.html_to_text is True


class TestDatabaseReaderConfig:
    """Tests for DatabaseReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify DatabaseReaderConfig initializes with defaults."""
        config = DatabaseReaderConfig()
        
        assert config.connection_string == ""
        assert config.query == ""
        assert config.metadata_fn is None
    
    def test_initialization_with_connection_details(self):
        """Verify initialization with connection details."""
        config = DatabaseReaderConfig(
            connection_string="postgresql://localhost/db",
            query="SELECT * FROM table"
        )
        
        assert config.connection_string == "postgresql://localhost/db"
        assert config.query == "SELECT * FROM table"


class TestMongoReaderConfig:
    """Tests for MongoReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify MongoReaderConfig initializes with defaults."""
        config = MongoReaderConfig()
        
        assert config.host == "localhost"
        assert config.port == 27017
        assert config.db_name == ""
        assert config.collection_name == ""
    
    def test_initialization_with_custom_values(self):
        """Verify initialization with custom values."""
        config = MongoReaderConfig(
            host="mongo.example.com",
            port=27018,
            db_name="test_db",
            collection_name="test_collection"
        )
        
        assert config.host == "mongo.example.com"
        assert config.port == 27018
        assert config.db_name == "test_db"
        assert config.collection_name == "test_collection"


class TestS3DirectoryReaderConfig:
    """Tests for S3DirectoryReaderConfig."""
    
    def test_initialization_with_key(self):
        """Verify initialization with single file key."""
        config = S3DirectoryReaderConfig(
            bucket="test-bucket",
            key="path/to/file.txt"
        )
        
        assert config.bucket == "test-bucket"
        assert config.key == "path/to/file.txt"
        assert config.prefix is None
    
    def test_initialization_with_prefix(self):
        """Verify initialization with directory prefix."""
        config = S3DirectoryReaderConfig(
            bucket="test-bucket",
            prefix="path/to/directory/"
        )
        
        assert config.bucket == "test-bucket"
        assert config.prefix == "path/to/directory/"
        assert config.key is None
    
    def test_initialization_requires_key_or_prefix(self):
        """Verify initialization requires either key or prefix."""
        with pytest.raises(ValueError, match="You must specify either `key` or `prefix`"):
            S3DirectoryReaderConfig(bucket="test-bucket")
    
    def test_initialization_rejects_both_key_and_prefix(self):
        """Verify initialization rejects both key and prefix."""
        with pytest.raises(ValueError, match="Only one of `key` or `prefix` may be set"):
            S3DirectoryReaderConfig(
                bucket="test-bucket",
                key="file.txt",
                prefix="directory/"
            )


class TestStructuredDataReaderConfig:
    """Tests for StructuredDataReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify StructuredDataReaderConfig initializes with defaults."""
        config = StructuredDataReaderConfig()
        
        assert config.col_index == 0
        assert config.col_joiner == ', '
        assert config.col_metadata is None
        assert config.pandas_config is None
        assert config.metadata_fn is None
        assert config.stream_s3 is False
        assert config.stream_threshold_mb == 100
    
    def test_initialization_with_streaming_options(self):
        """Verify initialization with S3 streaming options."""
        config = StructuredDataReaderConfig(
            stream_s3=True,
            stream_threshold_mb=50
        )
        
        assert config.stream_s3 is True
        assert config.stream_threshold_mb == 50


class TestDirectoryReaderConfig:
    """Tests for DirectoryReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify DirectoryReaderConfig initializes with defaults."""
        config = DirectoryReaderConfig()
        
        assert config.input_dir == ""
        assert config.exclude_hidden is True
        assert config.recursive is True
        assert config.required_exts is None
        assert config.metadata_fn is None
    
    def test_initialization_with_custom_values(self):
        """Verify initialization with custom values."""
        config = DirectoryReaderConfig(
            input_dir="/path/to/docs",
            exclude_hidden=False,
            recursive=False,
            required_exts=[".txt", ".md"]
        )
        
        assert config.input_dir == "/path/to/docs"
        assert config.exclude_hidden is False
        assert config.recursive is False
        assert config.required_exts == [".txt", ".md"]


class TestUniversalDirectoryReaderConfig:
    """Tests for UniversalDirectoryReaderConfig."""
    
    def test_initialization_with_local_directory(self):
        """Verify initialization for local directory reading."""
        config = UniversalDirectoryReaderConfig(
            input_dir="/local/path",
            recursive=True,
            required_exts=[".txt"]
        )
        
        assert config.input_dir == "/local/path"
        assert config.recursive is True
        assert config.required_exts == [".txt"]
    
    def test_initialization_with_s3_parameters(self):
        """Verify initialization with S3 parameters."""
        config = UniversalDirectoryReaderConfig(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="docs/",
            collection_id="collection-123"
        )
        
        assert config.region == "us-east-1"
        assert config.bucket_name == "test-bucket"
        assert config.key_prefix == "docs/"
        assert config.collection_id == "collection-123"
    
    def test_initialization_with_file_list(self):
        """Verify initialization with specific file list."""
        files = ["file1.txt", "file2.md", "file3.pdf"]
        config = UniversalDirectoryReaderConfig(input_files=files)
        
        assert config.input_files == files


class TestCloudStorageReaderConfigs:
    """Tests for cloud storage reader configurations."""
    
    def test_gcs_reader_config(self):
        """Verify GCSReaderConfig initialization."""
        config = GCSReaderConfig(
            bucket="gcs-bucket",
            key="path/to/file"
        )
        
        assert config.bucket == "gcs-bucket"
        assert config.key == "path/to/file"


class TestCommunicationReaderConfigs:
    """Tests for communication platform reader configurations."""
    
    def test_slack_reader_config(self):
        """Verify SlackReaderConfig initialization."""
        config = SlackReaderConfig(
            slack_token="xoxb-token",
            channel_ids=["C123", "C456"]
        )
        
        assert config.slack_token == "xoxb-token"
        assert config.channel_ids == ["C123", "C456"]
    
    def test_discord_reader_config(self):
        """Verify DiscordReaderConfig initialization."""
        config = DiscordReaderConfig(
            discord_token="discord-token",
            channel_ids=["123456", "789012"]
        )
        
        assert config.discord_token == "discord-token"
        assert config.channel_ids == ["123456", "789012"]
    
    def test_twitter_reader_config(self):
        """Verify TwitterReaderConfig initialization."""
        config = TwitterReaderConfig(
            bearer_token="twitter-bearer-token",
            num_tweets=50
        )
        
        assert config.bearer_token == "twitter-bearer-token"
        assert config.num_tweets == 50


class TestKnowledgeBaseReaderConfigs:
    """Tests for knowledge base reader configurations."""
    
    def test_wikipedia_reader_config(self):
        """Verify WikipediaReaderConfig initialization."""
        config = WikipediaReaderConfig(lang="fr")
        
        assert config.lang == "fr"
    
    def test_youtube_reader_config(self):
        """Verify YouTubeReaderConfig initialization."""
        config = YouTubeReaderConfig(language="es")
        
        assert config.language == "es"


class TestEmailReaderConfigs:
    """Tests for email reader configurations."""
    
    def test_gmail_reader_config(self):
        """Verify GmailReaderConfig initialization."""
        config = GmailReaderConfig(
            credentials_path="/path/to/creds.json",
            token_path="/path/to/token.json",
            use_iterative_parser=True
        )
        
        assert config.credentials_path == "/path/to/creds.json"
        assert config.token_path == "/path/to/token.json"
        assert config.use_iterative_parser is True
    
    def test_outlook_reader_config(self):
        """Verify OutlookReaderConfig initialization."""
        config = OutlookReaderConfig(
            client_id="client-id",
            client_secret="client-secret",
            tenant_id="tenant-id"
        )
        
        assert config.client_id == "client-id"
        assert config.client_secret == "client-secret"
        assert config.tenant_id == "tenant-id"


class TestDocumentGraphReaderConfig:
    """Tests for DocumentGraphReaderConfig."""
    
    def test_initialization_with_defaults(self):
        """Verify DocumentGraphReaderConfig initializes with defaults."""
        config = DocumentGraphReaderConfig()
        
        assert config.metadata_fn is None
    
    def test_initialization_with_metadata_fn(self):
        """Verify initialization with metadata function."""
        def metadata_fn(data):
            return {"processed": True}
        
        config = DocumentGraphReaderConfig(metadata_fn=metadata_fn)
        
        assert config.metadata_fn == metadata_fn
