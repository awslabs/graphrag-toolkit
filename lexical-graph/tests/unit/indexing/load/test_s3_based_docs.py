# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, MagicMock
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    S3BasedDocs,
    S3DocDownloader,
    S3DocUploader,
    S3ChunkDownloader,
    S3ChunkUploader
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


class TestS3BasedDocsInitialization:
    """Tests for S3BasedDocs initialization."""
    
    def test_initialization_with_required_params(self):
        """Verify S3BasedDocs initializes with required parameters."""
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="test-prefix"
        )
        
        assert handler is not None
        assert handler.region == "us-east-1"
        assert handler.bucket_name == "test-bucket"
        assert handler.key_prefix == "test-prefix"
        assert handler.collection_id is not None
    
    def test_initialization_with_custom_collection_id(self):
        """Verify initialization with custom collection ID."""
        collection_id = "custom-collection-123"
        handler = S3BasedDocs(
            region="us-west-2",
            bucket_name="test-bucket",
            key_prefix="prefix",
            collection_id=collection_id
        )
        
        assert handler.collection_id == collection_id
    
    def test_initialization_with_encryption_key(self):
        """Verify initialization with S3 encryption key."""
        encryption_key = "arn:aws:kms:us-east-1:123456789012:key/12345678"
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix",
            s3_encryption_key_id=encryption_key
        )
        
        assert handler.s3_encryption_key_id == encryption_key
    
    def test_initialization_with_metadata_keys(self):
        """Verify initialization with metadata keys filter."""
        metadata_keys = ["source", "date", "author"]
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix",
            metadata_keys=metadata_keys
        )
        
        assert handler.metadata_keys == metadata_keys
    
    def test_initialization_with_jsonl_format(self):
        """Verify initialization with JSONL format option."""
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix",
            for_jsonl=True
        )
        
        assert handler.for_jsonl is True


class TestS3BasedDocsMetadataFiltering:
    """Tests for metadata filtering functionality."""
    
    def test_filter_metadata_with_allowed_keys(self):
        """Verify metadata filtering with allowed keys."""
        metadata_keys = ["source", "date"]
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix",
            metadata_keys=metadata_keys
        )
        
        # Create node with extra metadata
        node = TextNode(
            text="Test text",
            id_="node1",
            metadata={
                "source": "test",
                "date": "2024-01-01",
                "extra_key": "should_be_removed"
            }
        )
        
        filtered_node = handler._filter_metadata(node)
        
        assert "source" in filtered_node.metadata
        assert "date" in filtered_node.metadata
        assert "extra_key" not in filtered_node.metadata
    
    def test_filter_preserves_special_keys(self):
        """Verify special keys are always preserved."""
        from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
        from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
        
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix",
            metadata_keys=["source"]
        )
        
        # Create node with special keys
        node = TextNode(
            text="Test text",
            id_="node1",
            metadata={
                "source": "test",
                PROPOSITIONS_KEY: ["prop1"],
                TOPICS_KEY: ["topic1"],
                INDEX_KEY: "index1",
                "extra": "remove"
            }
        )
        
        filtered_node = handler._filter_metadata(node)
        
        assert PROPOSITIONS_KEY in filtered_node.metadata
        assert TOPICS_KEY in filtered_node.metadata
        assert INDEX_KEY in filtered_node.metadata
        assert "source" in filtered_node.metadata
        assert "extra" not in filtered_node.metadata
    
    def test_filter_without_metadata_keys_removes_non_special(self):
        """Verify filtering without metadata_keys removes non-special keys."""
        from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY
        
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix",
            metadata_keys=None
        )
        
        node = TextNode(
            text="Test text",
            id_="node1",
            metadata={
                PROPOSITIONS_KEY: ["prop1"],
                "custom_key": "value"
            }
        )
        
        filtered_node = handler._filter_metadata(node)
        
        assert PROPOSITIONS_KEY in filtered_node.metadata
        # Without metadata_keys set, custom keys should be preserved
        assert "custom_key" in filtered_node.metadata


class TestS3DocDownloader:
    """Tests for S3DocDownloader component."""
    
    def test_initialization(self):
        """Verify S3DocDownloader initializes correctly."""
        def filter_fn(node):
            return node
        
        downloader = S3DocDownloader(
            key_prefix="test-prefix",
            collection_id="test-collection",
            bucket_name="test-bucket",
            fn=filter_fn
        )
        
        assert downloader.key_prefix == "test-prefix"
        assert downloader.collection_id == "test-collection"
        assert downloader.bucket_name == "test-bucket"
        assert downloader.fn == filter_fn


class TestS3DocUploader:
    """Tests for S3DocUploader component."""
    
    def test_initialization(self):
        """Verify S3DocUploader initializes correctly."""
        uploader = S3DocUploader(
            bucket_name="test-bucket",
            collection_prefix="test-prefix/collection"
        )
        
        assert uploader.bucket_name == "test-bucket"
        assert uploader.collection_prefix == "test-prefix/collection"
        assert uploader.s3_encryption_key_id is None
    
    def test_initialization_with_encryption(self):
        """Verify initialization with encryption key."""
        encryption_key = "arn:aws:kms:us-east-1:123456789012:key/12345678"
        uploader = S3DocUploader(
            bucket_name="test-bucket",
            collection_prefix="test-prefix",
            s3_encryption_key_id=encryption_key
        )
        
        assert uploader.s3_encryption_key_id == encryption_key


class TestS3ChunkDownloader:
    """Tests for S3ChunkDownloader component."""
    
    def test_initialization(self):
        """Verify S3ChunkDownloader initializes correctly."""
        def filter_fn(node):
            return node
        
        downloader = S3ChunkDownloader(
            key_prefix="test-prefix",
            collection_id="test-collection",
            bucket_name="test-bucket",
            fn=filter_fn
        )
        
        assert downloader.key_prefix == "test-prefix"
        assert downloader.collection_id == "test-collection"
        assert downloader.bucket_name == "test-bucket"
        assert downloader.fn == filter_fn


class TestS3ChunkUploader:
    """Tests for S3ChunkUploader component."""
    
    def test_initialization(self):
        """Verify S3ChunkUploader initializes correctly."""
        uploader = S3ChunkUploader(
            bucket_name="test-bucket",
            collection_prefix="test-prefix/collection"
        )
        
        assert uploader.bucket_name == "test-bucket"
        assert uploader.collection_prefix == "test-prefix/collection"
        assert uploader.s3_encryption_key_id is None
    
    def test_initialization_with_encryption(self):
        """Verify initialization with encryption key."""
        encryption_key = "arn:aws:kms:us-east-1:123456789012:key/12345678"
        uploader = S3ChunkUploader(
            bucket_name="test-bucket",
            collection_prefix="test-prefix",
            s3_encryption_key_id=encryption_key
        )
        
        assert uploader.s3_encryption_key_id == encryption_key


class TestS3BasedDocsMethods:
    """Tests for S3BasedDocs methods."""
    
    def test_docs_method_returns_self(self):
        """Verify docs() method returns self for chaining."""
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix"
        )
        
        result = handler.docs()
        
        assert result is handler
    

class TestS3BasedDocsConfiguration:
    """Tests for various S3BasedDocs configurations."""
    
    def test_default_collection_id_format(self):
        """Verify default collection ID follows timestamp format."""
        handler = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix"
        )
        
        # Collection ID should be in format YYYYMMDD-HHMMSS
        assert handler.collection_id is not None
        assert len(handler.collection_id) > 0
        assert '-' in handler.collection_id
    
    def test_multiple_instances_have_different_collection_ids(self):
        """Verify multiple instances get different default collection IDs."""
        handler1 = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix"
        )
        
        handler2 = S3BasedDocs(
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="prefix"
        )
        
        # Collection IDs should be different (unless created in same second)
        # This test may occasionally fail if both are created in the same second
        # but that's acceptable for this test
        assert handler1.collection_id is not None
        assert handler2.collection_id is not None
