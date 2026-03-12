# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.load.file_based_docs import FileBasedDocs
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


class TestFileBasedDocsInitialization:
    """Tests for FileBasedDocs initialization."""
    
    def test_initialization_with_directory(self):
        """Verify FileBasedDocs initializes with docs directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = FileBasedDocs(docs_directory=temp_dir)
            
            assert handler is not None
            assert handler.docs_directory == temp_dir
            assert handler.collection_id is not None
    
    def test_initialization_with_custom_collection_id(self):
        """Verify initialization with custom collection ID."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "test-collection-001"
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            assert handler.collection_id == collection_id
    
    def test_initialization_with_metadata_keys(self):
        """Verify initialization with metadata keys filter."""
        with tempfile.TemporaryDirectory() as temp_dir:
            metadata_keys = ["source", "date", "author"]
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                metadata_keys=metadata_keys
            )
            
            assert handler.metadata_keys == metadata_keys
    
    def test_initialization_creates_collection_directory(self):
        """Verify initialization creates collection directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "test-collection"
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            collection_path = os.path.join(temp_dir, collection_id)
            assert os.path.exists(collection_path)
            assert os.path.isdir(collection_path)


class TestFileBasedDocsReading:
    """Tests for reading documents from file system."""
    
    def test_read_documents_from_directory(self):
        """Verify reading documents from directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "test-collection"
            
            # Create directory structure with nodes
            collection_path = os.path.join(temp_dir, collection_id)
            os.makedirs(collection_path)
            
            source_doc_path = os.path.join(collection_path, "doc1")
            os.makedirs(source_doc_path)
            
            # Create sample nodes
            node1 = TextNode(text="Sample text 1", id_="node1")
            node2 = TextNode(text="Sample text 2", id_="node2")
            
            with open(os.path.join(source_doc_path, "node1.json"), 'w') as f:
                json.dump(node1.to_dict(), f)
            
            with open(os.path.join(source_doc_path, "node2.json"), 'w') as f:
                json.dump(node2.to_dict(), f)
            
            # Read documents
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            documents = list(handler)
            
            assert len(documents) == 1
            assert isinstance(documents[0], SourceDocument)
            assert len(documents[0].nodes) == 2
    
    def test_read_empty_collection(self):
        """Verify handling of empty collection directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "empty-collection"
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            documents = list(handler)
            
            assert len(documents) == 0
    
    def test_read_multiple_source_documents(self):
        """Verify reading multiple source documents."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "multi-doc-collection"
            collection_path = os.path.join(temp_dir, collection_id)
            os.makedirs(collection_path)
            
            # Create multiple source document directories
            for i in range(3):
                source_doc_path = os.path.join(collection_path, f"doc{i}")
                os.makedirs(source_doc_path)
                
                node = TextNode(text=f"Text for doc {i}", id_=f"node{i}")
                with open(os.path.join(source_doc_path, f"node{i}.json"), 'w') as f:
                    json.dump(node.to_dict(), f)
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            documents = list(handler)
            
            assert len(documents) == 3


class TestFileBasedDocsWriting:
    """Tests for writing documents to file system."""
    
    def test_write_source_documents(self):
        """Verify writing source documents to directory."""
        from llama_index.core.schema import NodeRelationship, RelatedNodeInfo
        
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "write-test"
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            # Create source documents with SOURCE relationship
            source_node_id = "source-doc-1"
            nodes = [
                TextNode(
                    text="Node 1",
                    id_="node1",
                    relationships={
                        NodeRelationship.SOURCE: RelatedNodeInfo(node_id=source_node_id)
                    }
                ),
                TextNode(
                    text="Node 2",
                    id_="node2",
                    relationships={
                        NodeRelationship.SOURCE: RelatedNodeInfo(node_id=source_node_id)
                    }
                )
            ]
            source_doc = SourceDocument(nodes=nodes)
            
            # Write documents
            result = list(handler.accept([source_doc]))
            
            assert len(result) == 1
            
            # Verify files were created
            source_doc_path = os.path.join(
                temp_dir,
                collection_id,
                source_doc.source_id()
            )
            assert os.path.exists(source_doc_path)
            assert os.path.exists(os.path.join(source_doc_path, "node1.json"))
            assert os.path.exists(os.path.join(source_doc_path, "node2.json"))
    
    def test_write_preserves_metadata(self):
        """Verify metadata is preserved when writing."""
        from llama_index.core.schema import NodeRelationship, RelatedNodeInfo
        
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "metadata-test"
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            # Create node with metadata and SOURCE relationship
            source_node_id = "source-doc-1"
            node = TextNode(
                text="Test text",
                id_="node1",
                metadata={"source": "test", "date": "2024-01-01"},
                relationships={
                    NodeRelationship.SOURCE: RelatedNodeInfo(node_id=source_node_id)
                }
            )
            source_doc = SourceDocument(nodes=[node])
            
            # Write and read back
            list(handler.accept([source_doc]))
            
            # Read back
            documents = list(handler)
            
            assert len(documents) == 1
            assert len(documents[0].nodes) == 1
            assert "source" in documents[0].nodes[0].metadata
            assert documents[0].nodes[0].metadata["source"] == "test"


class TestFileBasedDocsMetadataFiltering:
    """Tests for metadata filtering functionality."""
    
    def test_filter_metadata_with_allowed_keys(self):
        """Verify metadata filtering with allowed keys."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "filter-test"
            metadata_keys = ["source", "date"]
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id,
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
        with tempfile.TemporaryDirectory() as temp_dir:
            from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
            from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
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


class TestFileBasedDocsErrorHandling:
    """Tests for error handling."""
    
    def test_handle_invalid_json_file(self):
        """Verify handling of invalid JSON files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "error-test"
            collection_path = os.path.join(temp_dir, collection_id)
            os.makedirs(collection_path)
            
            source_doc_path = os.path.join(collection_path, "doc1")
            os.makedirs(source_doc_path)
            
            # Create invalid JSON file
            with open(os.path.join(source_doc_path, "invalid.json"), 'w') as f:
                f.write("invalid json content")
            
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                collection_id=collection_id
            )
            
            with pytest.raises(json.JSONDecodeError):
                list(handler)
    
    def test_docs_method_returns_self(self):
        """Verify docs() method returns self for chaining."""
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = FileBasedDocs(docs_directory=temp_dir)
            
            result = handler.docs()
            
            assert result is handler
