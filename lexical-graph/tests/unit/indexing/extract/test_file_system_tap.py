# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import os
import json
import tempfile
import shutil
from pathlib import Path
from llama_index.core.schema import Document, TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.file_system_tap import FileSystemTap
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


class TestFileSystemTapInitialization:
    """Tests for FileSystemTap initialization."""
    
    def test_initialization_creates_directories(self):
        """Verify FileSystemTap creates necessary directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            assert os.path.exists(tap.raw_sources_dir)
            assert os.path.exists(tap.chunks_dir)
            assert os.path.exists(tap.sources_dir)
    
    def test_initialization_with_clean_true(self):
        """Verify clean=True removes existing directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create initial tap
            tap1 = FileSystemTap(
                subdirectory_name="test_run",
                clean=False,
                output_dir=temp_dir
            )
            
            # Create a test file
            test_file = os.path.join(tap1.raw_sources_dir, "test.txt")
            with open(test_file, 'w') as f:
                f.write("test content")
            
            assert os.path.exists(test_file)
            
            # Create new tap with clean=True
            tap2 = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            # Test file should be gone
            assert not os.path.exists(test_file)
            # But directories should exist
            assert os.path.exists(tap2.raw_sources_dir)
    
    def test_initialization_with_clean_false(self):
        """Verify clean=False preserves existing directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create initial tap
            tap1 = FileSystemTap(
                subdirectory_name="test_run",
                clean=False,
                output_dir=temp_dir
            )
            
            # Create a test file
            test_file = os.path.join(tap1.raw_sources_dir, "test.txt")
            with open(test_file, 'w') as f:
                f.write("test content")
            
            # Create new tap with clean=False
            tap2 = FileSystemTap(
                subdirectory_name="test_run",
                clean=False,
                output_dir=temp_dir
            )
            
            # Test file should still exist
            assert os.path.exists(test_file)
    
    def test_initialization_directory_structure(self):
        """Verify correct directory structure is created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="my_test",
                clean=True,
                output_dir=temp_dir
            )
            
            expected_raw = os.path.join(temp_dir, "extracted", "my_test", "raw")
            expected_chunks = os.path.join(temp_dir, "extracted", "my_test", "chunks")
            expected_sources = os.path.join(temp_dir, "extracted", "my_test", "sources")
            
            assert tap.raw_sources_dir == expected_raw
            assert tap.chunks_dir == expected_chunks
            assert tap.sources_dir == expected_sources


class TestFileSystemTapHandleInputDocs:
    """Tests for handle_input_docs method."""
    
    def test_handle_input_docs_saves_raw_content(self):
        """Verify handle_input_docs saves raw document content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            doc = SourceDocument(
                refNode=Document(
                    text="Test document content",
                    doc_id="doc123"
                )
            )
            
            result = list(tap.handle_input_docs([doc]))
            
            # Check raw file was created
            raw_file = os.path.join(tap.raw_sources_dir, "doc123")
            assert os.path.exists(raw_file)
            
            with open(raw_file, 'r') as f:
                content = f.read()
            assert content == "Test document content"
    
    def test_handle_input_docs_saves_json_representation(self):
        """Verify handle_input_docs saves JSON representation of document."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            doc = SourceDocument(
                refNode=Document(
                    text="Test content",
                    doc_id="doc456",
                    metadata={"source": "test"}
                )
            )
            
            result = list(tap.handle_input_docs([doc]))
            
            # Check JSON file was created
            json_file = os.path.join(tap.sources_dir, "doc456.json")
            assert os.path.exists(json_file)
            
            with open(json_file, 'r') as f:
                data = json.load(f)
            assert 'text' in data
            assert data['text'] == "Test content"
    
    def test_handle_input_docs_returns_original_docs(self):
        """Verify handle_input_docs returns original documents unchanged."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            docs = [
                SourceDocument(refNode=Document(text="doc1", doc_id="id1")),
                SourceDocument(refNode=Document(text="doc2", doc_id="id2"))
            ]
            
            result = list(tap.handle_input_docs(docs))
            
            assert len(result) == 2
            assert result[0].refNode.text == "doc1"
            assert result[1].refNode.text == "doc2"
    
    def test_handle_input_docs_with_no_refnode(self):
        """Verify handle_input_docs handles documents without refNode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            doc = SourceDocument(refNode=None)
            
            result = list(tap.handle_input_docs([doc]))
            
            # Should not crash, just skip saving
            assert len(result) == 1
    
    def test_handle_input_docs_multiple_documents(self):
        """Verify handle_input_docs handles multiple documents."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            docs = [
                SourceDocument(refNode=Document(text=f"doc{i}", doc_id=f"id{i}"))
                for i in range(5)
            ]
            
            result = list(tap.handle_input_docs(docs))
            
            assert len(result) == 5
            
            # Check all files were created
            for i in range(5):
                raw_file = os.path.join(tap.raw_sources_dir, f"id{i}")
                json_file = os.path.join(tap.sources_dir, f"id{i}.json")
                assert os.path.exists(raw_file)
                assert os.path.exists(json_file)


class TestFileSystemTapHandleOutputDoc:
    """Tests for handle_output_doc method."""
    
    def test_handle_output_doc_saves_nodes(self):
        """Verify handle_output_doc saves node data as JSON."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            node = TextNode(text="chunk content", id_="node123")
            doc = SourceDocument(
                refNode=Document(text="test"),
                nodes=[node]
            )
            
            result = tap.handle_output_doc(doc)
            
            # Check node file was created
            node_file = os.path.join(tap.chunks_dir, "node123.json")
            assert os.path.exists(node_file)
            
            with open(node_file, 'r') as f:
                data = json.load(f)
            assert 'text' in data
            assert data['text'] == "chunk content"
    
    def test_handle_output_doc_returns_original_doc(self):
        """Verify handle_output_doc returns original document unchanged."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            node = TextNode(text="chunk", id_="node1")
            doc = SourceDocument(
                refNode=Document(text="test"),
                nodes=[node]
            )
            
            result = tap.handle_output_doc(doc)
            
            assert result is doc
            assert result.nodes[0].text == "chunk"
    
    def test_handle_output_doc_multiple_nodes(self):
        """Verify handle_output_doc handles multiple nodes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            nodes = [
                TextNode(text=f"chunk{i}", id_=f"node{i}")
                for i in range(3)
            ]
            doc = SourceDocument(
                refNode=Document(text="test"),
                nodes=nodes
            )
            
            result = tap.handle_output_doc(doc)
            
            # Check all node files were created
            for i in range(3):
                node_file = os.path.join(tap.chunks_dir, f"node{i}.json")
                assert os.path.exists(node_file)
    
    def test_handle_output_doc_with_no_nodes(self):
        """Verify handle_output_doc handles document with no nodes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="test_run",
                clean=True,
                output_dir=temp_dir
            )
            
            doc = SourceDocument(
                refNode=Document(text="test"),
                nodes=[]
            )
            
            result = tap.handle_output_doc(doc)
            
            # Should not crash
            assert result is doc
            assert len(result.nodes) == 0


class TestFileSystemTapIntegration:
    """Integration tests for FileSystemTap."""
    
    def test_full_pipeline_flow(self):
        """Verify complete input and output handling flow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            tap = FileSystemTap(
                subdirectory_name="integration_test",
                clean=True,
                output_dir=temp_dir
            )
            
            # Handle input
            input_doc = SourceDocument(
                refNode=Document(text="Original document", doc_id="doc1")
            )
            processed_docs = list(tap.handle_input_docs([input_doc]))
            
            # Add nodes to document
            processed_docs[0].nodes = [
                TextNode(text="chunk1", id_="chunk1"),
                TextNode(text="chunk2", id_="chunk2")
            ]
            
            # Handle output
            output_doc = tap.handle_output_doc(processed_docs[0])
            
            # Verify all files exist
            assert os.path.exists(os.path.join(tap.raw_sources_dir, "doc1"))
            assert os.path.exists(os.path.join(tap.sources_dir, "doc1.json"))
            assert os.path.exists(os.path.join(tap.chunks_dir, "chunk1.json"))
            assert os.path.exists(os.path.join(tap.chunks_dir, "chunk2.json"))
