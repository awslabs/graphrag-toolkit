# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import json
import tempfile
import os
import zipfile
from pathlib import Path
from unittest.mock import patch
from graphrag_toolkit.core.types import Node, NodeRef
from graphrag_toolkit.core.compat import NodeRelationship
from graphrag_toolkit.lexical_graph.indexing.load.file_based_docs import FileBasedDocs, windows_safe_filename
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


def _make_node(node_id, source_id, text="test"):
    """Helper to create a TextNode with a SOURCE relationship."""
    node = Node(text=text, node_id=node_id)
    node.relationships[NodeRelationship.SOURCE] = NodeRef(node_id=source_id)
    return node


class TestWindowsSafeFilename:
    """Tests for the windows_safe_filename utility function."""

    def test_windows_safe_filename(self):
        """Verify windows_safe_filename replaces :: with __."""
        assert windows_safe_filename('aws::abc:1234') == 'aws__abc_1234'

    def test_windows_safe_filename_no_double_colon(self):
        """Verify strings without :: are unchanged."""
        assert windows_safe_filename('simple-name') == 'simple-name'

    def test_windows_safe_filename_multiple_occurrences(self):
        """Verify all :: occurrences are replaced."""
        assert windows_safe_filename('a::b::c') == 'a__b__c'


class TestFileBasedDocsReadFromZip:
    """Tests for reading documents from a ZIP archive."""

    def test_read_from_zip(self, tmp_path):
        """Create a temp zip with expected structure, verify it yields correct SourceDocuments."""
        collection_id = "test-collection"
        source_id = "aws::abc:1234"
        node_id = "aws::abc:1234:chunk1"

        node = _make_node(node_id, source_id, text="hello from zip")

        # Build zip: collection_id/source_id/node_id.json
        zip_path = str(tmp_path / "test.zip")
        with zipfile.ZipFile(zip_path, 'w') as zf:
            entry_path = f"{collection_id}/{source_id}/{node_id}.json"
            zf.writestr(entry_path, json.dumps(node.to_dict()))

        handler = FileBasedDocs(
            docs_directory=str(tmp_path / "docs"),
            collection_id=collection_id,
            zip_source=zip_path,
        )

        documents = list(handler)

        assert len(documents) == 1
        assert len(documents[0].nodes) == 1
        assert documents[0].nodes[0].text == "hello from zip"
        assert documents[0].nodes[0].node_id == node_id

    def test_read_from_zip_multiple_sources(self, tmp_path):
        """Verify multiple source directories in zip yield multiple SourceDocuments."""
        collection_id = "col"
        zip_path = str(tmp_path / "multi.zip")

        with zipfile.ZipFile(zip_path, 'w') as zf:
            for i in range(3):
                node = _make_node(f"chunk{i}", f"source{i}", text=f"text{i}")
                zf.writestr(f"{collection_id}/source{i}/chunk{i}.json", json.dumps(node.to_dict()))

        handler = FileBasedDocs(
            docs_directory=str(tmp_path / "docs"),
            collection_id=collection_id,
            zip_source=zip_path,
        )

        documents = list(handler)
        assert len(documents) == 3


class TestFileBasedDocsReadFromDirectory:
    """Tests for default directory-based reading."""

    def test_read_from_directory(self, tmp_path):
        """Verify default behavior (no zip_source) reads from directory."""
        collection_id = "dir-collection"
        source_id = "src1"
        node_id = "chunk1"

        # Pre-create the directory structure
        source_dir = tmp_path / collection_id / source_id
        source_dir.mkdir(parents=True)

        node = _make_node(node_id, source_id, text="from directory")
        (source_dir / f"{node_id}.json").write_text(json.dumps(node.to_dict()))

        handler = FileBasedDocs(
            docs_directory=str(tmp_path),
            collection_id=collection_id,
        )

        documents = list(handler)

        assert len(documents) == 1
        assert documents[0].nodes[0].text == "from directory"


class TestFileBasedDocsFilenameSanitizer:
    """Tests for the filename_sanitizer feature on write."""

    def test_filename_sanitizer_on_write(self, tmp_path):
        """Verify that when filename_sanitizer is set, accept() uses sanitized names."""
        collection_id = "sanitized"
        source_id = "aws::abc:1234"
        node_id = "aws::abc:1234:chunk1"

        node = _make_node(node_id, source_id)
        source_doc = SourceDocument(nodes=[node])

        handler = FileBasedDocs(
            docs_directory=str(tmp_path),
            collection_id=collection_id,
            filename_sanitizer=windows_safe_filename,
        )

        list(handler.accept([source_doc]))

        sanitized_source = windows_safe_filename(source_id)
        sanitized_node = windows_safe_filename(node_id)
        expected_file = tmp_path / collection_id / sanitized_source / f"{sanitized_node}.json"
        assert expected_file.exists()

    def test_no_filename_sanitizer_on_write(self, tmp_path):
        """Verify default (no sanitizer) uses original node_id as filename."""
        collection_id = "unsanitized"
        source_id = "simple-source"
        node_id = "simple-chunk"

        node = _make_node(node_id, source_id)
        source_doc = SourceDocument(nodes=[node])

        handler = FileBasedDocs(
            docs_directory=str(tmp_path),
            collection_id=collection_id,
        )

        list(handler.accept([source_doc]))

        expected_file = tmp_path / collection_id / source_id / f"{node_id}.json"
        assert expected_file.exists()


class TestFileBasedDocsZipSkipsDirectoryCreation:
    """Tests for zip_source skipping directory preparation."""

    def test_zip_source_skips_directory_creation(self, tmp_path):
        """Verify that when zip_source is provided, _prepare_directory is not called during init."""
        zip_path = str(tmp_path / "dummy.zip")
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("col/src/node.json", "{}")

        docs_dir = str(tmp_path / "docs")

        with patch.object(FileBasedDocs, '_prepare_directory') as mock_prepare:
            FileBasedDocs(
                docs_directory=docs_dir,
                collection_id="col",
                zip_source=zip_path,
            )
            mock_prepare.assert_not_called()

    def test_no_zip_source_calls_prepare_directory(self, tmp_path):
        """Verify that without zip_source, _prepare_directory IS called."""
        with patch.object(FileBasedDocs, '_prepare_directory') as mock_prepare:
            FileBasedDocs(
                docs_directory=str(tmp_path),
                collection_id="col",
            )
            mock_prepare.assert_called_once()


class TestFileBasedDocsInitialization:
    """Tests for FileBasedDocs initialization."""

    def test_initialization_with_directory(self):
        """Verify FileBasedDocs initializes with docs directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = FileBasedDocs(docs_directory=temp_dir)
            assert handler.docs_directory == temp_dir
            assert handler.collection_id is not None

    def test_initialization_with_custom_collection_id(self):
        """Verify initialization with custom collection ID."""
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = FileBasedDocs(docs_directory=temp_dir, collection_id="test-001")
            assert handler.collection_id == "test-001"

    def test_initialization_creates_collection_directory(self):
        """Verify initialization creates collection directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            collection_id = "test-collection"
            FileBasedDocs(docs_directory=temp_dir, collection_id=collection_id)
            assert os.path.isdir(os.path.join(temp_dir, collection_id))


class TestFileBasedDocsMetadataFiltering:
    """Tests for metadata filtering functionality."""

    def test_filter_metadata_with_allowed_keys(self):
        """Verify metadata filtering with allowed keys."""
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = FileBasedDocs(
                docs_directory=temp_dir,
                metadata_keys=["source", "date"],
            )
            node = Node(
                text="Test",
                node_id="n1",
                metadata={"source": "x", "date": "y", "extra": "remove"},
            )
            filtered = handler._filter_metadata(node)
            assert "source" in filtered.metadata
            assert "date" in filtered.metadata
            assert "extra" not in filtered.metadata

    def test_filter_preserves_special_keys(self):
        """Verify special keys are always preserved."""
        from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY
        from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

        with tempfile.TemporaryDirectory() as temp_dir:
            handler = FileBasedDocs(docs_directory=temp_dir, metadata_keys=["source"])
            node = Node(
                text="Test",
                node_id="n1",
                metadata={
                    "source": "x",
                    PROPOSITIONS_KEY: ["p"],
                    TOPICS_KEY: ["t"],
                    INDEX_KEY: "i",
                    "extra": "remove",
                },
            )
            filtered = handler._filter_metadata(node)
            assert PROPOSITIONS_KEY in filtered.metadata
            assert TOPICS_KEY in filtered.metadata
            assert INDEX_KEY in filtered.metadata
            assert "extra" not in filtered.metadata


class TestFileBasedDocsErrorHandling:
    """Tests for error handling."""

    def test_handle_invalid_json_file(self, tmp_path):
        """Verify handling of invalid JSON files."""
        collection_id = "err"
        source_dir = tmp_path / collection_id / "doc1"
        source_dir.mkdir(parents=True)
        (source_dir / "bad.json").write_text("not json")

        handler = FileBasedDocs(docs_directory=str(tmp_path), collection_id=collection_id)
        with pytest.raises(json.JSONDecodeError):
            list(handler)

    def test_docs_method_returns_self(self, tmp_path):
        """Verify docs() method returns self for chaining."""
        handler = FileBasedDocs(docs_directory=str(tmp_path))
        assert handler.docs() is handler
