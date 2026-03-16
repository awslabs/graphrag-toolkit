"""
Unit tests for StreamingJSONLReaderProvider.

Tests configuration defaults, edge cases, and input validation.
"""

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock

import pytest

# Mock the problematic imports before importing the modules
sys.modules['fitz'] = MagicMock()

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
    StreamingJSONLReaderConfig,
)
from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.streaming_jsonl_reader_provider import (
    StreamingJSONLReaderProvider,
)


class TestStreamingJSONLReaderConfigDefaults:
    """Test configuration default values."""

    def test_default_batch_size(self):
        """Verify default batch_size is 100."""
        config = StreamingJSONLReaderConfig()
        assert config.batch_size == 100

    def test_default_text_field(self):
        """Verify default text_field is 'text'."""
        config = StreamingJSONLReaderConfig()
        assert config.text_field == "text"

    def test_default_strict_mode(self):
        """Verify default strict_mode is False."""
        config = StreamingJSONLReaderConfig()
        assert config.strict_mode is False

    def test_default_log_interval(self):
        """Verify default log_interval is 10000."""
        config = StreamingJSONLReaderConfig()
        assert config.log_interval == 10000

    def test_default_metadata_fn(self):
        """Verify default metadata_fn is None."""
        config = StreamingJSONLReaderConfig()
        assert config.metadata_fn is None

    def test_custom_config_values(self):
        """Verify custom configuration values are applied."""
        def custom_metadata(path):
            return {"custom": True}

        config = StreamingJSONLReaderConfig(
            batch_size=50,
            text_field="content",
            strict_mode=True,
            log_interval=5000,
            metadata_fn=custom_metadata
        )

        assert config.batch_size == 50
        assert config.text_field == "content"
        assert config.strict_mode is True
        assert config.log_interval == 5000
        assert config.metadata_fn is custom_metadata


class TestEmptyFile:
    """Test handling of empty files."""

    def test_empty_file_returns_empty_list(self):
        """Verify empty file returns empty document list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert documents == []
        finally:
            os.unlink(jsonl_file)

    def test_empty_file_lazy_load_yields_nothing(self):
        """Verify empty file yields no batches from lazy_load_data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            batches = list(reader.lazy_load_data(jsonl_file))
            assert batches == []
        finally:
            os.unlink(jsonl_file)

    def test_whitespace_only_file_returns_empty_list(self):
        """Verify file with only whitespace returns empty document list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write("   \n\n   \n")
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert documents == []
        finally:
            os.unlink(jsonl_file)


class TestSingleLine:
    """Test handling of single-line files."""

    def test_single_line_file(self):
        """Verify single line file returns one document."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps({"text": "single line content"}))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert len(documents) == 1
            assert documents[0].text == "single line content"
        finally:
            os.unlink(jsonl_file)

    def test_single_line_lazy_load_yields_one_batch(self):
        """Verify single line file yields exactly one batch."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps({"text": "single line"}))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(batch_size=100)
            reader = StreamingJSONLReaderProvider(config)

            batches = list(reader.lazy_load_data(jsonl_file))
            assert len(batches) == 1
            assert len(batches[0]) == 1
        finally:
            os.unlink(jsonl_file)


class TestBatchSizeOne:
    """Test batch_size=1 behavior."""

    def test_batch_size_one_yields_individual_documents(self):
        """Verify batch_size=1 yields one document per batch."""
        num_lines = 5
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
            f.write("\n".join(lines))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(batch_size=1)
            reader = StreamingJSONLReaderProvider(config)

            batches = list(reader.lazy_load_data(jsonl_file))

            # Should have exactly num_lines batches
            assert len(batches) == num_lines

            # Each batch should have exactly 1 document
            for batch in batches:
                assert len(batch) == 1
        finally:
            os.unlink(jsonl_file)

    def test_batch_size_one_read_returns_all(self):
        """Verify batch_size=1 still returns all documents from read()."""
        num_lines = 10
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
            f.write("\n".join(lines))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(batch_size=1)
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert len(documents) == num_lines
        finally:
            os.unlink(jsonl_file)


class TestInputValidation:
    """Test input validation and error handling."""

    def test_none_input_raises_error(self):
        """Verify None input raises appropriate error."""
        config = StreamingJSONLReaderConfig()
        reader = StreamingJSONLReaderProvider(config)

        with pytest.raises((ValueError, TypeError)):
            reader.read(None)

    def test_empty_string_input_raises_error(self):
        """Verify empty string input raises appropriate error."""
        config = StreamingJSONLReaderConfig()
        reader = StreamingJSONLReaderProvider(config)

        with pytest.raises((ValueError, FileNotFoundError, OSError)):
            reader.read("")

    def test_nonexistent_file_raises_error(self):
        """Verify non-existent file raises FileNotFoundError."""
        config = StreamingJSONLReaderConfig()
        reader = StreamingJSONLReaderProvider(config)

        with pytest.raises(FileNotFoundError):
            reader.read("/nonexistent/path/to/file.jsonl")

    def test_lazy_load_none_input_raises_error(self):
        """Verify None input to lazy_load_data raises appropriate error."""
        config = StreamingJSONLReaderConfig()
        reader = StreamingJSONLReaderProvider(config)

        with pytest.raises((ValueError, TypeError)):
            list(reader.lazy_load_data(None))

    def test_lazy_load_nonexistent_file_raises_error(self):
        """Verify non-existent file raises FileNotFoundError in lazy_load_data."""
        config = StreamingJSONLReaderConfig()
        reader = StreamingJSONLReaderProvider(config)

        with pytest.raises(FileNotFoundError):
            list(reader.lazy_load_data("/nonexistent/path/to/file.jsonl"))


class TestTextFieldBehavior:
    """Test text_field configuration behavior."""

    def test_custom_text_field(self):
        """Verify custom text_field extracts correct value."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps({"content": "my content", "text": "wrong field"}))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(text_field="content")
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert len(documents) == 1
            assert documents[0].text == "my content"
        finally:
            os.unlink(jsonl_file)

    def test_none_text_field_uses_full_json(self):
        """Verify text_field=None uses entire JSON as text."""
        obj = {"id": 1, "name": "test"}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps(obj))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(text_field=None)
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert len(documents) == 1
            assert documents[0].text == json.dumps(obj)
        finally:
            os.unlink(jsonl_file)


class TestStrictModeBehavior:
    """Test strict_mode configuration behavior."""

    def test_strict_mode_false_skips_invalid(self):
        """Verify strict_mode=False skips invalid lines."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps({"text": "valid"}) + "\n")
            f.write("{invalid json\n")
            f.write(json.dumps({"text": "also valid"}))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(strict_mode=False)
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert len(documents) == 2
        finally:
            os.unlink(jsonl_file)

    def test_strict_mode_true_raises_on_invalid(self):
        """Verify strict_mode=True raises on invalid JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps({"text": "valid"}) + "\n")
            f.write("{invalid json")
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig(strict_mode=True)
            reader = StreamingJSONLReaderProvider(config)

            with pytest.raises(json.JSONDecodeError):
                reader.read(jsonl_file)
        finally:
            os.unlink(jsonl_file)


class TestMetadataFields:
    """Test metadata field population."""

    def test_metadata_contains_required_fields(self):
        """Verify documents contain required metadata fields."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write(json.dumps({"text": "test content"}))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)
            assert len(documents) == 1

            metadata = documents[0].metadata
            assert "source" in metadata
            assert "line_number" in metadata
            assert "file_path" in metadata
            assert "document_type" in metadata

            assert metadata["source"] == "local_file"
            assert metadata["line_number"] == 1
            assert metadata["file_path"] == jsonl_file
            assert metadata["document_type"] == "jsonl"
        finally:
            os.unlink(jsonl_file)

    def test_line_numbers_are_sequential(self):
        """Verify line numbers are 1-based and sequential."""
        num_lines = 5
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            lines = [json.dumps({"text": f"line {i}"}) for i in range(num_lines)]
            f.write("\n".join(lines))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            documents = reader.read(jsonl_file)

            for i, doc in enumerate(documents):
                expected_line = i + 1
                assert doc.metadata["line_number"] == expected_line
        finally:
            os.unlink(jsonl_file)


class TestLoadDataInterface:
    """Test load_data interface (LlamaIndex compatibility)."""

    def test_load_data_returns_same_as_read(self):
        """Verify load_data returns same results as read."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            lines = [json.dumps({"text": f"line {i}"}) for i in range(10)]
            f.write("\n".join(lines))
            jsonl_file = f.name

        try:
            config = StreamingJSONLReaderConfig()
            reader = StreamingJSONLReaderProvider(config)

            read_docs = reader.read(jsonl_file)
            load_docs = reader.load_data(jsonl_file)

            assert len(read_docs) == len(load_docs)
            for read_doc, load_doc in zip(read_docs, load_docs):
                assert read_doc.text == load_doc.text
        finally:
            os.unlink(jsonl_file)
