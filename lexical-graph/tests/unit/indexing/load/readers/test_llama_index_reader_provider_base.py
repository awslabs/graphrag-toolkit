# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import sys
from unittest.mock import Mock
from llama_index.core.schema import Document
from llama_index.core.readers.base import BaseReader

# Mock the providers module to avoid loading optional dependencies
sys.modules['graphrag_toolkit.lexical_graph.indexing.load.readers.providers'] = Mock()

from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig


class TestLlamaIndexReaderProviderBaseInitialization:
    """Tests for LlamaIndexReaderProviderBase initialization."""
    
    def test_initialization_with_reader_class(self):
        """Verify initialization with reader class."""
        config = ReaderProviderConfig()
        
        # Create a mock reader class
        class MockReader(BaseReader):
            def load_data(self, *args, **kwargs):
                return [Document(text="test")]
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MockReader
        )
        
        assert provider is not None
        assert provider.config == config
        assert hasattr(provider, '_reader')
    
    def test_initialization_with_reader_kwargs(self):
        """Verify initialization passes kwargs to reader."""
        config = ReaderProviderConfig()
        
        class MockReader(BaseReader):
            def __init__(self, param1=None, param2=None):
                self.param1 = param1
                self.param2 = param2
            
            def load_data(self, *args, **kwargs):
                return []
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MockReader,
            param1="value1",
            param2="value2"
        )
        
        assert provider._reader.param1 == "value1"
        assert provider._reader.param2 == "value2"
    
    def test_initialization_stores_reader_instance(self):
        """Verify reader instance is stored."""
        config = ReaderProviderConfig()
        
        class MockReader(BaseReader):
            def load_data(self, *args, **kwargs):
                return []
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MockReader
        )
        
        assert isinstance(provider._reader, MockReader)


class TestLlamaIndexReaderProviderBaseRead:
    """Tests for read() method."""
    
    def test_read_calls_underlying_reader(self):
        """Verify read() calls the underlying reader's load_data()."""
        config = ReaderProviderConfig()
        
        class MockReader(BaseReader):
            def load_data(self, input_source):
                return [Document(text=f"Loaded from {input_source}")]
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MockReader
        )
        
        result = provider.read("test_source")
        
        assert len(result) == 1
        assert "test_source" in result[0].text
    
    def test_read_returns_documents(self):
        """Verify read() returns list of documents."""
        config = ReaderProviderConfig()
        
        class MockReader(BaseReader):
            def load_data(self, *args, **kwargs):
                return [
                    Document(text="doc1"),
                    Document(text="doc2")
                ]
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MockReader
        )
        
        result = provider.read("input")
        
        assert len(result) == 2
        assert all(isinstance(doc, Document) for doc in result)
    
    def test_read_with_empty_result(self):
        """Verify read() handles empty result."""
        config = ReaderProviderConfig()
        
        class MockReader(BaseReader):
            def load_data(self, *args, **kwargs):
                return []
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MockReader
        )
        
        result = provider.read("input")
        
        assert result == []


class TestLlamaIndexReaderProviderBaseErrorHandling:
    """Tests for error handling."""
    
    def test_read_handles_reader_exception(self):
        """Verify read() handles exceptions from underlying reader."""
        config = ReaderProviderConfig()
        
        class FailingReader(BaseReader):
            def load_data(self, *args, **kwargs):
                raise ValueError("Reader failed")
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=FailingReader
        )
        
        with pytest.raises(RuntimeError, match="Failed to read using FailingReader"):
            provider.read("input")
    
    def test_read_wraps_exception_with_context(self):
        """Verify read() wraps exceptions with context."""
        config = ReaderProviderConfig()
        
        class ErrorReader(BaseReader):
            def load_data(self, *args, **kwargs):
                raise IOError("File not found")
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=ErrorReader
        )
        
        with pytest.raises(RuntimeError) as exc_info:
            provider.read("missing_file.txt")
        
        assert "Failed to read using ErrorReader" in str(exc_info.value)
        assert "File not found" in str(exc_info.value)
    
    def test_read_preserves_original_exception(self):
        """Verify original exception is preserved in chain."""
        config = ReaderProviderConfig()
        
        class CustomError(Exception):
            pass
        
        class ErrorReader(BaseReader):
            def load_data(self, *args, **kwargs):
                raise CustomError("Custom error")
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=ErrorReader
        )
        
        with pytest.raises(RuntimeError) as exc_info:
            provider.read("input")
        
        # Check that original exception is in the chain
        assert exc_info.value.__cause__.__class__ == CustomError


class TestLlamaIndexReaderProviderBaseIntegration:
    """Integration tests for LlamaIndexReaderProviderBase."""
    
    def test_integration_with_mock_reader(self):
        """Verify integration with a mock LlamaIndex reader."""
        config = ReaderProviderConfig()
        
        class FileReader(BaseReader):
            def __init__(self, file_extension=".txt"):
                self.file_extension = file_extension
            
            def load_data(self, file_path):
                return [
                    Document(
                        text=f"Content from {file_path}",
                        metadata={
                            "file_path": file_path,
                            "extension": self.file_extension
                        }
                    )
                ]
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=FileReader,
            file_extension=".md"
        )
        
        result = provider.read("test.md")
        
        assert len(result) == 1
        assert "test.md" in result[0].text
        assert result[0].metadata["extension"] == ".md"
    
    def test_multiple_documents_from_reader(self):
        """Verify handling of multiple documents from reader."""
        config = ReaderProviderConfig()
        
        class MultiDocReader(BaseReader):
            def load_data(self, directory):
                return [
                    Document(text=f"File {i}", metadata={"index": i})
                    for i in range(5)
                ]
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MultiDocReader
        )
        
        result = provider.read("test_dir")
        
        assert len(result) == 5
        assert all(isinstance(doc, Document) for doc in result)
        assert result[0].metadata["index"] == 0
        assert result[4].metadata["index"] == 4
    
    def test_reader_with_complex_metadata(self):
        """Verify handling of complex metadata from reader."""
        config = ReaderProviderConfig()
        
        class MetadataReader(BaseReader):
            def load_data(self, source):
                return [
                    Document(
                        text="Test content",
                        metadata={
                            "source": source,
                            "nested": {
                                "key1": "value1",
                                "key2": "value2"
                            },
                            "list": [1, 2, 3]
                        }
                    )
                ]
        
        provider = LlamaIndexReaderProviderBase(
            config=config,
            reader_cls=MetadataReader
        )
        
        result = provider.read("complex_source")
        
        assert len(result) == 1
        assert result[0].metadata["source"] == "complex_source"
        assert "nested" in result[0].metadata
        assert "list" in result[0].metadata
