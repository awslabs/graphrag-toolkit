# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import asyncio
import sys
from unittest.mock import Mock
from llama_index.core.schema import Document

# Mock the providers module to avoid loading optional dependencies
sys.modules['graphrag_toolkit.lexical_graph.indexing.load.readers.providers'] = Mock()

from graphrag_toolkit.lexical_graph.indexing.load.readers.base_reader_provider import BaseReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig


class TestBaseReaderProviderInitialization:
    """Tests for BaseReaderProvider initialization."""
    
    def test_initialization_with_config(self):
        """Verify BaseReaderProvider initializes with config."""
        config = ReaderProviderConfig()
        
        # Create a concrete subclass for testing
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [Document(text="test")]
        
        provider = TestReader(config=config)
        
        assert provider is not None
        assert provider.config == config
    
    def test_initialization_stores_config(self):
        """Verify config is stored correctly."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return []
        
        provider = TestReader(config=config)
        
        assert hasattr(provider, 'config')
        assert provider.config is config


class TestBaseReaderProviderReadMethod:
    """Tests for read() method."""
    
    def test_read_delegates_to_load_data(self):
        """Verify read() delegates to load_data()."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [Document(text="test document")]
        
        provider = TestReader(config=config)
        result = provider.read("test_input")
        
        assert len(result) == 1
        assert result[0].text == "test document"
    
    def test_read_passes_input_source(self):
        """Verify read() passes input source to load_data()."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                # Return document with input source as text
                return [Document(text=str(args[0]) if args else "no input")]
        
        provider = TestReader(config=config)
        result = provider.read("specific_input")
        
        assert len(result) == 1
        assert "specific_input" in result[0].text


class TestBaseReaderProviderLoadData:
    """Tests for load_data() method."""
    
    def test_load_data_not_implemented_raises_error(self):
        """Verify load_data() raises NotImplementedError in base class."""
        config = ReaderProviderConfig()
        provider = BaseReaderProvider(config=config)
        
        with pytest.raises(NotImplementedError, match="Subclasses must implement load_data method"):
            provider.load_data("test_input")
    
    def test_subclass_can_implement_load_data(self):
        """Verify subclasses can implement load_data()."""
        config = ReaderProviderConfig()
        
        class CustomReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [
                    Document(text="doc1"),
                    Document(text="doc2")
                ]
        
        provider = CustomReader(config=config)
        result = provider.load_data()
        
        assert len(result) == 2
        assert result[0].text == "doc1"
        assert result[1].text == "doc2"


class TestBaseReaderProviderLazyLoading:
    """Tests for lazy_load_data() method."""
    
    def test_lazy_load_data_yields_documents(self):
        """Verify lazy_load_data() yields documents one at a time."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [
                    Document(text="doc1"),
                    Document(text="doc2"),
                    Document(text="doc3")
                ]
        
        provider = TestReader(config=config)
        result = list(provider.lazy_load_data())
        
        assert len(result) == 3
        assert all(isinstance(doc, Document) for doc in result)
    
    def test_lazy_load_data_is_generator(self):
        """Verify lazy_load_data() returns a generator."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [Document(text="test")]
        
        provider = TestReader(config=config)
        result = provider.lazy_load_data()
        
        # Check it's a generator
        assert hasattr(result, '__iter__')
        assert hasattr(result, '__next__')


class TestBaseReaderProviderAsyncLoading:
    """Tests for async loading methods."""
    
    @pytest.mark.asyncio
    async def test_async_load_data_returns_documents(self):
        """Verify aload_data() returns documents asynchronously."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [Document(text="async doc")]
        
        provider = TestReader(config=config)
        result = await provider._async_load_data()
        
        assert len(result) == 1
        assert result[0].text == "async doc"
    
    def test_aload_data_runs_async_load(self):
        """Verify aload_data() runs async loading."""
        config = ReaderProviderConfig()
        
        class TestReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return [Document(text="test")]
        
        provider = TestReader(config=config)
        result = provider.aload_data()
        
        assert len(result) == 1
        assert result[0].text == "test"


class TestBaseReaderProviderIntegration:
    """Integration tests for BaseReaderProvider."""
    
    def test_full_workflow_read_to_documents(self):
        """Verify full workflow from read to documents."""
        config = ReaderProviderConfig()
        
        class FileReader(BaseReaderProvider):
            def load_data(self, file_path, **kwargs):
                # Simulate reading a file
                return [
                    Document(
                        text=f"Content from {file_path}",
                        metadata={"source": file_path}
                    )
                ]
        
        provider = FileReader(config=config)
        
        # Test read method
        docs = provider.read("test.txt")
        assert len(docs) == 1
        assert "test.txt" in docs[0].text
        assert docs[0].metadata["source"] == "test.txt"
        
        # Test lazy loading
        lazy_docs = list(provider.lazy_load_data("test2.txt"))
        assert len(lazy_docs) == 1
        assert "test2.txt" in lazy_docs[0].text
    
    def test_error_handling_in_load_data(self):
        """Verify error handling in load_data implementation."""
        config = ReaderProviderConfig()
        
        class ErrorReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                raise ValueError("Failed to load data")
        
        provider = ErrorReader(config=config)
        
        with pytest.raises(ValueError, match="Failed to load data"):
            provider.read("test")
    
    def test_empty_document_list(self):
        """Verify handling of empty document list."""
        config = ReaderProviderConfig()
        
        class EmptyReader(BaseReaderProvider):
            def load_data(self, *args, **kwargs):
                return []
        
        provider = EmptyReader(config=config)
        result = provider.read("test")
        
        assert result == []
        assert len(result) == 0
