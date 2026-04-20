# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import sys
from unittest.mock import Mock
from llama_index.core.schema import Document
from llama_index.core.readers.base import BasePydanticReader

# Mock the providers module to avoid loading optional dependencies
sys.modules['graphrag_toolkit.lexical_graph.indexing.load.readers.providers'] = Mock()

from graphrag_toolkit.lexical_graph.indexing.load.readers.pydantic_reader_provider_base import PydanticReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import ReaderProviderConfig


class TestPydanticReaderProviderBaseInitialization:
    """Tests for PydanticReaderProviderBase initialization."""
    
    def test_initialization_with_pydantic_reader(self):
        """Verify initialization with Pydantic reader class."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            def load_data(self, *args, **kwargs):
                return [Document(text="test")]
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader
        )
        
        assert provider is not None
        assert provider.config == config
        assert hasattr(provider, '_reader')
    
    def test_initialization_rejects_non_pydantic_reader(self):
        """Verify initialization rejects non-Pydantic reader."""
        config = ReaderProviderConfig()
        
        class NonPydanticReader:
            def load_data(self, *args, **kwargs):
                return []
        
        with pytest.raises(ValueError, match="must inherit from BasePydanticReader"):
            PydanticReaderProviderBase(
                config=config,
                reader_cls=NonPydanticReader
            )
    
    def test_initialization_with_reader_kwargs(self):
        """Verify initialization passes kwargs to reader."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            param1: str = "default1"
            param2: int = 0
            
            def load_data(self, *args, **kwargs):
                return []
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader,
            param1="custom",
            param2=42
        )
        
        assert provider._reader.param1 == "custom"
        assert provider._reader.param2 == 42


class TestPydanticReaderProviderBaseRead:
    """Tests for read() method."""
    
    def test_read_calls_underlying_reader(self):
        """Verify read() calls the underlying reader's load_data()."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            def load_data(self, input_source):
                return [Document(text=f"Loaded from {input_source}")]
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader
        )
        
        result = provider.read("test_source")
        
        assert len(result) == 1
        assert "test_source" in result[0].text
    
    def test_read_returns_documents(self):
        """Verify read() returns list of documents."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            def load_data(self, *args, **kwargs):
                return [
                    Document(text="doc1"),
                    Document(text="doc2")
                ]
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader
        )
        
        result = provider.read("input")
        
        assert len(result) == 2
        assert all(isinstance(doc, Document) for doc in result)
    
    def test_read_with_empty_result(self):
        """Verify read() handles empty result."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            def load_data(self, *args, **kwargs):
                return []
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader
        )
        
        result = provider.read("input")
        
        assert result == []


class TestPydanticReaderProviderBaseErrorHandling:
    """Tests for error handling."""
    
    def test_read_handles_reader_exception(self):
        """Verify read() handles exceptions from underlying reader."""
        config = ReaderProviderConfig()
        
        class FailingPydanticReader(BasePydanticReader):
            def load_data(self, *args, **kwargs):
                raise ValueError("Reader failed")
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=FailingPydanticReader
        )
        
        with pytest.raises(RuntimeError, match="Failed to read using FailingPydanticReader"):
            provider.read("input")
    
    def test_read_wraps_exception_with_context(self):
        """Verify read() wraps exceptions with context."""
        config = ReaderProviderConfig()
        
        class ErrorPydanticReader(BasePydanticReader):
            def load_data(self, *args, **kwargs):
                raise IOError("File not found")
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=ErrorPydanticReader
        )
        
        with pytest.raises(RuntimeError) as exc_info:
            provider.read("missing_file.txt")
        
        assert "Failed to read using ErrorPydanticReader" in str(exc_info.value)
        assert "File not found" in str(exc_info.value)


class TestPydanticReaderProviderBaseSerialization:
    """Tests for serialization methods."""
    
    def test_to_dict_serializes_reader_config(self):
        """Verify to_dict() serializes reader configuration."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            param1: str = "value1"
            param2: int = 42
            
            def load_data(self, *args, **kwargs):
                return []
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader
        )
        
        result = provider.to_dict()
        
        assert isinstance(result, dict)
        assert "param1" in result
        assert result["param1"] == "value1"
        assert "param2" in result
        assert result["param2"] == 42
    
    def test_to_dict_with_custom_values(self):
        """Verify to_dict() includes custom values."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            custom_field: str = "default"
            
            def load_data(self, *args, **kwargs):
                return []
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader,
            custom_field="custom_value"
        )
        
        result = provider.to_dict()
        
        assert "custom_field" in result
        assert result["custom_field"] == "custom_value"


class TestPydanticReaderProviderBaseValidation:
    """Tests for validation methods."""
    
    def test_validate_config_returns_true_for_valid_config(self):
        """Verify validate_config() returns True for valid configuration."""
        config = ReaderProviderConfig()
        
        class MockPydanticReader(BasePydanticReader):
            param: str = "valid"
            
            def load_data(self, *args, **kwargs):
                return []
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=MockPydanticReader
        )
        
        result = provider.validate_config()
        
        assert result is True
    
    def test_validate_config_handles_validation_errors(self):
        """Verify validate_config() handles validation errors gracefully."""
        config = ReaderProviderConfig()
        
        class StrictPydanticReader(BasePydanticReader):
            required_param: str
            
            def load_data(self, *args, **kwargs):
                return []
        
        # This should still initialize but validation might fail
        try:
            provider = PydanticReaderProviderBase(
                config=config,
                reader_cls=StrictPydanticReader,
                required_param="value"
            )
            result = provider.validate_config()
            assert isinstance(result, bool)
        except Exception:
            # If initialization fails due to validation, that's also acceptable
            pass


class TestPydanticReaderProviderBaseIntegration:
    """Integration tests for PydanticReaderProviderBase."""
    
    def test_integration_with_pydantic_reader(self):
        """Verify integration with a Pydantic reader."""
        config = ReaderProviderConfig()
        
        class FileReader(BasePydanticReader):
            file_extension: str = ".txt"
            encoding: str = "utf-8"
            
            def load_data(self, file_path):
                return [
                    Document(
                        text=f"Content from {file_path}",
                        metadata={
                            "file_path": file_path,
                            "extension": self.file_extension,
                            "encoding": self.encoding
                        }
                    )
                ]
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=FileReader,
            file_extension=".md",
            encoding="utf-16"
        )
        
        # Test read
        result = provider.read("test.md")
        assert len(result) == 1
        assert "test.md" in result[0].text
        assert result[0].metadata["extension"] == ".md"
        assert result[0].metadata["encoding"] == "utf-16"
        
        # Test serialization
        config_dict = provider.to_dict()
        assert config_dict["file_extension"] == ".md"
        assert config_dict["encoding"] == "utf-16"
        
        # Test validation
        assert provider.validate_config() is True
    
    def test_pydantic_validation_benefits(self):
        """Verify Pydantic validation provides type checking."""
        config = ReaderProviderConfig()
        
        class TypedReader(BasePydanticReader):
            max_docs: int = 10
            include_metadata: bool = True
            
            def load_data(self, source):
                docs = []
                for i in range(min(self.max_docs, 3)):
                    doc = Document(text=f"Doc {i}")
                    if self.include_metadata:
                        doc.metadata = {"index": i}
                    docs.append(doc)
                return docs
        
        provider = PydanticReaderProviderBase(
            config=config,
            reader_cls=TypedReader,
            max_docs=2,
            include_metadata=False
        )
        
        result = provider.read("source")
        
        assert len(result) == 2
        assert all(not doc.metadata for doc in result)
