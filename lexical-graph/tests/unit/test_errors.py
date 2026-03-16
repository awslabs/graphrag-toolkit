# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for errors.py module.

This module tests custom exception handling and error messages.
"""

import pytest
from graphrag_toolkit.lexical_graph.errors import (
    ConfigurationError,
    ModelError,
    BatchJobError,
    IndexError,
    GraphQueryError
)


class TestConfigurationError:
    """Tests for ConfigurationError exception."""
    
    def test_custom_exception_initialization(self):
        """Verify ConfigurationError initializes with message."""
        error = ConfigurationError("Invalid configuration")
        assert str(error) == "Invalid configuration"
    
    def test_custom_exception_inheritance(self):
        """Verify ConfigurationError inherits from Exception."""
        error = ConfigurationError("Test error")
        assert isinstance(error, Exception)
    
    def test_custom_exception_message(self):
        """Verify ConfigurationError preserves error message."""
        message = "Configuration parameter 'chunk_size' must be positive"
        error = ConfigurationError(message)
        assert str(error) == message
    
    def test_error_can_be_raised_and_caught(self):
        """Verify ConfigurationError can be raised and caught."""
        with pytest.raises(ConfigurationError, match="Test configuration error"):
            raise ConfigurationError("Test configuration error")


class TestModelError:
    """Tests for ModelError exception."""
    
    def test_custom_exception_initialization(self):
        """Verify ModelError initializes with message."""
        error = ModelError("Model loading failed")
        assert str(error) == "Model loading failed"
    
    def test_custom_exception_inheritance(self):
        """Verify ModelError inherits from Exception."""
        error = ModelError("Test error")
        assert isinstance(error, Exception)
    
    def test_custom_exception_message(self):
        """Verify ModelError preserves error message."""
        message = "Failed to load LLM model: model not found"
        error = ModelError(message)
        assert str(error) == message
    
    def test_error_can_be_raised_and_caught(self):
        """Verify ModelError can be raised and caught."""
        with pytest.raises(ModelError, match="Model initialization failed"):
            raise ModelError("Model initialization failed")


class TestBatchJobError:
    """Tests for BatchJobError exception."""
    
    def test_custom_exception_initialization(self):
        """Verify BatchJobError initializes with message."""
        error = BatchJobError("Batch processing failed")
        assert str(error) == "Batch processing failed"
    
    def test_custom_exception_inheritance(self):
        """Verify BatchJobError inherits from Exception."""
        error = BatchJobError("Test error")
        assert isinstance(error, Exception)
    
    def test_custom_exception_message(self):
        """Verify BatchJobError preserves error message."""
        message = "Batch job failed at item 42: timeout exceeded"
        error = BatchJobError(message)
        assert str(error) == message
    
    def test_error_can_be_raised_and_caught(self):
        """Verify BatchJobError can be raised and caught."""
        with pytest.raises(BatchJobError, match="Batch timeout"):
            raise BatchJobError("Batch timeout")


class TestIndexError:
    """Tests for IndexError exception."""
    
    def test_custom_exception_initialization(self):
        """Verify IndexError initializes with message."""
        error = IndexError("Index creation failed")
        assert str(error) == "Index creation failed"
    
    def test_custom_exception_inheritance(self):
        """Verify IndexError inherits from Exception."""
        error = IndexError("Test error")
        assert isinstance(error, Exception)
    
    def test_custom_exception_message(self):
        """Verify IndexError preserves error message."""
        message = "Failed to create index: insufficient permissions"
        error = IndexError(message)
        assert str(error) == message
    
    def test_error_can_be_raised_and_caught(self):
        """Verify IndexError can be raised and caught."""
        with pytest.raises(IndexError, match="Index not found"):
            raise IndexError("Index not found")


class TestGraphQueryError:
    """Tests for GraphQueryError exception."""
    
    def test_custom_exception_initialization(self):
        """Verify GraphQueryError initializes with message."""
        error = GraphQueryError("Query execution failed")
        assert str(error) == "Query execution failed"
    
    def test_custom_exception_inheritance(self):
        """Verify GraphQueryError inherits from Exception."""
        error = GraphQueryError("Test error")
        assert isinstance(error, Exception)
    
    def test_custom_exception_message(self):
        """Verify GraphQueryError preserves error message."""
        message = "Graph query failed: syntax error in Gremlin query"
        error = GraphQueryError(message)
        assert str(error) == message
    
    def test_error_can_be_raised_and_caught(self):
        """Verify GraphQueryError can be raised and caught."""
        with pytest.raises(GraphQueryError, match="Connection timeout"):
            raise GraphQueryError("Connection timeout")


class TestErrorContext:
    """Tests for error context preservation."""
    
    def test_error_with_context_information(self):
        """Verify errors can include context information in message."""
        context = "tenant_id='acme', operation='index_documents'"
        error = ConfigurationError(f"Configuration error: {context}")
        assert "tenant_id='acme'" in str(error)
        assert "operation='index_documents'" in str(error)
    
    def test_error_chaining(self):
        """Verify errors can be chained with 'from' clause."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise ConfigurationError("Configuration failed") from e
        except ConfigurationError as config_error:
            assert config_error.__cause__ is not None
            assert isinstance(config_error.__cause__, ValueError)
            assert str(config_error.__cause__) == "Original error"
    
    def test_multiple_error_types_distinct(self):
        """Verify different error types are distinct and can be caught separately."""
        errors = [
            ConfigurationError("config"),
            ModelError("model"),
            BatchJobError("batch"),
            IndexError("index"),
            GraphQueryError("query")
        ]
        
        # Each error type should be distinct
        for i, error1 in enumerate(errors):
            for j, error2 in enumerate(errors):
                if i != j:
                    assert type(error1) != type(error2)
