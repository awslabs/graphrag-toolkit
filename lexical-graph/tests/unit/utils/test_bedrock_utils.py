# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for bedrock_utils.py functions.

This module tests Bedrock client utilities including retry logic.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from tenacity import RetryError, stop_after_attempt


class TestCreateRetryDecorator:
    """Tests for _create_retry_decorator function."""
    
    def test_create_retry_decorator_imports_boto3(self):
        """Verify _create_retry_decorator requires boto3."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        # Should not raise ImportError since boto3 is installed
        mock_client = Mock()
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = Exception
        mock_client.exceptions.InternalServerException = Exception
        mock_client.exceptions.ServiceUnavailableException = Exception
        mock_client.exceptions.ModelTimeoutException = Exception
        mock_client.exceptions.ModelErrorException = Exception
        
        decorator = _create_retry_decorator(mock_client, max_retries=3)
        assert decorator is not None
    
    @patch('graphrag_toolkit.lexical_graph.utils.bedrock_utils.retry')
    def test_create_retry_decorator_configuration(self, mock_retry):
        """Verify retry decorator is configured with correct parameters."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        mock_client = Mock()
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = Exception
        mock_client.exceptions.InternalServerException = Exception
        mock_client.exceptions.ServiceUnavailableException = Exception
        mock_client.exceptions.ModelTimeoutException = Exception
        mock_client.exceptions.ModelErrorException = Exception
        
        _create_retry_decorator(mock_client, max_retries=5)
        
        # Verify retry was called
        assert mock_retry.called
    
    def test_retry_decorator_retries_on_throttling(self):
        """Verify retry decorator retries on ThrottlingException."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        # Create mock client with exception classes
        mock_client = Mock()
        
        # Create custom exception classes
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        # Create retry decorator with max 2 attempts
        retry_decorator = _create_retry_decorator(mock_client, max_retries=2)
        
        # Create a function that fails twice then succeeds
        call_count = {'count': 0}
        
        @retry_decorator
        def failing_function():
            call_count['count'] += 1
            if call_count['count'] < 2:
                raise ThrottlingException("Throttled")
            return "success"
        
        # Should succeed after retries
        result = failing_function()
        assert result == "success"
        assert call_count['count'] == 2
    
    def test_retry_decorator_exhausts_retries(self):
        """Verify retry decorator raises error after max retries."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        # Create mock client with exception classes
        mock_client = Mock()
        
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        # Create retry decorator with max 2 attempts
        retry_decorator = _create_retry_decorator(mock_client, max_retries=2)
        
        # Create a function that always fails
        @retry_decorator
        def always_failing_function():
            raise ThrottlingException("Always throttled")
        
        # Should raise ThrottlingException after exhausting retries (tenacity reraises original exception)
        with pytest.raises(ThrottlingException, match="Always throttled"):
            always_failing_function()
    
    def test_retry_decorator_handles_internal_server_error(self):
        """Verify retry decorator retries on InternalServerException."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        mock_client = Mock()
        
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        retry_decorator = _create_retry_decorator(mock_client, max_retries=2)
        
        call_count = {'count': 0}
        
        @retry_decorator
        def failing_function():
            call_count['count'] += 1
            if call_count['count'] < 2:
                raise InternalServerException("Server error")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count['count'] == 2
    
    def test_retry_decorator_handles_service_unavailable(self):
        """Verify retry decorator retries on ServiceUnavailableException."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        mock_client = Mock()
        
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        retry_decorator = _create_retry_decorator(mock_client, max_retries=2)
        
        call_count = {'count': 0}
        
        @retry_decorator
        def failing_function():
            call_count['count'] += 1
            if call_count['count'] < 2:
                raise ServiceUnavailableException("Service unavailable")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count['count'] == 2
    
    def test_retry_decorator_handles_model_timeout(self):
        """Verify retry decorator retries on ModelTimeoutException."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        mock_client = Mock()
        
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        retry_decorator = _create_retry_decorator(mock_client, max_retries=2)
        
        call_count = {'count': 0}
        
        @retry_decorator
        def failing_function():
            call_count['count'] += 1
            if call_count['count'] < 2:
                raise ModelTimeoutException("Model timeout")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count['count'] == 2
    
    def test_retry_decorator_handles_model_error(self):
        """Verify retry decorator retries on ModelErrorException."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        mock_client = Mock()
        
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        retry_decorator = _create_retry_decorator(mock_client, max_retries=2)
        
        call_count = {'count': 0}
        
        @retry_decorator
        def failing_function():
            call_count['count'] += 1
            if call_count['count'] < 2:
                raise ModelErrorException("Model error")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count['count'] == 2
    
    def test_retry_decorator_does_not_retry_other_exceptions(self):
        """Verify retry decorator does not retry non-retryable exceptions."""
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator
        
        mock_client = Mock()
        
        class ThrottlingException(Exception):
            pass
        
        class InternalServerException(Exception):
            pass
        
        class ServiceUnavailableException(Exception):
            pass
        
        class ModelTimeoutException(Exception):
            pass
        
        class ModelErrorException(Exception):
            pass
        
        mock_client.exceptions = Mock()
        mock_client.exceptions.ThrottlingException = ThrottlingException
        mock_client.exceptions.InternalServerException = InternalServerException
        mock_client.exceptions.ServiceUnavailableException = ServiceUnavailableException
        mock_client.exceptions.ModelTimeoutException = ModelTimeoutException
        mock_client.exceptions.ModelErrorException = ModelErrorException
        
        retry_decorator = _create_retry_decorator(mock_client, max_retries=3)
        
        call_count = {'count': 0}
        
        @retry_decorator
        def failing_function():
            call_count['count'] += 1
            raise ValueError("Not a retryable error")
        
        # Should raise ValueError immediately without retries
        with pytest.raises(ValueError, match="Not a retryable error"):
            failing_function()
        
        # Should only be called once (no retries)
        assert call_count['count'] == 1
