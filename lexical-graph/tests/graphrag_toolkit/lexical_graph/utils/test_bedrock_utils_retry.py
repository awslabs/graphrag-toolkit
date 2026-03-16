# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for bedrock_utils._create_retry_decorator.

Covers:
  - Returns a callable decorator when boto3 is available
  - Raises ImportError when boto3 is not available
"""

import pytest
from unittest.mock import MagicMock, patch


class TestCreateRetryDecorator:

    def test_returns_callable_with_boto3(self):
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator

        mock_client = MagicMock()
        mock_client.exceptions.ThrottlingException = Exception
        mock_client.exceptions.InternalServerException = Exception
        mock_client.exceptions.ServiceUnavailableException = Exception
        mock_client.exceptions.ModelTimeoutException = Exception
        mock_client.exceptions.ModelErrorException = Exception

        decorator = _create_retry_decorator(mock_client, max_retries=3)
        assert callable(decorator)

    def test_raises_import_error_without_boto3(self):
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator

        mock_client = MagicMock()
        mock_client.exceptions.ThrottlingException = Exception
        mock_client.exceptions.InternalServerException = Exception
        mock_client.exceptions.ServiceUnavailableException = Exception
        mock_client.exceptions.ModelTimeoutException = Exception
        mock_client.exceptions.ModelErrorException = Exception

        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(ImportError, match="boto3 package not found"):
                _create_retry_decorator(mock_client, max_retries=3)

    def test_decorator_wraps_function(self):
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import _create_retry_decorator

        mock_client = MagicMock()
        mock_client.exceptions.ThrottlingException = Exception
        mock_client.exceptions.InternalServerException = Exception
        mock_client.exceptions.ServiceUnavailableException = Exception
        mock_client.exceptions.ModelTimeoutException = Exception
        mock_client.exceptions.ModelErrorException = Exception

        decorator = _create_retry_decorator(mock_client, max_retries=1)

        @decorator
        def my_func():
            return "ok"

        assert my_func() == "ok"
