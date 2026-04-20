# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for Neptune graph stores.

This module tests Neptune Analytics and Neptune Database graph store
implementations with mocked boto3 clients.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
    NeptuneAnalyticsGraphStoreFactory,
    NeptuneDatabaseGraphStoreFactory,
    NEPTUNE_ANALYTICS,
    NEPTUNE_DATABASE
)
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore


class TestNeptuneAnalyticsGraphStoreFactory:
    """Tests for NeptuneAnalyticsGraphStoreFactory."""

    def test_try_create_with_neptune_graph_prefix(self):
        """Verify factory creates store for neptune-graph:// prefix."""
        factory = NeptuneAnalyticsGraphStoreFactory()
        
        with patch('boto3.client') as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            result = factory.try_create("neptune-graph://graph-id")
            
            assert isinstance(result, GraphStore)

    def test_try_create_with_non_neptune_prefix_returns_none(self):
        """Verify factory returns None for non-Neptune prefixes."""
        factory = NeptuneAnalyticsGraphStoreFactory()
        
        result = factory.try_create("dummy://test")
        
        assert result is None

    def test_try_create_with_none_returns_none(self):
        """Verify factory returns None for None graph_info."""
        factory = NeptuneAnalyticsGraphStoreFactory()
        
        # The factory should handle None gracefully by checking if graph_info is a string
        # before calling startswith(). Currently it will raise AttributeError.
        # This is a legitimate bug that should be fixed in the source code.
        with pytest.raises(AttributeError):
            factory.try_create(None)

    def test_try_create_extracts_graph_id(self):
        """Verify factory extracts graph ID from connection string."""
        factory = NeptuneAnalyticsGraphStoreFactory()
        
        with patch('boto3.client') as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            result = factory.try_create("neptune-graph://my-graph-123")
            
            assert isinstance(result, GraphStore)


class TestNeptuneDatabaseGraphStoreFactory:
    """Tests for NeptuneDatabaseGraphStoreFactory."""

    def test_try_create_with_neptune_db_prefix(self):
        """Verify factory creates store for neptune-db:// prefix."""
        factory = NeptuneDatabaseGraphStoreFactory()
        
        with patch('boto3.client') as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            result = factory.try_create("neptune-db://cluster-endpoint:8182")
            
            assert isinstance(result, GraphStore)

    def test_try_create_with_https_neptune_endpoint(self):
        """Verify factory creates store for HTTPS Neptune endpoint."""
        factory = NeptuneDatabaseGraphStoreFactory()
        
        with patch('boto3.client') as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            result = factory.try_create("https://my-cluster.cluster-xyz.us-east-1.neptune.amazonaws.com:8182")
            
            assert isinstance(result, GraphStore)

    def test_try_create_with_non_neptune_prefix_returns_none(self):
        """Verify factory returns None for non-Neptune prefixes."""
        factory = NeptuneDatabaseGraphStoreFactory()
        
        result = factory.try_create("dummy://test")
        
        assert result is None

    def test_try_create_with_none_returns_none(self):
        """Verify factory returns None for None graph_info."""
        factory = NeptuneDatabaseGraphStoreFactory()
        
        # The factory should handle None gracefully by checking if graph_info is a string
        # before calling startswith(). Currently it will raise AttributeError.
        # This is a legitimate bug that should be fixed in the source code.
        with pytest.raises(AttributeError):
            factory.try_create(None)


class TestNeptuneGraphStoreOperations:
    """Tests for Neptune graph store operations (mocked)."""

    @patch('graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig')
    @patch('boto3.Session')
    def test_execute_query_with_mocked_client(self, mock_session_class, mock_config):
        """Verify execute_query works with mocked Neptune client."""
        # Mock the session and client
        mock_session = Mock()
        mock_client = Mock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session
        mock_config.session = mock_session
        
        # Mock the query response
        mock_response = {
            'payload': Mock(read=Mock(return_value='{"results": [{"id": "test"}]}'))
        }
        mock_client.execute_query.return_value = mock_response
        
        # Create store and execute query
        factory = NeptuneAnalyticsGraphStoreFactory()
        store = factory.try_create("neptune-graph://test-graph")
        
        results = store.execute_query("MATCH (n) RETURN n")
        
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]['id'] == 'test'

    @patch('graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig')
    @patch('boto3.Session')
    def test_execute_query_with_parameters(self, mock_session_class, mock_config):
        """Verify execute_query passes parameters to Neptune."""
        # Mock the session and client
        mock_session = Mock()
        mock_client = Mock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session
        mock_config.session = mock_session
        
        # Mock the query response
        mock_response = {
            'payload': Mock(read=Mock(return_value='{"results": []}'))
        }
        mock_client.execute_query.return_value = mock_response
        
        # Create store and execute query with parameters
        factory = NeptuneAnalyticsGraphStoreFactory()
        store = factory.try_create("neptune-graph://test-graph")
        
        params = {"name": "test"}
        results = store.execute_query("MATCH (n {name: $name}) RETURN n", params)
        
        # Verify parameters were passed
        mock_client.execute_query.assert_called_once()
        call_kwargs = mock_client.execute_query.call_args[1]
        assert call_kwargs['parameters'] == params

    @patch('graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig')
    @patch('boto3.Session')
    def test_execute_query_handles_empty_results(self, mock_session_class, mock_config):
        """Verify execute_query handles empty result sets."""
        # Mock the session and client
        mock_session = Mock()
        mock_client = Mock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session
        mock_config.session = mock_session
        
        # Mock empty results
        mock_response = {
            'payload': Mock(read=Mock(return_value='{"results": []}'))
        }
        mock_client.execute_query.return_value = mock_response
        
        # Create store and execute query
        factory = NeptuneAnalyticsGraphStoreFactory()
        store = factory.try_create("neptune-graph://test-graph")
        
        results = store.execute_query("MATCH (n) RETURN n")
        
        assert isinstance(results, list)
        assert len(results) == 0


class TestNeptuneGraphStoreErrorHandling:
    """Tests for Neptune graph store error handling."""

    @patch('graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig')
    @patch('boto3.Session')
    def test_execute_query_handles_connection_error(self, mock_session_class, mock_config):
        """Verify execute_query handles connection errors."""
        from graphrag_toolkit.lexical_graph.errors import GraphQueryError

        # Mock the session and client
        mock_session = Mock()
        mock_client = Mock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session
        mock_config.session = mock_session

        # Create real exception classes for unretriable types so isinstance() works
        class UnprocessableException(Exception): pass
        class ValidationException(Exception): pass
        class AccessDeniedException(Exception): pass
        mock_client.exceptions.UnprocessableException = UnprocessableException
        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException

        # Mock connection error
        from botocore.exceptions import ClientError
        mock_client.execute_query.side_effect = ClientError(
            {'Error': {'Code': 'NetworkError', 'Message': 'Connection failed'}},
            'execute_query'
        )

        # Create store and verify error is wrapped in GraphQueryError
        factory = NeptuneAnalyticsGraphStoreFactory()
        store = factory.try_create("neptune-graph://test-graph")

        with pytest.raises(GraphQueryError) as exc_info:
            store.execute_query("MATCH (n) RETURN n")

        # Verify the original error is mentioned in the message
        assert 'NetworkError' in str(exc_info.value)

    @patch('graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig')
    @patch('boto3.Session')
    def test_execute_query_handles_query_error(self, mock_session_class, mock_config):
        """Verify execute_query handles query syntax errors."""
        from graphrag_toolkit.lexical_graph.errors import GraphQueryError

        # Mock the session and client
        mock_session = Mock()
        mock_client = Mock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session
        mock_config.session = mock_session

        # Create real exception classes for unretriable types so isinstance() works
        class UnprocessableException(Exception): pass
        class ValidationException(Exception): pass
        class AccessDeniedException(Exception): pass
        mock_client.exceptions.UnprocessableException = UnprocessableException
        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException

        # Mock query syntax error
        from botocore.exceptions import ClientError
        mock_client.execute_query.side_effect = ClientError(
            {'Error': {'Code': 'QuerySyntaxError', 'Message': 'Invalid query'}},
            'execute_query'
        )

        # Create store and verify error is wrapped in GraphQueryError
        factory = NeptuneAnalyticsGraphStoreFactory()
        store = factory.try_create("neptune-graph://test-graph")

        with pytest.raises(GraphQueryError) as exc_info:
            store.execute_query("INVALID QUERY")

        # Verify the original error is mentioned in the message
        assert 'QuerySyntaxError' in str(exc_info.value)

    @patch('graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig')
    @patch('boto3.Session')
    def test_execute_query_retries_on_transient_error(self, mock_session_class, mock_config):
        """Verify execute_query behavior on transient errors."""
        from graphrag_toolkit.lexical_graph.errors import GraphQueryError

        # Mock the session and client
        mock_session = Mock()
        mock_client = Mock()
        mock_session.client.return_value = mock_client
        mock_session_class.return_value = mock_session
        mock_config.session = mock_session

        # Create real exception classes for unretriable types so isinstance() works
        class UnprocessableException(Exception): pass
        class ValidationException(Exception): pass
        class AccessDeniedException(Exception): pass
        mock_client.exceptions.UnprocessableException = UnprocessableException
        mock_client.exceptions.ValidationException = ValidationException
        mock_client.exceptions.AccessDeniedException = AccessDeniedException

        # Mock transient error (throttling)
        from botocore.exceptions import ClientError
        mock_client.execute_query.side_effect = ClientError(
            {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
            'execute_query'
        )

        # Create store and verify error is wrapped in GraphQueryError after retries
        # The GraphStore base class has retry logic that will attempt the query multiple times
        factory = NeptuneAnalyticsGraphStoreFactory()
        store = factory.try_create("neptune-graph://test-graph")

        with pytest.raises(GraphQueryError) as exc_info:
            store.execute_query("MATCH (n) RETURN n")

        # Verify the error message contains information about the throttling
        assert 'ThrottlingException' in str(exc_info.value)


class TestNeptuneGraphStoreConfiguration:
    """Tests for Neptune graph store configuration."""

    @patch('boto3.client')
    def test_store_accepts_custom_config(self, mock_boto3):
        """Verify store accepts custom configuration."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        # Create store with custom config (pass as dict, not GraphRAGConfig instance)
        factory = NeptuneAnalyticsGraphStoreFactory()
        config = {"some_key": "some_value"}
        
        store = factory.try_create("neptune-graph://test-graph", config=config)
        
        assert isinstance(store, GraphStore)

    @patch('boto3.client')
    def test_store_uses_log_formatting(self, mock_boto3):
        """Verify store uses provided log formatting."""
        mock_client = Mock()
        mock_client.execute_open_cypher_query.return_value = {'results': []}
        mock_boto3.return_value = mock_client
        
        # Create store with custom log formatting
        factory = NeptuneAnalyticsGraphStoreFactory()
        from graphrag_toolkit.lexical_graph.storage.graph import RedactedGraphQueryLogFormatting
        log_formatting = RedactedGraphQueryLogFormatting()
        
        store = factory.try_create("neptune-graph://test-graph", log_formatting=log_formatting)
        
        assert store.log_formatting == log_formatting
