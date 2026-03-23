# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for neo4j_graph_store.py and neo4j_graph_store_factory.py.

Covers:
  - Neo4jDatabaseClient.__init__ (validation branches)
  - Neo4jDatabaseClient.__getstate__
  - Neo4jDatabaseClient.client property (success, ImportError, connection error)
  - Neo4jDatabaseClient.__exit__
  - Neo4jGraphStoreFactory.try_create (matching and non-matching schemes)
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Neo4jDatabaseClient.__init__ validation
# ---------------------------------------------------------------------------

class TestNeo4jDatabaseClientInit:

    def _make_client(self, url, **kwargs):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        return Neo4jDatabaseClient(endpoint_url=url, **kwargs)

    def test_valid_url_parsed(self):
        client = self._make_client("bolt://localhost:7687")
        assert "localhost" in client.connection_string
        assert "7687" in client.connection_string

    def test_missing_host_raises(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        with pytest.raises(ValueError, match="Host missing"):
            Neo4jDatabaseClient(endpoint_url="bolt://:7687")

    def test_missing_port_raises(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        with pytest.raises(ValueError, match="Port missing"):
            Neo4jDatabaseClient(endpoint_url="bolt://localhost")

    def test_username_without_password_raises(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        with pytest.raises(ValueError, match="Password is required"):
            Neo4jDatabaseClient(endpoint_url="bolt://user@localhost:7687")

    def test_non_alphanumeric_database_raises(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        with pytest.raises(ValueError, match="alphanumeric"):
            Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687/my-db")

    def test_valid_database_name(self):
        client = self._make_client("bolt://localhost:7687/neo4j")
        assert client.database == "neo4j"

    def test_url_with_credentials(self):
        client = self._make_client("bolt://user:pass@localhost:7687")
        assert client.username == "user"
        assert client.password == "pass"

    def test_url_with_query_string(self):
        client = self._make_client("bolt://localhost:7687?routing=true")
        assert "routing=true" in client.connection_string


# ---------------------------------------------------------------------------
# Neo4jDatabaseClient.__getstate__
# ---------------------------------------------------------------------------

class TestNeo4jDatabaseClientGetstate:

    def test_getstate_closes_and_nils_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        mock_driver = MagicMock()
        client._client = mock_driver

        state = client.__getstate__()
        mock_driver.close.assert_called_once()
        assert client._client is None

    def test_getstate_no_client_is_safe(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        client._client = None
        # Should not raise
        client.__getstate__()


# ---------------------------------------------------------------------------
# Neo4jDatabaseClient.client property
# ---------------------------------------------------------------------------

class TestNeo4jDatabaseClientProperty:

    def test_client_import_error_raises(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        client._client = None

        with patch.dict("sys.modules", {"neo4j": None}):
            with pytest.raises(ImportError, match="Neo4j package not found"):
                _ = client.client

    def test_client_connection_error_raises(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        client._client = None

        mock_neo4j = MagicMock()
        mock_neo4j.GraphDatabase.driver.side_effect = Exception("connection refused")

        with patch.dict("sys.modules", {"neo4j": mock_neo4j}):
            with pytest.raises(ConnectionError, match="Unexpected error while connecting to Neo4j"):
                _ = client.client

    def test_client_cached_on_second_access(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        mock_driver = MagicMock()
        client._client = mock_driver

        mock_neo4j = MagicMock()
        with patch.dict("sys.modules", {"neo4j": mock_neo4j}):
            result = client.client
            assert result is mock_driver
            # GraphDatabase.driver should NOT be called since _client is already set
            mock_neo4j.GraphDatabase.driver.assert_not_called()


# ---------------------------------------------------------------------------
# Neo4jDatabaseClient.__exit__
# ---------------------------------------------------------------------------

class TestNeo4jDatabaseClientExit:

    def test_exit_closes_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        mock_driver = MagicMock()
        client._client = mock_driver

        client.__exit__(None, None, None)
        mock_driver.close.assert_called_once()

    def test_exit_no_client_is_safe(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")
        client._client = None
        # Should not raise
        client.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# Neo4jGraphStoreFactory.try_create
# ---------------------------------------------------------------------------

class TestNeo4jGraphStoreFactory:

    def test_try_create_bolt_scheme_returns_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient

        factory = Neo4jGraphStoreFactory()
        result = factory.try_create("bolt://localhost:7687")
        assert isinstance(result, Neo4jDatabaseClient)

    def test_try_create_neo4j_scheme_returns_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient

        factory = Neo4jGraphStoreFactory()
        result = factory.try_create("neo4j://localhost:7687")
        assert isinstance(result, Neo4jDatabaseClient)

    def test_try_create_non_matching_returns_none(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory

        factory = Neo4jGraphStoreFactory()
        result = factory.try_create("neptune-graph://some-graph-id")
        assert result is None

    def test_try_create_bolt_plus_s_scheme(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store_factory import Neo4jGraphStoreFactory
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient

        factory = Neo4jGraphStoreFactory()
        result = factory.try_create("bolt+s://localhost:7687")
        assert isinstance(result, Neo4jDatabaseClient)


# ---------------------------------------------------------------------------
# Neo4jDatabaseClient._execute_query
# ---------------------------------------------------------------------------

class TestNeo4jDatabaseClientExecuteQuery:

    def _make_client_with_mock_driver(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")

        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_record = MagicMock()
        mock_record.data.return_value = {"key": "value"}
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([mock_record]))
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_driver.session.return_value = mock_session
        mock_driver.execute_query.return_value = MagicMock()

        # Pre-set _client so the property doesn't try to import neo4j
        client._client = mock_driver
        return client, mock_driver, mock_session

    def test_execute_query_returns_results(self):
        client, mock_driver, mock_session = self._make_client_with_mock_driver()
        mock_neo4j = MagicMock()
        with patch.dict("sys.modules", {"neo4j": mock_neo4j}):
            results = client._execute_query("MATCH (n) RETURN n", {})
        assert results == [{"key": "value"}]

    def test_execute_query_with_database(self):
        client, mock_driver, mock_session = self._make_client_with_mock_driver()
        client.database = "neo4j"
        mock_neo4j = MagicMock()
        with patch.dict("sys.modules", {"neo4j": mock_neo4j}):
            results = client._execute_query("MATCH (n) RETURN n", {})
        mock_driver.execute_query.assert_called()

    def test_execute_query_default_empty_params(self):
        client, mock_driver, mock_session = self._make_client_with_mock_driver()
        mock_neo4j = MagicMock()
        with patch.dict("sys.modules", {"neo4j": mock_neo4j}):
            results = client._execute_query("MATCH (n) RETURN n")
        assert isinstance(results, list)


# ---------------------------------------------------------------------------
# Neo4jDatabaseClient.init
# ---------------------------------------------------------------------------

class TestNeo4jDatabaseClientInit:

    def test_init_creates_indexes(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")

        mock_graph_store = MagicMock()
        mock_graph_store.tenant_id = "default"
        mock_graph_store.execute_query_with_retry = MagicMock()

        client.init(graph_store=mock_graph_store)
        # Should have called execute_query_with_retry for each index creation statement
        assert mock_graph_store.execute_query_with_retry.call_count >= 6

    def test_init_uses_self_when_no_graph_store(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient
        client = Neo4jDatabaseClient(endpoint_url="bolt://localhost:7687")

        with patch(
            "graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store.Neo4jDatabaseClient.execute_query_with_retry"
        ) as mock_exec:
            client.init()
            assert mock_exec.call_count >= 6
