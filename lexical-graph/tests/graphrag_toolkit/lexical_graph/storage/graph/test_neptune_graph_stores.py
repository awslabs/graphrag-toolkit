# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for neptune_graph_stores.py.

Covers:
  - NeptuneAnalyticsGraphStoreFactory.try_create
  - NeptuneDatabaseGraphStoreFactory.try_create
  - NeptuneAnalyticsClient.__getstate__
  - NeptuneAnalyticsClient.client property
  - NeptuneDatabaseClient.__getstate__
  - NeptuneDatabaseClient.client property
  - intercept_before_parse (valid JSON, invalid JSON with error match, no match)
"""

import json
import pytest
from unittest.mock import MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# NeptuneAnalyticsGraphStoreFactory.try_create
# ---------------------------------------------------------------------------

class TestNeptuneAnalyticsGraphStoreFactory:

    def test_try_create_matching_prefix_returns_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
            NeptuneAnalyticsGraphStoreFactory, NeptuneAnalyticsClient
        )
        factory = NeptuneAnalyticsGraphStoreFactory()
        result = factory.try_create("neptune-graph://my-graph-id")
        assert isinstance(result, NeptuneAnalyticsClient)
        assert result.graph_id == "my-graph-id"

    def test_try_create_non_matching_returns_none(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneAnalyticsGraphStoreFactory
        factory = NeptuneAnalyticsGraphStoreFactory()
        result = factory.try_create("neptune-db://some-host")
        assert result is None


# ---------------------------------------------------------------------------
# NeptuneDatabaseGraphStoreFactory.try_create
# ---------------------------------------------------------------------------

class TestNeptuneDatabaseGraphStoreFactory:

    def test_try_create_neptune_db_prefix(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
            NeptuneDatabaseGraphStoreFactory, NeptuneDatabaseClient
        )
        factory = NeptuneDatabaseGraphStoreFactory()
        result = factory.try_create("neptune-db://my-cluster.neptune.amazonaws.com")
        assert isinstance(result, NeptuneDatabaseClient)

    def test_try_create_dns_suffix(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
            NeptuneDatabaseGraphStoreFactory, NeptuneDatabaseClient
        )
        factory = NeptuneDatabaseGraphStoreFactory()
        result = factory.try_create("my-cluster.neptune.amazonaws.com")
        assert isinstance(result, NeptuneDatabaseClient)

    def test_try_create_https_with_dns(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
            NeptuneDatabaseGraphStoreFactory, NeptuneDatabaseClient
        )
        factory = NeptuneDatabaseGraphStoreFactory()
        result = factory.try_create("https://my-cluster.neptune.amazonaws.com")
        assert isinstance(result, NeptuneDatabaseClient)

    def test_try_create_non_matching_returns_none(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneDatabaseGraphStoreFactory
        factory = NeptuneDatabaseGraphStoreFactory()
        result = factory.try_create("neptune-graph://some-graph-id")
        assert result is None


# ---------------------------------------------------------------------------
# NeptuneAnalyticsClient.__getstate__ and .client
# ---------------------------------------------------------------------------

class TestNeptuneAnalyticsClient:

    def _make_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneAnalyticsClient
        return NeptuneAnalyticsClient(graph_id="test-graph")

    def test_getstate_nils_client(self):
        client = self._make_client()
        client._client = MagicMock()
        client.__getstate__()
        assert client._client is None

    def test_client_property_creates_lazily(self):
        client = self._make_client()
        client._client = None

        mock_session = MagicMock()
        mock_boto_client = MagicMock()
        mock_session.client.return_value = mock_boto_client

        with patch("graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            result = client.client
            assert result is mock_boto_client

    def test_client_property_cached(self):
        client = self._make_client()
        existing = MagicMock()
        client._client = existing
        assert client.client is existing


# ---------------------------------------------------------------------------
# NeptuneDatabaseClient.__getstate__ and .client
# ---------------------------------------------------------------------------

class TestNeptuneDatabaseClient:

    def _make_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneDatabaseClient
        return NeptuneDatabaseClient(endpoint_url="https://my-cluster.neptune.amazonaws.com:8182")

    def test_getstate_nils_client(self):
        client = self._make_client()
        client._client = MagicMock()
        client.__getstate__()
        assert client._client is None

    def test_client_property_creates_lazily(self):
        client = self._make_client()
        client._client = None

        mock_session = MagicMock()
        mock_boto_client = MagicMock()
        mock_boto_client.meta.events.register = MagicMock()
        mock_session.client.return_value = mock_boto_client

        with patch("graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores.GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            result = client.client
            assert result is mock_boto_client

    def test_client_property_cached(self):
        client = self._make_client()
        existing = MagicMock()
        client._client = existing
        assert client.client is existing


# ---------------------------------------------------------------------------
# intercept_before_parse
# ---------------------------------------------------------------------------

class TestInterceptBeforeParse:

    def _make_response_dict(self, body_bytes, status_code=200):
        return {
            "status_code": status_code,
            "body": body_bytes,
        }

    def test_non_200_status_returns_early(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import intercept_before_parse
        op_model = MagicMock()
        response_dict = self._make_response_dict(b'{"results":[]}', status_code=500)
        # Should return None without touching body
        result = intercept_before_parse(op_model, response_dict)
        assert result is None
        assert response_dict["body"] == b'{"results":[]}'

    def test_valid_json_body_replaced_with_dummy(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import intercept_before_parse
        op_model = MagicMock()
        original = {"results": [{"a": 1}]}
        response_dict = self._make_response_dict(json.dumps(original).encode("utf-8"))
        customized = {}
        intercept_before_parse(op_model, response_dict, customized_response_dict=customized)
        # Body replaced with dummy
        assert response_dict["body"] == b'{"results":[]}'
        # Actual results stored in customized dict
        assert customized["results"] == original["results"]


# ---------------------------------------------------------------------------
# NeptuneAnalyticsClient._execute_query
# ---------------------------------------------------------------------------

class TestNeptuneAnalyticsClientExecuteQuery:

    def _make_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneAnalyticsClient
        return NeptuneAnalyticsClient(graph_id="test-graph")

    def test_execute_query_returns_results(self):
        client = self._make_client()

        mock_boto_client = MagicMock()
        import json, io
        payload = json.dumps({"results": [{"n": 1}]}).encode("utf-8")
        mock_boto_client.execute_query.return_value = {
            "payload": io.BytesIO(payload)
        }
        client._client = mock_boto_client

        results = client._execute_query("MATCH (n) RETURN n", {})
        assert results == [{"n": 1}]
        mock_boto_client.execute_query.assert_called_once()

    def test_execute_query_passes_parameters(self):
        client = self._make_client()

        mock_boto_client = MagicMock()
        import json, io
        payload = json.dumps({"results": []}).encode("utf-8")
        mock_boto_client.execute_query.return_value = {
            "payload": io.BytesIO(payload)
        }
        client._client = mock_boto_client

        client._execute_query("MATCH (n) WHERE n.id = $id RETURN n", {"id": "abc"})
        call_kwargs = mock_boto_client.execute_query.call_args[1]
        assert call_kwargs["parameters"] == {"id": "abc"}


# ---------------------------------------------------------------------------
# NeptuneDatabaseClient._execute_query
# ---------------------------------------------------------------------------

class TestNeptuneDatabaseClientExecuteQuery:

    def _make_client(self):
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneDatabaseClient
        return NeptuneDatabaseClient(endpoint_url="https://my-cluster.neptune.amazonaws.com:8182")

    def test_execute_query_returns_results(self):
        client = self._make_client()

        mock_boto_client = MagicMock()
        mock_boto_client.execute_open_cypher_query.return_value = {
            "results": [{"n": 1}]
        }
        client._client = mock_boto_client

        results = client._execute_query("MATCH (n) RETURN n", {})
        assert results == [{"n": 1}]
        mock_boto_client.execute_open_cypher_query.assert_called_once()

    def test_execute_query_passes_parameters_as_json(self):
        client = self._make_client()

        mock_boto_client = MagicMock()
        mock_boto_client.execute_open_cypher_query.return_value = {"results": []}
        client._client = mock_boto_client

        client._execute_query("MATCH (n) RETURN n", {"key": "val"})
        call_kwargs = mock_boto_client.execute_open_cypher_query.call_args[1]
        import json
        assert json.loads(call_kwargs["parameters"]) == {"key": "val"}
