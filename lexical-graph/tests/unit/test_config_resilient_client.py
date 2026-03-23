# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for config.py – ResilientClient and _GraphRAGConfig AWS client helpers.

Covers:
  - ResilientClient._create_client (success and SSOTokenLoadError)
  - ResilientClient._refresh_client
  - ResilientClient.__getattr__ proxy with credential expiry retry
  - _GraphRAGConfig.session (with/without profile, error path)
  - _GraphRAGConfig._get_or_create_client (cache hit, cache miss, error)
  - _GraphRAGConfig.s3 / .bedrock / .rds properties
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from botocore.exceptions import SSOTokenLoadError
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_expired_error():
    """Return a botocore ClientError that looks like an expired-token error."""
    return ClientError(
        {"Error": {"Code": "ExpiredToken", "Message": "Token expired"}},
        "SomeOperation",
    )


# ---------------------------------------------------------------------------
# ResilientClient._create_client
# ---------------------------------------------------------------------------

class TestResilientClientCreateClient:

    def test_create_client_success(self):
        from graphrag_toolkit.lexical_graph.config import ResilientClient

        mock_config = MagicMock()
        mock_boto_client = MagicMock()
        mock_config.session.client.return_value = mock_boto_client

        rc = ResilientClient.__new__(ResilientClient)
        rc.config = mock_config
        rc.service_name = "s3"
        rc._lock = __import__("threading").Lock()

        result = rc._create_client()
        assert result is mock_boto_client
        mock_config.session.client.assert_called_once_with("s3")

    def test_create_client_sso_token_error_raises_runtime(self):
        from graphrag_toolkit.lexical_graph.config import ResilientClient

        mock_config = MagicMock()
        mock_config.aws_profile = "my-profile"
        mock_config.session.client.side_effect = SSOTokenLoadError(error_msg="token expired")

        rc = ResilientClient.__new__(ResilientClient)
        rc.config = mock_config
        rc.service_name = "s3"
        rc._lock = __import__("threading").Lock()

        with pytest.raises(RuntimeError, match="SSO token is missing or expired"):
            rc._create_client()


# ---------------------------------------------------------------------------
# ResilientClient._refresh_client
# ---------------------------------------------------------------------------

class TestResilientClientRefreshClient:

    def test_refresh_client_replaces_client(self):
        from graphrag_toolkit.lexical_graph.config import ResilientClient

        mock_config = MagicMock()
        new_client = MagicMock()
        mock_config.session.client.return_value = new_client

        rc = ResilientClient.__new__(ResilientClient)
        rc.config = mock_config
        rc.service_name = "bedrock"
        rc._lock = __import__("threading").Lock()
        rc._client = MagicMock()  # old client

        rc._refresh_client()
        assert rc._client is new_client


# ---------------------------------------------------------------------------
# ResilientClient.__getattr__ proxy
# ---------------------------------------------------------------------------

class TestResilientClientGetattr:

    def _make_rc(self, inner_client):
        from graphrag_toolkit.lexical_graph.config import ResilientClient
        mock_config = MagicMock()
        mock_config.session.client.return_value = inner_client
        rc = ResilientClient.__new__(ResilientClient)
        rc.config = mock_config
        rc.service_name = "s3"
        rc._lock = __import__("threading").Lock()
        rc._client = inner_client
        return rc

    def test_proxy_calls_underlying_method(self):
        inner = MagicMock()
        inner.list_buckets.return_value = {"Buckets": []}
        rc = self._make_rc(inner)
        result = rc.list_buckets()
        assert result == {"Buckets": []}

    def test_proxy_retries_on_expired_token(self):
        """On ExpiredToken the wrapper refreshes and retries."""
        inner = MagicMock()
        # First call raises expired, second succeeds
        inner.list_buckets.side_effect = [_make_expired_error(), {"Buckets": []}]

        from graphrag_toolkit.lexical_graph.config import ResilientClient
        mock_config = MagicMock()
        mock_config.session.client.return_value = inner
        rc = ResilientClient.__new__(ResilientClient)
        rc.config = mock_config
        rc.service_name = "s3"
        rc._lock = __import__("threading").Lock()
        rc._client = inner

        result = rc.list_buckets()
        assert result == {"Buckets": []}

    def test_proxy_non_expired_error_propagates(self):
        """Non-expiry ClientError is re-raised without retry."""
        inner = MagicMock()
        inner.list_buckets.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "ListBuckets"
        )
        rc = self._make_rc(inner)
        with pytest.raises(ClientError, match="AccessDenied"):
            rc.list_buckets()

    def test_proxy_non_callable_attribute(self):
        """Non-callable attributes are returned directly from the inner client."""
        inner = MagicMock()
        inner.meta = "some-meta"
        rc = self._make_rc(inner)
        assert rc.meta == "some-meta"


# ---------------------------------------------------------------------------
# _GraphRAGConfig.session
# ---------------------------------------------------------------------------

class TestGraphRAGConfigSession:

    def test_session_created_without_profile(self):
        from graphrag_toolkit.lexical_graph.config import _GraphRAGConfig
        cfg = _GraphRAGConfig()
        cfg._aws_profile = None
        cfg._aws_region = "us-east-1"

        mock_session = MagicMock()
        with patch("graphrag_toolkit.lexical_graph.config.Boto3Session", return_value=mock_session) as mock_cls:
            s = cfg.session
            assert s is mock_session
            mock_cls.assert_called_once_with(region_name="us-east-1")

    def test_session_created_with_profile(self):
        from graphrag_toolkit.lexical_graph.config import _GraphRAGConfig
        cfg = _GraphRAGConfig()
        cfg._aws_profile = "my-profile"
        cfg._aws_region = "eu-west-1"

        mock_session = MagicMock()
        with patch("graphrag_toolkit.lexical_graph.config.Boto3Session", return_value=mock_session) as mock_cls:
            s = cfg.session
            assert s is mock_session
            mock_cls.assert_called_once_with(profile_name="my-profile", region_name="eu-west-1")

    def test_session_cached_on_second_access(self):
        from graphrag_toolkit.lexical_graph.config import _GraphRAGConfig
        cfg = _GraphRAGConfig()
        cfg._aws_profile = None
        cfg._aws_region = "us-east-1"

        mock_session = MagicMock()
        with patch("graphrag_toolkit.lexical_graph.config.Boto3Session", return_value=mock_session):
            s1 = cfg.session
            s2 = cfg.session
            assert s1 is s2

    def test_session_raises_runtime_on_error(self):
        from graphrag_toolkit.lexical_graph.config import _GraphRAGConfig
        cfg = _GraphRAGConfig()
        cfg._aws_profile = None
        cfg._aws_region = "us-east-1"

        with patch("graphrag_toolkit.lexical_graph.config.Boto3Session", side_effect=Exception("boom")):
            with pytest.raises(RuntimeError, match="Unable to initialize boto3 session"):
                _ = cfg.session


# ---------------------------------------------------------------------------
# _GraphRAGConfig._get_or_create_client
# ---------------------------------------------------------------------------

class TestGraphRAGConfigGetOrCreateClient:

    def _make_cfg(self):
        from graphrag_toolkit.lexical_graph.config import _GraphRAGConfig
        cfg = _GraphRAGConfig()
        cfg._aws_profile = None
        cfg._aws_region = "us-east-1"
        cfg._aws_clients = {}
        return cfg

    def test_returns_cached_client(self):
        cfg = self._make_cfg()
        fake_client = MagicMock()
        cfg._aws_clients["s3"] = fake_client
        result = cfg._get_or_create_client("s3")
        assert result is fake_client

    def test_creates_new_resilient_client(self):
        from graphrag_toolkit.lexical_graph.config import ResilientClient
        cfg = self._make_cfg()

        mock_inner = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_inner

        with patch.object(type(cfg), "session", new_callable=PropertyMock, return_value=mock_session):
            result = cfg._get_or_create_client("s3")
            assert isinstance(result, ResilientClient)
            assert "s3" in cfg._aws_clients

    def test_raises_attribute_error_on_failure(self):
        cfg = self._make_cfg()

        with patch("graphrag_toolkit.lexical_graph.config.ResilientClient", side_effect=Exception("fail")):
            with pytest.raises(AttributeError, match="Failed to create boto3 client"):
                cfg._get_or_create_client("s3")


# ---------------------------------------------------------------------------
# _GraphRAGConfig.s3 / .bedrock / .rds properties
# ---------------------------------------------------------------------------

class TestGraphRAGConfigServiceProperties:

    def _make_cfg_with_mock_client(self):
        from graphrag_toolkit.lexical_graph.config import _GraphRAGConfig
        cfg = _GraphRAGConfig()
        cfg._aws_clients = {}
        fake = MagicMock()
        cfg._aws_clients["s3"] = fake
        cfg._aws_clients["bedrock"] = fake
        cfg._aws_clients["rds"] = fake
        return cfg, fake

    def test_s3_property(self):
        cfg, fake = self._make_cfg_with_mock_client()
        assert cfg.s3 is fake

    def test_bedrock_property(self):
        cfg, fake = self._make_cfg_with_mock_client()
        assert cfg.bedrock is fake

    def test_rds_property(self):
        cfg, fake = self._make_cfg_with_mock_client()
        assert cfg.rds is fake
