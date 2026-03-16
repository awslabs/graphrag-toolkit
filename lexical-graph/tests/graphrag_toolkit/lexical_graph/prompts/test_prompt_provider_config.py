# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for AWSConfig in prompt_provider_config.py.

Covers:
  - AWSConfig.session (with/without profile)
  - AWSConfig._get_or_create_client (cache hit, cache miss)
  - AWSConfig.s3 / .bedrock / .sts properties
"""

import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import AWSConfig
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import ProviderConfig


# Concrete subclass for testing (AWSConfig is abstract via ProviderConfig)
@dataclass(kw_only=True)
class ConcreteAWSConfig(AWSConfig):
    def build(self):
        return None


# ---------------------------------------------------------------------------
# AWSConfig.session
# ---------------------------------------------------------------------------

class TestAWSConfigSession:

    def test_session_without_profile(self):
        cfg = ConcreteAWSConfig(aws_region="us-east-1")
        mock_session = MagicMock()
        with patch("graphrag_toolkit.lexical_graph.prompts.prompt_provider_config.Boto3Session", return_value=mock_session) as mock_cls:
            s = cfg.session
            assert s is mock_session
            mock_cls.assert_called_once_with(region_name="us-east-1")

    def test_session_with_profile(self):
        cfg = ConcreteAWSConfig(aws_profile="my-profile", aws_region="eu-west-1")
        mock_session = MagicMock()
        with patch("graphrag_toolkit.lexical_graph.prompts.prompt_provider_config.Boto3Session", return_value=mock_session) as mock_cls:
            s = cfg.session
            assert s is mock_session
            mock_cls.assert_called_once_with(profile_name="my-profile", region_name="eu-west-1")

    def test_session_cached(self):
        cfg = ConcreteAWSConfig(aws_region="us-east-1")
        mock_session = MagicMock()
        with patch("graphrag_toolkit.lexical_graph.prompts.prompt_provider_config.Boto3Session", return_value=mock_session):
            s1 = cfg.session
            s2 = cfg.session
            assert s1 is s2


# ---------------------------------------------------------------------------
# AWSConfig._get_or_create_client
# ---------------------------------------------------------------------------

class TestAWSConfigGetOrCreateClient:

    def test_returns_cached_client(self):
        cfg = ConcreteAWSConfig()
        fake = MagicMock()
        cfg._aws_clients["s3"] = fake
        assert cfg._get_or_create_client("s3") is fake

    def test_creates_new_client(self):
        cfg = ConcreteAWSConfig(aws_region="us-east-1")
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        with patch("graphrag_toolkit.lexical_graph.prompts.prompt_provider_config.Boto3Session", return_value=mock_session):
            result = cfg._get_or_create_client("s3")
            assert result is mock_client
            assert "s3" in cfg._aws_clients


# ---------------------------------------------------------------------------
# AWSConfig.s3 / .bedrock / .sts
# ---------------------------------------------------------------------------

class TestAWSConfigServiceProperties:

    def _cfg_with_clients(self):
        cfg = ConcreteAWSConfig()
        fake = MagicMock()
        cfg._aws_clients["s3"] = fake
        cfg._aws_clients["bedrock-agent"] = fake
        cfg._aws_clients["sts"] = fake
        return cfg, fake

    def test_s3_property(self):
        cfg, fake = self._cfg_with_clients()
        assert cfg.s3 is fake

    def test_bedrock_property(self):
        cfg, fake = self._cfg_with_clients()
        assert cfg.bedrock is fake

    def test_sts_property(self):
        cfg, fake = self._cfg_with_clients()
        assert cfg.sts is fake
