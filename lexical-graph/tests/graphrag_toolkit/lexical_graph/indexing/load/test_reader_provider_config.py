# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for AWSReaderConfigBase.get_boto3_session."""

import pytest
from unittest.mock import MagicMock, patch

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config_base import AWSReaderConfigBase


class ConcreteReaderConfig(AWSReaderConfigBase):
    pass


class TestAWSReaderConfigBaseGetBoto3Session:

    def test_session_without_profile(self):
        cfg = ConcreteReaderConfig(aws_region="us-east-1")
        mock_session = MagicMock()
        with patch("boto3.session.Session", return_value=mock_session) as mock_cls:
            result = cfg.get_boto3_session()
            assert result is mock_session
            mock_cls.assert_called_once_with(region_name="us-east-1")

    def test_session_with_profile(self):
        cfg = ConcreteReaderConfig(aws_profile="my-profile", aws_region="eu-west-1")
        mock_session = MagicMock()
        with patch("boto3.session.Session", return_value=mock_session) as mock_cls:
            result = cfg.get_boto3_session()
            assert result is mock_session
            mock_cls.assert_called_once_with(profile_name="my-profile", region_name="eu-west-1")
