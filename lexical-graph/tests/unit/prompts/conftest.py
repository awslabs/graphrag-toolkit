# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures for prompts module tests."""

import pytest
from unittest.mock import Mock, patch


@pytest.fixture
def mock_bedrock_client():
    """Fixture providing a mock Bedrock client."""
    mock_client = Mock()
    mock_client.get_prompt.return_value = {
        'variants': [{
            'templateConfiguration': {
                'text': {
                    'text': 'Mock prompt from Bedrock'
                }
            }
        }]
    }
    return mock_client


@pytest.fixture
def mock_s3_client():
    """Fixture providing a mock S3 client."""
    mock_client = Mock()
    mock_client.get_object.return_value = {
        'Body': Mock(read=Mock(return_value=b'Mock prompt from S3'))
    }
    return mock_client


@pytest.fixture
def patch_bedrock_config(mock_bedrock_client):
    """Fixture that patches BedrockPromptProviderConfig to use mock client."""
    with patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_config.AWSConfig._get_or_create_client') as mock_method:
        mock_method.return_value = mock_bedrock_client
        yield mock_bedrock_client


@pytest.fixture
def patch_s3_config(mock_s3_client):
    """Fixture that patches S3PromptProviderConfig to use mock client."""
    with patch('graphrag_toolkit.lexical_graph.prompts.prompt_provider_config.AWSConfig._get_or_create_client') as mock_method:
        mock_method.return_value = mock_s3_client
        yield mock_s3_client
