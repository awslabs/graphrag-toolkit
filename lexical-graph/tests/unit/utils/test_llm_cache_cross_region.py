# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for cross-region client recreation after pickle in LLMCache.

Reproduces issue #344: when BedrockConverse is configured with a region_name
different from the deployment region (GraphRAGConfig.aws_region), the client
recreation in LLMCache uses the wrong region after unpickling.
"""

import pickle
import pytest
from contextlib import contextmanager
from unittest.mock import patch, MagicMock

from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from llama_index.llms.bedrock_converse import BedrockConverse

@contextmanager
def patched_config(mock_session):
    """Patch GraphRAGConfig in llm_cache so client creation can run.

    The connection pool is sized from extraction_num_threads_per_worker, which
    is compared against ints, so it needs a real number, not a MagicMock.
    """
    target = 'graphrag_toolkit.lexical_graph.utils.llm_cache.GraphRAGConfig'
    with patch(target) as mock_config:
        mock_config.session = mock_session
        mock_config.extraction_num_threads_per_worker = 32
        yield mock_config

def assert_client_region(mock_session, expected_region):
    """Assert the client was built exactly once, in the LLM's own region."""
    mock_session.client.assert_called_once()
    call_args = mock_session.client.call_args
    assert call_args[1].get('region_name') == expected_region, \
        f"Client must be created with region_name={expected_region!r}, got: {call_args}"


class TestCrossRegionClientRecreation:
    """Verify LLMCache recreates the boto3 client in the LLM's configured region."""

    @patch('boto3.Session')
    def test_predict_recreates_client_with_llm_region(self, mock_boto_session):
        """After pickle round-trip, _client must be recreated in the LLM's
        region_name, not GraphRAGConfig.aws_region (issue #344)."""
        llm = BedrockConverse(model='us.anthropic.claude-sonnet-4-6', region_name='us-west-2')

        # Simulate pickle round-trip (ProcessPoolExecutor does this)
        data = pickle.dumps(llm)
        llm_unpickled = pickle.loads(data)

        # Confirm the bug preconditions
        assert not hasattr(llm_unpickled, '_client'), "_client should be gone after unpickle"
        assert llm_unpickled.region_name == 'us-west-2', "region_name should survive pickle"

        cache = LLMCache(llm=llm_unpickled, enable_cache=False)

        # Mock GraphRAGConfig.session to track how client is created
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client

        with patched_config(mock_session):
            # Directly trigger the client recreation logic by calling predict,
            # which will fail after client recreation (no real Bedrock), but we
            # only care that the client was created with correct region.
            try:
                mock_prompt = MagicMock()
                mock_prompt.format.return_value = 'formatted'
                cache.predict(mock_prompt)
            except Exception:
                pass  # Expected — no real Bedrock endpoint

        assert_client_region(mock_session, 'us-west-2')

    @patch('boto3.Session')
    def test_stream_recreates_client_with_llm_region(self, mock_boto_session):
        """stream() path must also recreate client in correct region."""
        llm = BedrockConverse(model='us.anthropic.claude-sonnet-4-6', region_name='us-west-2')

        data = pickle.dumps(llm)
        llm_unpickled = pickle.loads(data)
        cache = LLMCache(llm=llm_unpickled, enable_cache=False)

        mock_session = MagicMock()
        mock_session.client.return_value = MagicMock()

        with patched_config(mock_session):
            try:
                mock_prompt = MagicMock()
                mock_prompt.format.return_value = 'formatted'
                cache.stream(mock_prompt)
            except Exception:
                pass

        assert_client_region(mock_session, 'us-west-2')

    @patch('boto3.Session')
    def test_cached_predict_recreates_client_with_llm_region(self, mock_boto_session):
        """Cache-enabled predict path must also use correct region."""
        llm = BedrockConverse(model='us.anthropic.claude-sonnet-4-6', region_name='us-west-2')

        data = pickle.dumps(llm)
        llm_unpickled = pickle.loads(data)
        cache = LLMCache(llm=llm_unpickled, enable_cache=True)

        mock_session = MagicMock()
        mock_session.client.return_value = MagicMock()

        with patched_config(mock_session):
            with patch('os.path.exists', return_value=False):
                try:
                    mock_prompt = MagicMock()
                    mock_prompt.format.return_value = 'formatted'
                    cache.predict(mock_prompt)
                except Exception:
                    pass

        assert_client_region(mock_session, 'us-west-2')
