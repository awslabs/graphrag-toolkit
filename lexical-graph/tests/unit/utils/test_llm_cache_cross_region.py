# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for cross-region client recreation after pickle in LLMCache.

Reproduces issue #344: when BedrockConverse is configured with a region_name
different from the deployment region (GraphRAGConfig.aws_region), the client
recreation in LLMCache uses the wrong region after unpickling.
"""

import pickle
import threading
import pytest
from contextlib import contextmanager
from unittest.mock import patch, MagicMock
from pydantic import ValidationError

from graphrag_toolkit.lexical_graph.config import BOTOCORE_DEFAULT_MAX_POOL_CONNECTIONS
from graphrag_toolkit.lexical_graph.utils.llm_concurrency import MIN_POOL_SIZE, MAX_POOL_SIZE
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from llama_index.llms.bedrock_converse import BedrockConverse

CONFIGURED_NUM_THREADS = 32

@contextmanager
def patched_config(mock_session):
    """Patch GraphRAGConfig in llm_cache so client creation can run.

    The connection pool is sized from a thread count, which is multiplied and
    compared against ints, so it needs a real number, not a MagicMock.
    """
    target = 'graphrag_toolkit.lexical_graph.utils.llm_cache.GraphRAGConfig'
    with patch(target) as mock_config:
        mock_config.session = mock_session
        mock_config.extraction_num_threads_per_worker = CONFIGURED_NUM_THREADS
        yield mock_config

def unpickled_llm(region_name='us-west-2'):
    """A BedrockConverse through the round-trip ProcessPoolExecutor puts it through."""
    llm = BedrockConverse(model='us.anthropic.claude-sonnet-4-6', region_name=region_name)
    return pickle.loads(pickle.dumps(llm))

def trigger_client_creation(cache, method='predict'):
    """Call through `cache` far enough to build the client, ignoring the failure after."""
    mock_prompt = MagicMock()
    mock_prompt.format.return_value = 'formatted'
    try:
        getattr(cache, method)(mock_prompt)
    except Exception:
        pass  # Expected - no real Bedrock endpoint

def client_kwargs(mock_session):
    """The kwargs the one client build was made with."""
    mock_session.client.assert_called_once()
    return mock_session.client.call_args[1]

def assert_client_region(mock_session, expected_region):
    """Assert the client was built exactly once, in the LLM's own region."""
    call_args = mock_session.client.call_args
    assert client_kwargs(mock_session).get('region_name') == expected_region, \
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


class TestConnectionPoolSizing:
    """The client pool must hold twice the concurrent calls the cache is sized for.

    botocore defaults to 10 connections. Past the pool it discards and reopens
    them, which gives back the concurrency the LLM call pool buys.
    """

    @patch('boto3.Session')
    def test_pool_is_sized_from_the_caches_num_threads(self, mock_boto_session):
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=64)

        mock_session = MagicMock()
        with patched_config(mock_session):
            trigger_client_creation(cache)

        assert client_kwargs(mock_session)['config'].max_pool_connections == 128

    @patch('boto3.Session')
    def test_pool_falls_back_to_the_configured_thread_count(self, mock_boto_session):
        """No num_threads and nothing has used the LLM call pool yet."""
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False)

        mock_session = MagicMock()
        with patched_config(mock_session):
            trigger_client_creation(cache)

        assert client_kwargs(mock_session)['config'].max_pool_connections == \
            CONFIGURED_NUM_THREADS * 2

    @patch('boto3.Session')
    def test_pool_treats_an_explicit_zero_as_a_value_not_as_unset(self, mock_boto_session):
        """Zero asks for nothing, so the floors decide. Reading it as unset would
        fall through and size the client for the configured count instead."""
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=0)

        mock_session = MagicMock()
        with patched_config(mock_session):
            trigger_client_creation(cache)

        assert client_kwargs(mock_session)['config'].max_pool_connections != \
            CONFIGURED_NUM_THREADS * 2

    @patch('boto3.Session')
    def test_pool_never_drops_below_either_floor(self, mock_boto_session):
        """MIN_POOL_SIZE tracks cpu_count, so which of the two floors binds
        depends on the host. Assert the property, not the arithmetic."""
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=1)

        mock_session = MagicMock()
        with patched_config(mock_session):
            trigger_client_creation(cache)

        pool = client_kwargs(mock_session)['config'].max_pool_connections

        assert pool >= BOTOCORE_DEFAULT_MAX_POOL_CONNECTIONS
        assert pool >= MIN_POOL_SIZE

    @patch('boto3.Session')
    def test_pool_follows_the_executor_floor_when_it_is_the_largest(self, mock_boto_session):
        """Why that floor is there: a caller reaching this before anything has
        created the LLM call pool still has to get a client wide enough for it."""
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=1)

        mock_session = MagicMock()
        # Clear of the other two terms, so the executor floor is the one left.
        floor = BOTOCORE_DEFAULT_MAX_POOL_CONNECTIONS + 8
        target = 'graphrag_toolkit.lexical_graph.utils.llm_cache.MIN_POOL_SIZE'
        with patched_config(mock_session), patch(target, floor):
            trigger_client_creation(cache)

        assert client_kwargs(mock_session)['config'].max_pool_connections == floor

    @patch('boto3.Session')
    def test_pool_is_capped_when_the_caller_asks_for_more_than_the_pool_can_run(self, mock_boto_session):
        """The executor clamps at MAX_POOL_SIZE, so a larger request would size
        the client for concurrency the pool will never reach."""
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=MAX_POOL_SIZE * 400)

        mock_session = MagicMock()
        with patched_config(mock_session):
            trigger_client_creation(cache)

        assert client_kwargs(mock_session)['config'].max_pool_connections == MAX_POOL_SIZE * 2

    def test_a_negative_thread_count_is_rejected(self):
        """Negative counts are absorbed by the floors today, so they are accepted
        and silently mean nothing."""
        with pytest.raises(ValidationError):
            LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=-50)

    @patch('boto3.Session')
    def test_pool_is_sized_from_the_llm_call_pool_when_nothing_else_is_set(self, mock_boto_session):
        """
        Extraction runs in a spawned process where a thread count set on
        GraphRAGConfig in the parent is gone, so the LLM call pool's own size is
        what the client has to follow.
        """
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False)

        mock_session = MagicMock()
        target = 'graphrag_toolkit.lexical_graph.utils.llm_cache.pool_size'
        with patched_config(mock_session), patch(target, return_value=48):
            trigger_client_creation(cache)

        assert client_kwargs(mock_session)['config'].max_pool_connections == 96


class TestConcurrentClientCreation:
    """The lazy `_client` build is racy: every extraction worker unpickles an LLM
    without one and the LLM call pool releases num_threads calls into it at once."""

    @patch('boto3.Session')
    def test_concurrent_predicts_build_one_client(self, mock_boto_session):
        cache = LLMCache(llm=unpickled_llm(), enable_cache=False, num_threads=32)

        entrants = 32
        barrier = threading.Barrier(entrants)
        mock_session = MagicMock()

        with patched_config(mock_session):
            def race():
                barrier.wait(timeout=10.0)
                trigger_client_creation(cache)

            threads = [threading.Thread(target=race) for _ in range(entrants)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30.0)

        assert mock_session.client.call_count == 1, \
            f'{entrants} concurrent callers built {mock_session.client.call_count} clients'
