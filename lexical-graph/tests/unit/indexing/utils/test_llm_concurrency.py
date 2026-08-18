# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import threading

import pytest

from graphrag_toolkit.lexical_graph.indexing.utils import llm_concurrency
from graphrag_toolkit.lexical_graph.indexing.utils.llm_concurrency import (
    MIN_POOL_SIZE,
    run_blocking,
)

# Enough over the floor that a pool left at the floor cannot satisfy it, but
# small enough to keep the thread count sane on CI.
OVER_FLOOR = MIN_POOL_SIZE + 8

# A pool that never grows would leave the barrier waiting for the whole timeout,
# so keep it short enough that a failure reports quickly.
BARRIER_TIMEOUT = 10.0


@pytest.fixture(autouse=True)
def fresh_pool():
    """
    The pool is module state, so a test that grows it would otherwise decide what
    the next test sees.
    """
    llm_concurrency._executor = None
    llm_concurrency._executor_size = 0
    yield
    if llm_concurrency._executor is not None:
        llm_concurrency._executor.shutdown(wait=False)
    llm_concurrency._executor = None
    llm_concurrency._executor_size = 0


async def _gather_blocking(count, num_threads, fn):
    return await asyncio.gather(
        *[run_blocking(fn, num_threads) for _ in range(count)]
    )


class TestPoolSizing:

    def test_pool_is_never_smaller_than_cpython_default(self):
        executor = llm_concurrency._executor_for(1)

        assert executor._max_workers == MIN_POOL_SIZE

    def test_pool_grows_to_the_requested_size(self):
        executor = llm_concurrency._executor_for(OVER_FLOOR)

        assert executor._max_workers == OVER_FLOOR

    def test_pool_is_reused_when_it_is_already_big_enough(self):
        grown = llm_concurrency._executor_for(OVER_FLOOR)
        again = llm_concurrency._executor_for(4)

        assert again is grown
        assert again._max_workers == OVER_FLOOR

    def test_growing_replaces_the_pool(self):
        small = llm_concurrency._executor_for(MIN_POOL_SIZE)
        grown = llm_concurrency._executor_for(OVER_FLOOR)

        assert grown is not small
        assert grown._max_workers == OVER_FLOOR


class TestRunBlocking:

    async def test_calls_run_concurrently_past_the_default_cap(self):
        """
        The point of the module: `asyncio.to_thread` caps in-flight blocking calls
        at MIN_POOL_SIZE, and this barrier only releases if more than that many
        calls are running at once.
        """
        barrier = threading.Barrier(OVER_FLOOR)

        def wait_for_the_others():
            barrier.wait(timeout=BARRIER_TIMEOUT)
            return 'done'

        results = await _gather_blocking(
            OVER_FLOOR, OVER_FLOOR, wait_for_the_others
        )

        assert results == ['done'] * OVER_FLOOR

    async def test_returns_the_callable_result(self):
        assert await run_blocking(lambda: 42, 4) == 42

    async def test_propagates_the_callable_exception(self):
        def boom():
            raise ValueError('from the pool')

        with pytest.raises(ValueError, match='from the pool'):
            await run_blocking(boom, 4)

    async def test_runs_off_the_event_loop_thread(self):
        calling_thread = threading.current_thread().name

        ran_on = await run_blocking(lambda: threading.current_thread().name, 4)

        assert ran_on != calling_thread
        assert ran_on.startswith('graphrag-llm')
