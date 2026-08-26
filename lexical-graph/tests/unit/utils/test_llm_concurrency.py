# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import contextvars
import threading

import pytest

from graphrag_toolkit.lexical_graph.utils import llm_concurrency
from graphrag_toolkit.lexical_graph.utils.llm_concurrency import (
    MAX_POOL_SIZE,
    MIN_POOL_SIZE,
    pool_size,
    run_blocking,
    shutdown,
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
    shutdown()
    yield
    shutdown()


def _grow_pool(num_threads):
    """Drive the pool to `num_threads` and hand back the executor it created."""
    llm_concurrency._submit(lambda: None, num_threads).result()
    return llm_concurrency._executor


async def _gather_blocking(count, num_threads, fn):
    return await asyncio.gather(
        *[run_blocking(fn, num_threads) for _ in range(count)]
    )


class TestPoolSizing:

    def test_pool_is_never_smaller_than_cpython_default(self):
        assert _grow_pool(1)._max_workers == MIN_POOL_SIZE

    def test_pool_grows_to_the_requested_size(self):
        assert _grow_pool(OVER_FLOOR)._max_workers == OVER_FLOOR

    def test_pool_is_reused_when_it_is_already_big_enough(self):
        grown = _grow_pool(OVER_FLOOR)
        again = _grow_pool(4)

        assert again is grown
        assert again._max_workers == OVER_FLOOR

    def test_growing_replaces_the_pool(self):
        small = _grow_pool(MIN_POOL_SIZE)
        grown = _grow_pool(OVER_FLOOR)

        assert grown is not small
        assert grown._max_workers == OVER_FLOOR

    def test_a_request_above_the_maximum_is_capped(self):
        assert llm_concurrency._pool_size_for(MAX_POOL_SIZE * 4) == MAX_POOL_SIZE

    def test_pool_size_reports_the_current_size(self):
        assert pool_size() == 0

        _grow_pool(OVER_FLOOR)

        assert pool_size() == OVER_FLOOR

    def test_shutdown_drops_the_pool(self):
        _grow_pool(OVER_FLOOR)

        shutdown()

        assert pool_size() == 0
        assert llm_concurrency._executor is None


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

    async def test_the_call_sees_the_callers_context(self):
        var = contextvars.ContextVar('llm_concurrency_test', default='unset')
        var.set('from the caller')

        assert await run_blocking(var.get, 4) == 'from the caller'

    async def test_a_call_does_not_leak_its_context_to_the_next_one(self):
        """
        The pool reuses threads, so a ContextVar a call sets would be visible to
        every later call on that thread if the context were not copied per call.
        """
        var = contextvars.ContextVar('llm_concurrency_leak_test', default='unset')

        await run_blocking(lambda: var.set('from an earlier call'), 1)

        assert await run_blocking(var.get, 1) == 'unset'

    def test_a_grow_cannot_shut_the_pool_under_an_in_flight_submit(self):
        """
        A submit outside the lock that swaps the pool can land on a pool another
        caller has already replaced and shut down, which raises `RuntimeError:
        cannot schedule new futures after shutdown`.

        Hold one caller inside `pool.submit`, then have a second caller ask for a
        bigger pool. While the first is submitting the second must still be
        waiting for the lock, and the pool it is submitting to must still be open.
        """
        pool = _grow_pool(4)
        submitting = threading.Event()
        release = threading.Event()
        pool_submit = pool.submit

        def slow_submit(fn):
            submitting.set()
            release.wait(timeout=BARRIER_TIMEOUT)
            return pool_submit(fn)

        pool.submit = slow_submit

        holder = threading.Thread(
            target=lambda: llm_concurrency._submit(lambda: 'ok', 4).result()
        )
        holder.start()
        assert submitting.wait(timeout=BARRIER_TIMEOUT)

        grower = threading.Thread(
            target=lambda: llm_concurrency._submit(lambda: 'ok', OVER_FLOOR).result()
        )
        grower.start()
        grower.join(timeout=0.2)

        try:
            assert grower.is_alive(), 'the grow ran while a submit was in flight'
            assert not pool._shutdown, 'the pool was shut down under an in-flight submit'
        finally:
            release.set()
            holder.join(timeout=BARRIER_TIMEOUT)
            grower.join(timeout=BARRIER_TIMEOUT)
