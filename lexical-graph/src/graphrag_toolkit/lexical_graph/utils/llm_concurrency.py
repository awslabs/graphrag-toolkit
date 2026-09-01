# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Thread pool for the blocking LLM calls extraction makes.

`asyncio.to_thread` runs them on the event loop's default executor, which CPython
caps at `min(32, cpu_count + 4)`. That cap, not the configured thread count,
decides how many calls are in flight. This module owns a pool sized to the
request instead.

Callers pass their own worker count. Extraction runs in a spawned process, where
a count set on GraphRAGConfig is absent and reads back as the default, while the
extractor's `num_workers` is pickled with the component and survives.
"""

import asyncio
import concurrent.futures
import contextvars
import functools
import logging
import os
import threading

logger = logging.getLogger(__name__)

# CPython's size for the asyncio default executor, kept as a floor so this pool
# is never smaller than the one it stands in for.
MIN_POOL_SIZE = min(32, (os.cpu_count() or 1) + 4)

# Every process spawned by `run_pipeline` gets its own pool and every thread
# holds a bedrock-runtime connection. This bounds one process, so the fleet
# ceiling is workers x MAX_POOL_SIZE. Requests above this are clamped.
MAX_POOL_SIZE = 256

_lock = threading.Lock()
_executor = None
_executor_size = 0
_warned_above_max = False
_warned_below_request = False


def _pool_size_for(num_threads: int) -> int:
    """The requested count, floored at MIN_POOL_SIZE and capped at MAX_POOL_SIZE."""
    return min(max(num_threads, MIN_POOL_SIZE), MAX_POOL_SIZE)


def pool_size() -> int:
    """
    Workers in the current pool, or 0 before any caller has asked for one.

    In a spawned worker this is the only value reflecting the concurrency the
    caller asked for, so `llm_cache` sizes its connection pool from it.
    """
    return _executor_size


def shutdown() -> None:
    """
    Drop the pool and stop its threads. Calls already running finish.

    Pipeline workers are torn down per batch, so the pool goes with them. A
    caller driving the extractors in-process owns the lifetime and calls this.
    """
    global _executor, _executor_size, _warned_above_max, _warned_below_request

    with _lock:
        previous, _executor, _executor_size = _executor, None, 0
        _warned_above_max = False
        _warned_below_request = False

    if previous is not None:
        previous.shutdown(wait=False)


def _submit(fn, num_threads: int) -> concurrent.futures.Future:
    """
    Submit `fn` to the pool, sized from `num_threads` on the first call.

    The size is fixed once: replacing the pool to grow it leaves the old one's
    threads running until their work drains, so the maximum would bound one pool
    rather than the process. The submit runs under the lock that creates the
    pool, so a concurrent `shutdown` cannot clear the executor mid-submit.
    """
    global _executor, _executor_size, _warned_above_max, _warned_below_request

    wanted = _pool_size_for(num_threads)

    with _lock:
        # Once per process: a request above the maximum arrives once per node.
        if num_threads > MAX_POOL_SIZE and not _warned_above_max:
            _warned_above_max = True
            logger.warning(
                f'Requested LLM call pool size is above the maximum, using the maximum '
                f'[requested: {num_threads}, max: {MAX_POOL_SIZE}]'
            )

        if _executor is None:
            _executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=wanted,
                thread_name_prefix='graphrag-llm',
            )
            _executor_size = wanted
            logger.debug(f'Sized LLM call pool [max_workers: {wanted}]')
        elif wanted > _executor_size and not _warned_below_request:
            _warned_below_request = True
            logger.warning(
                f'LLM call pool is smaller than a later request, using its existing size '
                f'[requested: {wanted}, pool: {_executor_size}]'
            )

        return _executor.submit(fn)


async def run_blocking(fn, num_threads: int):
    """
    Run a blocking callable on the LLM call pool.

    Drop-in for `asyncio.to_thread(fn)`, differing only in which pool it lands
    on. The context is copied per call because the pool reuses threads, so a
    ContextVar left by one call would otherwise be read by the next node.
    llama_index nests its callback events off such a var.
    """
    ctx = contextvars.copy_context()
    future = _submit(functools.partial(ctx.run, fn), num_threads)
    return await asyncio.wrap_future(future)
