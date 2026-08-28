# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Thread pool for the blocking LLM calls extraction makes.

Extraction runs one asyncio job per node and each job awaits a synchronous LLM
call. `asyncio.to_thread` hands that call to the event loop's default executor,
which CPython sizes at `min(32, cpu_count + 4)`. That cap, not
`extraction_num_threads_per_worker`, decides how many calls are in flight: past
it the extra jobs queue for a thread.

This module owns a pool sized to the requested concurrency instead, floored at
CPython's size so a low thread setting cannot make things worse than before, and
capped at MAX_POOL_SIZE.

Callers pass their own worker count rather than letting this module read
GraphRAGConfig. Extraction runs in a process spawned by `run_pipeline`, and spawn
inherits no parent memory, so a thread count set programmatically on
GraphRAGConfig is absent in the worker and reads back as the default. The
extractor's own `num_workers` is pickled with the component and survives.
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
# holds a bedrock-runtime connection, so an unreasonably high count would
# otherwise multiply across processes. Requests above this are clamped.
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

    In a spawned extraction worker this is the only value that reflects the
    concurrency the caller asked for, because it comes from the worker count
    pickled with the extractor. `llm_cache` reads it to size the bedrock-runtime
    connection pool to match.
    """
    return _executor_size


def shutdown() -> None:
    """
    Drop the pool and stop its threads.

    The pool belongs to the process that created it. Pipeline extraction runs in
    workers that `run_pipeline` spawns and tears down per batch, so the pool goes
    with them and nothing there needs to call this. A caller driving the
    extractors in-process owns the lifetime instead, and this is how it releases
    the threads.

    Calls already running finish; no new call is queued.
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

    The size is fixed once. Replacing the pool to grow it would leave the old
    one's threads running until their work drained, so MAX_POOL_SIZE would bound
    a single pool rather than the process.

    The submit runs under the lock that creates the pool, so a concurrent
    `shutdown` cannot clear the executor between the read and the submit.
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

    Drop-in for `asyncio.to_thread(fn)`: the call runs in a copy of the calling
    context, as to_thread does, and differs only in which pool it lands on. The
    context copy matters because the pool reuses threads across calls, so a
    ContextVar left set by one call would otherwise be read by the next node's
    extraction; llama_index nests its callback events off such a var.

    Args:
        fn: the blocking callable.
        num_threads: how many calls the caller intends to have in flight. Pass
            the caller's own worker count; see the module docstring for why
            reading GraphRAGConfig here does not work in a spawned worker.
    """
    ctx = contextvars.copy_context()
    future = _submit(functools.partial(ctx.run, fn), num_threads)
    return await asyncio.wrap_future(future)
