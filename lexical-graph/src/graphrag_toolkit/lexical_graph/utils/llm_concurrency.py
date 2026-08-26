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

# CPython's own size for the asyncio default executor. Reproduced here as a
# floor, not a cap, so this pool can never be smaller than the one it replaces.
MIN_POOL_SIZE = min(32, (os.cpu_count() or 1) + 4)

# Ceiling on the pool. Every process spawned by `run_pipeline` gets its own pool
# and every thread in it holds a bedrock-runtime connection, so a thread count
# set far too high multiplies across processes into thousands of threads and
# sockets. Requests above this are clamped.
MAX_POOL_SIZE = 256

_lock = threading.Lock()
_executor = None
_executor_size = 0


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

    The pool is process-global and its threads are not daemons, so they outlive
    the extraction that created them in any host process that keeps running.
    Calls already running finish; no new call is queued.
    """
    global _executor, _executor_size

    with _lock:
        previous, _executor, _executor_size = _executor, None, 0

    if previous is not None:
        previous.shutdown(wait=False)


def _submit(fn, num_threads: int) -> concurrent.futures.Future:
    """
    Submit `fn` to a pool of at least `num_threads` workers, creating or growing it.

    Growing replaces the pool rather than resizing in place, which
    ThreadPoolExecutor does not support. The old pool is shut down without
    waiting, so work already running on it finishes while no new work is queued.

    The submit happens under the same lock that swaps the pool. Submitting
    outside it can land on a pool another thread has already replaced and shut
    down, which raises `RuntimeError: cannot schedule new futures after shutdown`.
    """
    global _executor, _executor_size

    wanted = _pool_size_for(num_threads)

    with _lock:
        if _executor is None or _executor_size < wanted:
            if num_threads > MAX_POOL_SIZE:
                logger.warning(
                    f'Requested LLM call pool size is above the maximum, using the maximum '
                    f'[requested: {num_threads}, max: {MAX_POOL_SIZE}]'
                )
            previous = _executor
            _executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=wanted,
                thread_name_prefix='graphrag-llm',
            )
            _executor_size = wanted
            logger.debug(f'Sized LLM call pool [max_workers: {wanted}]')
            if previous is not None:
                previous.shutdown(wait=False)
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
