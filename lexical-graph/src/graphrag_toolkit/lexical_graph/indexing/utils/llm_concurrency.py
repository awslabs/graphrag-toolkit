# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Thread pool for the blocking LLM calls extraction makes.

Extraction runs one asyncio job per node and each job awaits a synchronous LLM
call. `asyncio.to_thread` hands that call to the event loop's default executor,
which CPython sizes at `min(32, cpu_count + 4)` and which nothing here replaces.
That cap, not `extraction_num_threads_per_worker`, decides how many calls are
actually in flight: past it the extra jobs queue for a thread.

Measured on a 14-core machine, where the default cap is 18: raising the job
semaphore to 32 or 64 left peak in-flight pinned at 18, while running the same
64 jobs against a pool sized to the semaphore reached 64 in-flight and cut wall
time from 4.9s to 2.6s.

This module owns a pool sized to the requested concurrency so the setting means
what its name says. The pool is never smaller than CPython's default, so a low
thread setting cannot make things worse than before.

Callers pass their own worker count rather than letting this module read
GraphRAGConfig. Extraction runs in a process spawned by `run_pipeline`, and spawn
inherits no parent memory, so a thread count set programmatically on
GraphRAGConfig is absent in the worker and reads back as the default. The
extractor's own `num_workers` is pickled with the component and survives, which
makes it the only value here that reflects what was actually asked for.
"""

import asyncio
import concurrent.futures
import logging
import os
import threading

logger = logging.getLogger(__name__)

# CPython's own size for the asyncio default executor. Reproduced here as a
# floor, not a cap, so this pool can never be smaller than the one it replaces.
MIN_POOL_SIZE = min(32, (os.cpu_count() or 1) + 4)

_lock = threading.Lock()
_executor = None
_executor_size = 0


def _pool_size_for(num_threads: int) -> int:
    """
    How many workers a caller asking for `num_threads` gets.

    The requested count, floored at MIN_POOL_SIZE. Worked through on a 16 vCPU
    machine, where the floor is 20: ask for 4 and the pool is 20, ask for 64 and
    the pool is 64. Nothing here caps the request.
    """
    return max(num_threads, MIN_POOL_SIZE)


def _executor_for(num_threads: int) -> concurrent.futures.ThreadPoolExecutor:
    """
    Return a pool with at least `num_threads` workers, creating or growing it.

    Growing replaces the pool rather than resizing in place, which
    ThreadPoolExecutor does not support. The old pool is shut down without
    waiting, so work already running on it finishes while no new work is queued.
    """
    global _executor, _executor_size

    wanted = _pool_size_for(num_threads)

    with _lock:
        if _executor is None or _executor_size < wanted:
            previous = _executor
            _executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=wanted,
                thread_name_prefix='graphrag-llm',
            )
            _executor_size = wanted
            logger.debug(f'Sized LLM call pool [max_workers: {wanted}]')
            if previous is not None:
                previous.shutdown(wait=False)
        return _executor


async def run_blocking(fn, num_threads: int):
    """
    Run a blocking callable on the LLM call pool.

    Drop-in for `asyncio.to_thread(fn)`, differing only in which pool the call
    lands on.

    Args:
        fn: the blocking callable.
        num_threads: how many calls the caller intends to have in flight. Pass
            the caller's own worker count; see the module docstring for why
            reading GraphRAGConfig here does not work in a spawned worker.
    """
    loop = asyncio.get_running_loop()
    executor = _executor_for(num_threads)
    return await loop.run_in_executor(executor, fn)
