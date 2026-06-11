# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Async batch execution and iteration utilities."""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine, Iterable, Iterator, List, Optional, TypeVar

T = TypeVar("T")


def iter_batch(iterable: Iterable[T], batch_size: int) -> Iterator[List[T]]:
    """Yield successive batches from an iterable."""
    items = list(iterable)
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


async def run_jobs(
    jobs: List[Coroutine[Any, Any, T]],
    show_progress: bool = False,
    workers: int = 4,
    desc: Optional[str] = None,
) -> List[T]:
    """Run async jobs with a concurrency semaphore."""
    semaphore = asyncio.Semaphore(workers)

    async def worker(job: Coroutine) -> Any:
        async with semaphore:
            return await job

    pool_jobs = [worker(job) for job in jobs]

    if show_progress:
        try:
            from tqdm.asyncio import tqdm_asyncio
            results = await tqdm_asyncio.gather(*pool_jobs, desc=desc)
        except ImportError:
            results = await asyncio.gather(*pool_jobs)
    else:
        results = await asyncio.gather(*pool_jobs)

    return results
