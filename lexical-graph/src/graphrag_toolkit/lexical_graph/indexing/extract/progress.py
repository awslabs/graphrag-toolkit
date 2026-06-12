# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Progress reporting for asynchronous extraction jobs.

LlamaIndex's ``run_jobs(show_progress=True, ...)`` renders a ``tqdm`` progress
bar to stderr using carriage returns (``\\r``). In interactive terminals this is
a nice live bar, but in non-TTY environments (ECS Fargate -> CloudWatch, Docker,
CI) line-based log collectors drop or collapse the carriage-return updates, so
extraction has near-zero progress visibility (issue #129).

``run_jobs_with_progress`` is a drop-in wrapper around ``run_jobs`` that:

* In an interactive TTY, delegates to ``run_jobs`` unchanged, preserving the
  live ``tqdm`` bar.
* In a non-TTY environment, suppresses the ``tqdm`` bar (``show_progress=False``)
  and instead emits periodic newline-delimited ``INFO`` log records as jobs
  complete, on a percent/time cadence. These survive line-based log collection.

No new dependencies are introduced; only the standard library and the existing
``run_jobs`` import are used.
"""

import logging
import sys
import time
from typing import Any, Coroutine, List, Optional, TypeVar

from llama_index.core.async_utils import run_jobs

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Defaults chosen to be quiet enough for large runs but responsive enough to
# show liveness: log at most every ~10% of progress and no more often than
# every 5 seconds. The final 100% line is always emitted.
DEFAULT_LOG_EVERY_PCT = 10.0
DEFAULT_MIN_INTERVAL_SECONDS = 5.0


def _stderr_is_tty() -> bool:
    """Return whether stderr is an interactive terminal.

    Defensive against environments where ``sys.stderr`` is replaced with an
    object lacking ``isatty`` (treated as non-TTY).
    """
    isatty = getattr(sys.stderr, "isatty", None)
    if not callable(isatty):
        return False
    try:
        return bool(isatty())
    except Exception:  # pragma: no cover - extremely defensive
        return False


async def run_jobs_with_progress(
    jobs: List[Coroutine[Any, Any, T]],
    *,
    show_progress: bool = False,
    workers: int = 4,
    desc: Optional[str] = None,
    total: Optional[int] = None,
    logger: logging.Logger = logger,
    log_every_pct: float = DEFAULT_LOG_EVERY_PCT,
    min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
) -> List[T]:
    """Run ``jobs`` concurrently with progress visibility in any environment.

    In an interactive TTY this delegates to :func:`run_jobs` with the supplied
    ``show_progress`` (preserving the live ``tqdm`` bar). In a non-TTY
    environment the ``tqdm`` bar is suppressed and periodic newline-delimited
    ``INFO`` progress records are emitted instead.

    Args:
        jobs: Coroutines to run, mirroring :func:`run_jobs`.
        show_progress: Whether the caller requested a progress bar. Honoured
            verbatim in a TTY; forced to ``False`` for ``run_jobs`` in a
            non-TTY (periodic INFO logging replaces the bar).
        workers: Maximum concurrent jobs, passed through to :func:`run_jobs`.
        desc: Human-readable description, used as the log-line prefix and passed
            through to :func:`run_jobs` for the ``tqdm`` bar.
        total: Total number of jobs (defaults to ``len(jobs)``); used to compute
            percentages.
        logger: Logger used for periodic INFO progress records.
        log_every_pct: Emit a progress record whenever completion advances at
            least this many percentage points since the last record. ``0`` logs
            on every completed job.
        min_interval_seconds: Suppress progress records emitted within this many
            seconds of the previous one (the final 100% line is always emitted).

    Returns:
        Results in the same order as ``jobs`` (matching :func:`run_jobs`).
    """
    if total is None:
        total = len(jobs)

    # Interactive terminal: keep the prior behaviour (live tqdm bar) untouched.
    if _stderr_is_tty():
        return await run_jobs(
            jobs,
            show_progress=show_progress,
            workers=workers,
            desc=desc,
        )

    # Non-TTY: suppress the carriage-return tqdm bar and emit newline-delimited
    # INFO progress on a percent/time cadence instead. We still delegate the
    # actual concurrency to run_jobs (with show_progress=False) so its worker
    # semantics and result ordering are preserved; each job is wrapped to record
    # its completion and emit progress.
    prefix = desc if desc else "Progress"

    if total <= 0:
        # Nothing to do; still delegate so the empty-input contract matches.
        return await run_jobs(jobs, show_progress=False, workers=workers, desc=desc)

    completed = 0
    last_logged_pct = -1.0
    last_log_time = 0.0

    def _emit(force: bool = False) -> None:
        nonlocal last_logged_pct, last_log_time
        pct = (completed / total) * 100.0
        now = time.monotonic()
        if not force:
            if (pct - last_logged_pct) < log_every_pct:
                return
            if (now - last_log_time) < min_interval_seconds:
                return
        logger.info("%s: %d/%d (%.0f%%)", prefix, completed, total, pct)
        last_logged_pct = pct
        last_log_time = now

    async def _tracked(job: Coroutine[Any, Any, T]) -> T:
        nonlocal completed
        result = await job
        completed += 1
        # Always emit the final completion line; otherwise honour the cadence.
        _emit(force=(completed == total))
        return result

    wrapped_jobs = [_tracked(job) for job in jobs]
    return await run_jobs(
        wrapped_jobs,
        show_progress=False,
        workers=workers,
        desc=desc,
    )
