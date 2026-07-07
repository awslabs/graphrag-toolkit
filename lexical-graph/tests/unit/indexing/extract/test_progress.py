# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the non-TTY extraction progress helper (issue #129).

These tests pin the behaviour of ``run_jobs_with_progress``:

* In a non-TTY environment (e.g. ECS Fargate -> CloudWatch, Docker, CI) the
  helper must emit periodic newline-delimited INFO progress records and must
  call the underlying ``run_jobs`` with ``show_progress=False`` (the tqdm
  carriage-return bar is useless to line-based log collectors).
* In an interactive TTY the helper must preserve the prior behaviour: it
  delegates to ``run_jobs`` with the caller-supplied ``show_progress`` and does
  not emit the periodic INFO progress spam.
"""

import asyncio
import logging

import pytest

from graphrag_toolkit.lexical_graph.indexing.extract import progress as progress_module
from graphrag_toolkit.lexical_graph.indexing.extract.progress import (
    run_jobs_with_progress,
)


async def _make_jobs(n):
    """Build ``n`` trivial coroutines that each return their index."""

    async def _job(i):
        return i

    return [_job(i) for i in range(n)]


class TestRunJobsWithProgressNonTty:
    """Non-TTY behaviour: periodic INFO logging replaces the tqdm bar."""

    @pytest.mark.asyncio
    async def test_forces_show_progress_false_when_not_a_tty(self, monkeypatch):
        """When stderr is not a TTY, run_jobs is called with show_progress=False."""
        monkeypatch.setattr(progress_module.sys.stderr, "isatty", lambda: False)

        captured = {}

        async def fake_run_jobs(jobs, show_progress=False, workers=4, desc=None):
            captured["show_progress"] = show_progress
            captured["workers"] = workers
            captured["desc"] = desc
            return [await j for j in jobs]

        monkeypatch.setattr(progress_module, "run_jobs", fake_run_jobs)

        jobs = await _make_jobs(5)
        results = await run_jobs_with_progress(
            jobs,
            show_progress=True,
            workers=2,
            desc="Extracting topics",
            total=5,
        )

        assert sorted(results) == [0, 1, 2, 3, 4]
        # The live tqdm bar must be suppressed in non-TTY environments.
        assert captured["show_progress"] is False
        # Other parameters are passed through unchanged.
        assert captured["workers"] == 2
        assert captured["desc"] == "Extracting topics"

    @pytest.mark.asyncio
    async def test_emits_periodic_info_progress_when_not_a_tty(
        self, monkeypatch, caplog
    ):
        """When stderr is not a TTY, periodic INFO progress records are emitted."""
        monkeypatch.setattr(progress_module.sys.stderr, "isatty", lambda: False)

        jobs = await _make_jobs(10)

        with caplog.at_level(logging.INFO, logger=progress_module.logger.name):
            await run_jobs_with_progress(
                jobs,
                show_progress=True,
                workers=4,
                desc="Extracting propositions",
                total=10,
                # Log on every job so the test is deterministic.
                min_interval_seconds=0,
                log_every_pct=0,
            )

        info_records = [
            r for r in caplog.records if r.levelno == logging.INFO
        ]
        assert info_records, "expected at least one INFO progress record"
        # Progress messages should be human/log-collector friendly: include the
        # desc, a completed/total count, and a percentage.
        joined = "\n".join(r.getMessage() for r in info_records)
        assert "Extracting propositions" in joined
        assert "10/10" in joined
        assert "100%" in joined
        # A final 100% line must always be emitted so collectors see completion.
        assert any("100%" in r.getMessage() for r in info_records)

    @pytest.mark.asyncio
    async def test_returns_results_in_input_order_when_not_a_tty(self, monkeypatch):
        """Results preserve input order regardless of completion order."""
        monkeypatch.setattr(progress_module.sys.stderr, "isatty", lambda: False)

        async def _job(i, delay):
            await asyncio.sleep(delay)
            return i

        # Job 0 finishes last, job 2 finishes first.
        jobs = [_job(0, 0.03), _job(1, 0.02), _job(2, 0.0)]

        results = await run_jobs_with_progress(
            jobs,
            show_progress=False,
            workers=4,
            desc="Extracting propositions",
            total=3,
            min_interval_seconds=0,
            log_every_pct=0,
        )

        assert results == [0, 1, 2]


class TestRunJobsWithProgressTty:
    """TTY behaviour: prior tqdm behaviour is preserved."""

    @pytest.mark.asyncio
    async def test_delegates_show_progress_unchanged_when_tty(
        self, monkeypatch, caplog
    ):
        """In a TTY, run_jobs gets the caller's show_progress and no INFO spam."""
        monkeypatch.setattr(progress_module.sys.stderr, "isatty", lambda: True)

        captured = {}

        async def fake_run_jobs(jobs, show_progress=False, workers=4, desc=None):
            captured["show_progress"] = show_progress
            return [await j for j in jobs]

        monkeypatch.setattr(progress_module, "run_jobs", fake_run_jobs)

        jobs = await _make_jobs(4)

        with caplog.at_level(logging.INFO, logger=progress_module.logger.name):
            results = await run_jobs_with_progress(
                jobs,
                show_progress=True,
                workers=4,
                desc="Extracting topics",
                total=4,
            )

        assert sorted(results) == [0, 1, 2, 3]
        # TTY path must preserve the live tqdm bar (caller's show_progress).
        assert captured["show_progress"] is True
        # No periodic INFO progress spam in interactive TTYs.
        progress_records = [
            r
            for r in caplog.records
            if r.levelno == logging.INFO and "%" in r.getMessage()
        ]
        assert progress_records == []

    @pytest.mark.asyncio
    async def test_respects_show_progress_false_in_tty(self, monkeypatch):
        """If the caller disables progress in a TTY, run_jobs sees show_progress=False."""
        monkeypatch.setattr(progress_module.sys.stderr, "isatty", lambda: True)

        captured = {}

        async def fake_run_jobs(jobs, show_progress=False, workers=4, desc=None):
            captured["show_progress"] = show_progress
            return [await j for j in jobs]

        monkeypatch.setattr(progress_module, "run_jobs", fake_run_jobs)

        jobs = await _make_jobs(2)
        await run_jobs_with_progress(
            jobs,
            show_progress=False,
            workers=4,
            desc="Extracting topics",
            total=2,
        )

        assert captured["show_progress"] is False
