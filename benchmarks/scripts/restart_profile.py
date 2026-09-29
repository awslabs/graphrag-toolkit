# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Driving an extraction that is interrupted and restarted, for benchmarking.

A restart is simulated by stopping the run part way and running it again under
the same run id, which is what a dying process leaves behind. The alternative,
deleting staged objects after a complete run, produces a collection no
interruption would produce.
"""

import logging
import os
import time

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from benchmarks.utils.doc_limit import extraction_doc_limit

logger = logging.getLogger(__name__)

EVERY_DOCUMENT = 'every-document'


class InterruptedRun(Exception):
    """Raised to stop an extraction where a dying process would stop it."""


class InterruptAfter:
    """
    A handler that stages through the handler it wraps, counting the documents
    it stores, and stops the run once it has stored a given number. An ``after``
    of None counts without ever stopping.

    ``extract`` consumes a handler as ``Pipe(handler.accept)`` and asks nothing
    else of it, so wrapping that one method is enough. Raising from inside the
    generator leaves the pipeline part way through, which is the state a
    restart has to read.
    """

    def __init__(self, inner, after:Optional[int]):
        self.inner = inner
        self.after = after
        # Source ids rather than a running total. A source split across
        # extraction rounds reaches staging as several documents, and one with
        # nothing written comes back with a source id of None, so counting the
        # documents that arrive counts neither sources nor stores.
        self.stored_source_ids = set()
        # The handler yields the documents it skipped alongside the ones it
        # stored, so counting everything that comes back counts a restart's
        # skips as progress and stops it before it has stored anything. This is
        # the set the handler itself skips on: S3BasedDocs ignores the skip set
        # when writing JSONL.
        self.skipping = (
            set() if getattr(inner, 'for_jsonl', False)
            else set(getattr(inner, 'skip_source_ids', None) or ())
        )

    @property
    def stored(self) -> int:
        """How many source documents this run stored."""
        return len(self.stored_source_ids)

    def accept(self, source_documents, **kwargs):
        for doc in self.inner.accept(source_documents, **kwargs):
            source_id = doc.source_id()
            if source_id is not None and source_id not in self.skipping:
                self.stored_source_ids.add(source_id)
            yield doc
            if self.after is not None and self.stored >= self.after:
                raise InterruptedRun(f'stopping after storing {self.stored} documents')


@dataclass
class RestartReport:
    """What a profiled run did, for the numbers the benchmark publishes."""

    profile:str
    documents:int
    restarts_planned:int
    doc_limit:Optional[int] = None
    runs:int = 0
    interruptions:int = 0
    first_run_seconds:float = 0.0
    restart_seconds:float = 0.0
    documents_per_run:List[int] = field(default_factory=list)

    @property
    def total_seconds(self) -> float:
        return self.first_run_seconds + self.restart_seconds

    def as_output(self) -> dict:
        return {
            'restart_profile': self.profile,
            'documents': self.documents,
            # The cap, so two profiles are not compared across corpus sizes:
            # 'documents' alone does not say whether it is the whole corpus or
            # a slice of it.
            'extract_doc_limit': self.doc_limit,
            # What this run will actually attempt, after the cap below. The
            # number asked for is in BENCHMARK_RESTARTS, and a reduction is
            # logged where it is made.
            'restarts_planned': self.restarts_planned,
            'runs': self.runs,
            'interruptions': self.interruptions,
            'first_run_seconds': round(self.first_run_seconds, 1),
            'restart_seconds': round(self.restart_seconds, 1),
            'total_seconds': round(self.total_seconds, 1),
            'documents_staged_per_run': self.documents_per_run,
        }


def restarts_from_env(document_count:int) -> int:
    """
    How many times to interrupt this run.

    BENCHMARK_RESTARTS takes a count, or 'every-document' for the worst case.
    Unset is a run with no interruption, which is the baseline the other
    profiles are compared against.
    """
    raw = os.environ.get('BENCHMARK_RESTARTS', '').strip()

    if not raw:
        return 0

    if raw.lower() == EVERY_DOCUMENT:
        return max(0, document_count - 1)

    try:
        restarts = int(raw)
    except ValueError:
        raise ValueError(
            f"BENCHMARK_RESTARTS must be an integer or '{EVERY_DOCUMENT}', but got '{raw}'"
        )

    if restarts < 0:
        raise ValueError(f'BENCHMARK_RESTARTS cannot be negative, but got {restarts}')

    reachable = max(0, document_count - 1)
    if restarts > reachable:
        logger.info(
            f'[restart benchmark] BENCHMARK_RESTARTS={restarts} asks for more stops than '
            f'there are documents to reach; using {reachable}'
        )

    return min(restarts, reachable)


def profile_name(restarts:int, document_count:int) -> str:
    """What to call this profile in the results, so runs can be compared by name."""
    if restarts == 0:
        return 'baseline'
    if restarts >= document_count - 1:
        return EVERY_DOCUMENT
    return f'{restarts}-restart' if restarts == 1 else f'{restarts}-restarts'


def run_with_restarts(extract, new_handler:Callable[[], Any], documents:List[Any],
                      restarts:int) -> RestartReport:
    """
    Extract the documents, interrupting the given number of times.

    Every run is handed the whole document set. A run plan records the
    documents it divided, and a run that offered a different set would be
    refused as a new run rather than a restart of this one. What a restart
    skips is decided by what the collection already holds, not by what it is
    asked to extract.

    ``new_handler`` is called once per run, and must be a factory rather than a
    handler: a staging handler resolves what to skip when it is built, so one
    instance reused across runs would keep skipping the set the first run
    started with and restage everything the runs after it stored.

    ``extract`` is called as extract(documents, handler). The caller supplies
    it already carrying the run id and plan store, so this knows nothing about
    either.
    """
    if restarts > max(0, len(documents) - 1):
        raise ValueError(
            f'{restarts} interruptions cannot be reached in {len(documents)} documents; '
            f'restarts_from_env caps this, and a direct caller has to do the same'
        )

    report = RestartReport(
        profile=profile_name(restarts, len(documents)),
        documents=len(documents),
        restarts_planned=restarts,
        doc_limit=extraction_doc_limit(),
    )

    # Each interrupted run stops once it has staged this many documents. A
    # restart stages only what is missing, so the stops fall at roughly even
    # intervals through the work remaining, not through the collection.
    stride = max(1, len(documents) // (restarts + 1)) if restarts else 0
    remaining = restarts

    try:
        while True:
            started = time.monotonic()
            # Every run is wrapped, including the last one, so each reports the
            # documents it stored rather than having its share inferred from
            # the runs before it.
            wrapped = InterruptAfter(new_handler(), stride if remaining else None)

            try:
                extract(documents, wrapped)
                interrupted = False
            except InterruptedRun as stopped:
                interrupted = True
                logger.info(f'[restart benchmark] run {report.runs + 1} interrupted: {stopped}')

            elapsed = time.monotonic() - started
            report.runs += 1
            report.documents_per_run.append(wrapped.stored)

            if report.runs == 1:
                report.first_run_seconds = elapsed
            else:
                report.restart_seconds += elapsed

            if not interrupted:
                break

            report.interruptions += 1
            remaining -= 1
    finally:
        # Logged even when a run fails outright, since the runs that already
        # finished are the only record of how far the profile got.
        logger.info(f'[restart benchmark] {report.as_output()}')

    return report
