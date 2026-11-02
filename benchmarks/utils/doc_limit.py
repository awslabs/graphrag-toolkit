# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
The cap on how many source documents a benchmark extracts.

BENCHMARK_EXTRACT_DOC_LIMIT already caps the integration tests' batch_extract
run. The same variable caps the benchmark harness here, so a 1,100-document
corpus can be run at 100 or 500 documents without staging a second copy of it
in S3.

The cap also lowers the document counts the extract and build steps assert, so
a capped run reaches the same assertions rather than failing on the full
corpus's count.
"""

import logging
from typing import List, Optional, TypeVar

from benchmarks.utils.benchmark_env import env_int

logger = logging.getLogger(__name__)

DOC_LIMIT_VAR = 'BENCHMARK_EXTRACT_DOC_LIMIT'

T = TypeVar('T')


def extraction_doc_limit() -> Optional[int]:
    """
    The configured cap, or None when the run is uncapped.

    A cap of zero or less is read as no cap, matching the integration tests'
    batch_extract: a run asked for nothing is a misconfiguration, not a request
    to extract an empty corpus.

    Raises:
        ValueError: The variable is set to something that is not an integer.
    """
    limit = env_int(DOC_LIMIT_VAR, None)

    return limit if limit is not None and limit > 0 else None


def apply_extraction_doc_limit(docs: List[T]) -> List[T]:
    """The first N documents when a cap is set, otherwise every document."""
    limit = extraction_doc_limit()

    if limit is None or limit >= len(docs):
        return docs

    logger.info(
        f'{DOC_LIMIT_VAR} is set: extracting the first {limit} of {len(docs)} documents'
    )

    return docs[:limit]


def capped_expected_docs(expected_docs: Optional[int]) -> Optional[int]:
    """
    The document count a capped run should assert against.

    Each dataset states the size of its full corpus. Under a cap the run
    extracts fewer documents than that, so the number asserted is the smaller
    of the two.
    """
    limit = extraction_doc_limit()

    if expected_docs is None or limit is None:
        return expected_docs

    return min(expected_docs, limit)
