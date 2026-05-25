# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/processors/zero_scores."""

import pytest
from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import (
    EntityContexts,
    SearchResult,
    SearchResultCollection,
    Source,
    Statement,
    Topic,
    Versioning,
)
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.processors.zero_scores import ZeroScores


@pytest.fixture
def processor():
    return ZeroScores(ProcessorArgs(debug_results=[]), FilterConfig())


def _collection():
    versioning = Versioning(valid_from=0, valid_to=9999999999)
    source = Source(sourceId='doc1', metadata={}, versioning=versioning)
    topic = Topic(
        topic='T',
        topicId='t1',
        statements=[
            Statement(statement='s1', score=0.9),
            Statement(statement='s2', score=0.5),
        ],
    )
    result = SearchResult(source=source, topics=[topic], score=0.95)
    return SearchResultCollection(
        results=[result],
        entity_contexts=EntityContexts(contexts=[], keywords=[]),
    )


class TestZeroScores:
    def test_zeroes_search_result_and_statement_scores(self, processor):
        processed = processor._process_results(_collection(), QueryBundle('q'))
        result = processed.results[0]
        assert result.score == 0.0
        assert all(s.score == 0.0 for s in result.topics[0].statements)
