# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/processors/truncate_statements."""

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
from graphrag_toolkit.lexical_graph.retrieval.processors.truncate_statements import (
    TruncateStatements,
)


def _collection_with_statements(n):
    versioning = Versioning(valid_from=0, valid_to=9999999999)
    source = Source(sourceId='doc1', metadata={}, versioning=versioning)
    statements = [Statement(statement=f's{i}', score=1.0 - i * 0.1) for i in range(n)]
    topic = Topic(topic='T', topicId='t1', statements=statements)
    result = SearchResult(source=source, topics=[topic])
    return SearchResultCollection(
        results=[result],
        entity_contexts=EntityContexts(contexts=[], keywords=[]),
    )


class TestTruncateStatements:
    def test_truncates_to_max_statements_per_topic(self):
        processor = TruncateStatements(
            ProcessorArgs(debug_results=[], max_statements_per_topic=2),
            FilterConfig(),
        )
        processed = processor._process_results(
            _collection_with_statements(5), QueryBundle('q')
        )
        topic = processed.results[0].topics[0]
        assert len(topic.statements) == 2
        assert [s.statement for s in topic.statements] == ['s0', 's1']

    def test_leaves_topic_unchanged_when_under_limit(self):
        processor = TruncateStatements(
            ProcessorArgs(debug_results=[], max_statements_per_topic=10),
            FilterConfig(),
        )
        processed = processor._process_results(
            _collection_with_statements(3), QueryBundle('q')
        )
        assert len(processed.results[0].topics[0].statements) == 3
