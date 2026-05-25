# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/processors/rerank_statements."""

from unittest.mock import MagicMock, patch

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
from graphrag_toolkit.lexical_graph.retrieval.processors import rerank_statements as mod
from graphrag_toolkit.lexical_graph.retrieval.processors.rerank_statements import (
    RerankStatements,
    default_reranking_source_metadata_fn,
)


def _collection_with_statements():
    versioning = Versioning(valid_from=0, valid_to=9999999999)
    source = Source(sourceId='doc1', metadata={'author': 'alice'}, versioning=versioning)
    statements = [
        Statement(statement='s1', statement_str='one fact', score=0.0),
        Statement(statement='s2', statement_str='two fact', score=0.0),
    ]
    topic = Topic(topic='T', topicId='t1', statements=statements)
    return SearchResultCollection(
        results=[SearchResult(source=source, topics=[topic])],
        entity_contexts=EntityContexts(contexts=[], keywords=[]),
    )


class TestDefaultRerankingSourceMetadataFn:
    def test_numeric_value_stringified(self):
        source = Source(sourceId='s', metadata={'year': 2024}, versioning=Versioning(valid_from=0, valid_to=9))
        assert default_reranking_source_metadata_fn(source) == '2024'

    def test_date_value_reformatted(self):
        source = Source(
            sourceId='s', metadata={'date': '2024-03-15'},
            versioning=Versioning(valid_from=0, valid_to=9),
        )
        result = default_reranking_source_metadata_fn(source)
        assert 'March' in result and '2024' in result

    def test_url_value_dropped(self):
        source = Source(
            sourceId='s', metadata={'url': 'https://example.com/foo'},
            versioning=Versioning(valid_from=0, valid_to=9),
        )
        assert default_reranking_source_metadata_fn(source) == ''

    def test_plain_text_passthrough(self):
        source = Source(
            sourceId='s', metadata={'title': 'Hello World'},
            versioning=Versioning(valid_from=0, valid_to=9),
        )
        assert default_reranking_source_metadata_fn(source) == 'Hello World'

    def test_combines_multiple_values_with_comma(self):
        source = Source(
            sourceId='s',
            metadata={'title': 'Hello', 'author': 'Alice'},
            versioning=Versioning(valid_from=0, valid_to=9),
        )
        result = default_reranking_source_metadata_fn(source)
        assert 'Hello' in result and 'Alice' in result
        assert ', ' in result


class TestProcessResults:
    def test_none_reranker_returns_results_unchanged(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='none', debug_results=[]), FilterConfig(),
        )
        collection = _collection_with_statements()
        result = processor._process_results(collection, QueryBundle('q'))
        assert result is collection

    def test_no_reranker_value_returns_results_unchanged(self):
        processor = RerankStatements(
            ProcessorArgs(reranker=None, debug_results=[]), FilterConfig(),
        )
        collection = _collection_with_statements()
        assert processor._process_results(collection, QueryBundle('q')) is collection

    def test_unknown_reranker_returns_results_unchanged(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='something-weird', debug_results=[]), FilterConfig(),
        )
        collection = _collection_with_statements()
        assert processor._process_results(collection, QueryBundle('q')) is collection

    def test_empty_results_short_circuits(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='tfidf', debug_results=[]), FilterConfig(),
        )
        empty = SearchResultCollection(
            results=[],
            entity_contexts=EntityContexts(contexts=[], keywords=[]),
        )
        result = processor._process_results(empty, QueryBundle('q'))
        assert result is empty

    def test_tfidf_reranker_assigns_scores_and_sorts(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='tfidf', debug_results=[], max_statements=10),
            FilterConfig(),
        )
        collection = _collection_with_statements()

        # Bypass the actual tfidf scoring with deterministic output.
        with patch.object(processor, '_score_values_with_tfidf') as scorer:
            # Build keys the way _format_statement_context does: source, topic, statement.
            def fake(values, *_a, **_kw):
                return {values[0]: 0.2, values[1]: 0.9}
            scorer.side_effect = fake
            result = processor._process_results(collection, QueryBundle('q'))

        statements = result.results[0].topics[0].statements
        assert statements[0].statement_str == 'two fact'
        assert statements[0].score == 0.9
        assert statements[1].score == 0.2

    def test_model_reranker_dispatches_to_score_values(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='model', debug_results=[]), FilterConfig(),
        )
        with patch.object(processor, '_score_values') as scorer:
            scorer.return_value = {}
            processor._process_results(_collection_with_statements(), QueryBundle('q'))
        scorer.assert_called_once()

    def test_bedrock_reranker_dispatches_to_score_values_with_bedrock(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='bedrock', debug_results=[]), FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as scorer:
            scorer.return_value = {}
            processor._process_results(_collection_with_statements(), QueryBundle('q'))
        scorer.assert_called_once()
