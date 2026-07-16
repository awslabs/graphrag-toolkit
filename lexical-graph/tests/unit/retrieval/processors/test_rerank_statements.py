# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/processors/rerank_statements."""

import logging
from unittest.mock import Mock, patch

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
from graphrag_toolkit.lexical_graph.retrieval.processors.reranker_chain import KNOWN_RERANKERS


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

    def test_none_value_stringified_via_typeerror(self):
        # parse(None) raises TypeError, which falls back to str(None).
        source = Source(
            sourceId='s', metadata={'x': None},
            versioning=Versioning(valid_from=0, valid_to=9),
        )
        assert default_reranking_source_metadata_fn(source) == 'None'


def _entity_contexts():
    return EntityContexts(contexts=[], keywords=[])


class TestScoreValuesWithTfidf:
    def test_delegates_to_tfidf_scorer(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='tfidf', debug_results=[], max_statements=5),
            FilterConfig(),
        )
        with patch.object(mod, 'score_values_with_tfidf', return_value={'a': 0.5}) as scorer:
            out = processor._score_values_with_tfidf(
                ['a', 'b'], QueryBundle('hello world'), _entity_contexts(),
            )
        assert out == {'a': 0.5}
        scorer.assert_called_once()


class TestScoreValuesWithModel:
    def test_builds_score_map_from_reranker(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='model', debug_results=[], max_statements=5),
            FilterConfig(),
            reranking_model=Mock(),
        )
        node = Mock(text='a', score=0.9)
        with patch.object(mod, 'SentenceReranker') as reranker_cls:
            reranker_cls.return_value.postprocess_nodes.return_value = [node]
            out = processor._score_values(['a'], QueryBundle('q'), _entity_contexts())
        assert out == {'a': 0.9}


class TestScoreValuesWithBedrock:
    def test_maps_results_back_to_values(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='bedrock', debug_results=[], max_statements=5),
            FilterConfig(),
        )
        with patch.object(mod, 'boto3') as boto3_mod, \
             patch.object(mod, 'GraphRAGConfig') as config:
            boto3_mod.Session.return_value.region_name = 'us-east-1'
            config.bedrock_reranking_model = 'model-x'
            boto3_mod.client.return_value.rerank.return_value = {
                'results': [{'index': 0, 'relevanceScore': 0.7}],
            }
            out = processor._score_values_with_bedrock(
                ['a', 'b'], QueryBundle('q'), _entity_contexts(),
            )
        assert out == {'a': 0.7}


class TestProcessResults:
    def test_none_reranker_returns_results_unchanged(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='none', debug_results=[]), FilterConfig(),
        )
        collection = _collection_with_statements()
        result = processor._process_results(collection, QueryBundle('q'))
        assert result is collection

    def test_none_reranker_short_circuits_before_formatting_values(self):
        source_metadata_fn = Mock()
        processor = RerankStatements(
            ProcessorArgs(
                reranker='none',
                debug_results=[],
                reranking_source_metadata_fn=source_metadata_fn,
            ),
            FilterConfig(),
        )
        collection = _collection_with_statements()
        result = processor._process_results(collection, QueryBundle('q'))
        assert result is collection
        source_metadata_fn.assert_not_called()

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

    def test_model_reranker_dispatches_and_applies_scores(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='model', debug_results=[], max_statements=10),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values') as scorer:
            scorer.side_effect = lambda values, *_a, **_kw: {
                values[0]: 0.1, values[1]: 0.8,
            }
            result = processor._process_results(
                _collection_with_statements(), QueryBundle('q'),
            )
        scorer.assert_called_once()
        statements = result.results[0].topics[0].statements
        assert {s.score for s in statements} == {0.1, 0.8}
        assert statements[0].score >= statements[1].score

    def test_bedrock_reranker_dispatches_and_applies_scores(self):
        processor = RerankStatements(
            ProcessorArgs(reranker='bedrock', debug_results=[], max_statements=10),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as scorer:
            scorer.side_effect = lambda values, *_a, **_kw: {
                values[0]: 0.3, values[1]: 0.7,
            }
            result = processor._process_results(
                _collection_with_statements(), QueryBundle('q'),
            )
        scorer.assert_called_once()
        statements = result.results[0].topics[0].statements
        assert {s.score for s in statements} == {0.3, 0.7}
        assert statements[0].score >= statements[1].score

    def test_reranker_chain_uses_first_successful_reranker(self):
        processor = RerankStatements(
            ProcessorArgs(reranker=['bedrock', 'tfidf'], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = lambda values, *_a, **_kw: {
                values[0]: 0.3, values[1]: 0.7,
            }
            result = processor._process_results(
                _collection_with_statements(), QueryBundle('q'),
            )
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_not_called()
        statements = result.results[0].topics[0].statements
        assert {s.score for s in statements} == {0.3, 0.7}

    def test_reranker_chain_falls_back_after_exception(self, caplog):
        processor = RerankStatements(
            ProcessorArgs(reranker=['bedrock', 'tfidf'], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        failure = RuntimeError('throttled')
        caplog.set_level(logging.WARNING, logger=mod.__name__)
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = failure
            tfidf_scorer.side_effect = lambda values, *_a, **_kw: {
                values[0]: 0.4, values[1]: 0.9,
            }
            result = processor._process_results(
                _collection_with_statements(), QueryBundle('q'),
            )
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_called_once()
        statements = result.results[0].topics[0].statements
        assert statements[0].score == 0.9
        assert statements[1].score == 0.4
        warning = next(
            record for record in caplog.records
            if record.levelno == logging.WARNING and 'Reranking with bedrock failed' in record.getMessage()
        )
        assert warning.getMessage() == 'Reranking with bedrock failed (RuntimeError); trying tfidf'
        assert warning.exc_info is not None
        assert warning.exc_info[1] is failure

    def test_reranker_chain_empty_score_map_is_terminal(self, caplog):
        fallback_policy = Mock(return_value=True)
        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'tfidf'],
                reranker_fallback_policy=fallback_policy,
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        caplog.set_level(logging.ERROR, logger=mod.__name__)
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.return_value = {}
            result = processor._process_results(
                _collection_with_statements(), QueryBundle('q'),
            )
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_not_called()
        fallback_policy.assert_not_called()
        assert any(
            record.levelno == logging.ERROR
            and record.getMessage()
            == 'Reranking with bedrock returned an empty score map; all statements will be dropped'
            for record in caplog.records
        )
        assert result.results == []

    def test_object_fallback_policy_is_rejected(self):
        class PolicyObject:
            def should_fallback(self, reranker, *, error=None):
                return True

        args = ProcessorArgs(
            reranker=['bedrock', 'tfidf'],
            reranker_fallback_policy=PolicyObject(),
            debug_results=[],
        )
        with pytest.raises(TypeError):
            RerankStatements(args, FilterConfig())

    def test_reranker_chain_can_fall_back_to_none(self, caplog):
        processor = RerankStatements(
            ProcessorArgs(reranker=['bedrock', 'none'], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        collection = _collection_with_statements()
        caplog.set_level(logging.WARNING, logger=mod.__name__)
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = RuntimeError('throttled')
            result = processor._process_results(collection, QueryBundle('q'))
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_not_called()
        assert any(
            record.levelno == logging.WARNING
            and record.getMessage() == 'Reranking with bedrock failed (RuntimeError); trying none'
            for record in caplog.records
        )
        assert result is collection

    def test_policy_denied_failure_does_not_reach_none(self):
        fallback_policy = Mock(return_value=False)
        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'none'],
                reranker_fallback_policy=fallback_policy,
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        failure = RuntimeError('not eligible for fallback')
        with patch.object(processor, '_score_values_with_bedrock', side_effect=failure):
            with pytest.raises(RuntimeError) as raised:
                processor._process_results(
                    _collection_with_statements(), QueryBundle('q'),
                )
        assert raised.value is failure
        fallback_policy.assert_called_once_with('bedrock', error=failure)

    def test_terminal_reranker_exception_propagates(self):
        policy_calls = []

        def fallback_only_from_bedrock(reranker, *, error=None):
            policy_calls.append((reranker, error))
            return reranker == 'bedrock'

        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'tfidf'],
                reranker_fallback_policy=fallback_only_from_bedrock,
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        bedrock_failure = RuntimeError('retryable failure')
        terminal_failure = RuntimeError('terminal failure')
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = bedrock_failure
            tfidf_scorer.side_effect = terminal_failure
            with pytest.raises(RuntimeError) as raised:
                processor._process_results(
                    _collection_with_statements(), QueryBundle('q'),
                )
        assert raised.value is terminal_failure
        assert policy_calls == [('bedrock', bedrock_failure)]

    def test_terminal_exception_propagates_by_default(self):
        processor = RerankStatements(
            ProcessorArgs(reranker=['bedrock', 'tfidf'], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = RuntimeError('retryable failure')
            tfidf_scorer.side_effect = RuntimeError('terminal failure')
            with pytest.raises(RuntimeError, match='terminal failure'):
                processor._process_results(
                    _collection_with_statements(), QueryBundle('q'),
                )
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_called_once()

    def test_custom_fallback_policy_can_stop_fallback(self):
        calls = []

        def fallback_policy(reranker, *, error=None):
            calls.append(reranker)
            return False

        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'tfidf'],
                reranker_fallback_policy=fallback_policy,
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = RuntimeError('do not fallback')
            with pytest.raises(RuntimeError):
                processor._process_results(
                    _collection_with_statements(), QueryBundle('q'),
                )
        assert calls == ['bedrock']
        tfidf_scorer.assert_not_called()

    def test_three_reranker_chain_stops_at_middle_success(self):
        processor = RerankStatements(
            ProcessorArgs(reranker=['bedrock', 'tfidf', 'model'], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer, \
             patch.object(processor, '_score_values') as model_scorer:
            bedrock_scorer.side_effect = RuntimeError('throttled')
            tfidf_scorer.side_effect = lambda values, *_a, **_kw: {values[0]: 0.5, values[1]: 0.8}
            result = processor._process_results(_collection_with_statements(), QueryBundle('q'))
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_called_once()
        model_scorer.assert_not_called()
        statements = result.results[0].topics[0].statements
        assert statements[0].score == 0.8

    def test_three_reranker_chain_reaches_third_after_two_failures(self):
        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'tfidf', 'model'],
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer, \
             patch.object(processor, '_score_values') as model_scorer:
            bedrock_scorer.side_effect = RuntimeError('first failure')
            tfidf_scorer.side_effect = RuntimeError('second failure')
            model_scorer.side_effect = lambda values, *_a, **_kw: {
                values[0]: 0.2, values[1]: 0.95,
            }
            result = processor._process_results(
                _collection_with_statements(), QueryBundle('q'),
            )
        bedrock_scorer.assert_called_once()
        tfidf_scorer.assert_called_once()
        model_scorer.assert_called_once()
        statements = result.results[0].topics[0].statements
        assert [statement.score for statement in statements] == [0.95, 0.2]

    def test_all_rerankers_fail_with_last_exception(self):
        policy_calls = []

        def fallback_policy(reranker, *, error=None):
            policy_calls.append((reranker, error))
            return True

        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'tfidf', 'model'],
                reranker_fallback_policy=fallback_policy,
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        bedrock_failure = RuntimeError('first failure')
        tfidf_failure = RuntimeError('second failure')
        model_failure = RuntimeError('last failure')
        with patch.object(processor, '_score_values_with_bedrock', side_effect=bedrock_failure), \
             patch.object(processor, '_score_values_with_tfidf', side_effect=tfidf_failure), \
             patch.object(processor, '_score_values', side_effect=model_failure):
            with pytest.raises(RuntimeError) as raised:
                processor._process_results(
                    _collection_with_statements(), QueryBundle('q'),
                )
        assert raised.value is model_failure
        assert policy_calls == [
            ('bedrock', bedrock_failure),
            ('tfidf', tfidf_failure),
        ]

    def test_successful_reranking_logs_only_at_debug(self, caplog):
        processor = RerankStatements(
            ProcessorArgs(reranker=['bedrock', 'tfidf'], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        caplog.set_level(logging.DEBUG, logger=mod.__name__)
        with patch.object(processor, '_score_values_with_bedrock', return_value={'value': 1.0}), \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            result = processor._score_values_with_chain(
                ['value'], QueryBundle('q'), _entity_contexts(),
            )
        assert result == {'value': 1.0}
        tfidf_scorer.assert_not_called()
        success_records = [
            record for record in caplog.records
            if record.getMessage() == 'Reranking succeeded with bedrock'
        ]
        assert len(success_records) == 1
        assert success_records[0].levelno == logging.DEBUG
        assert not any(
            record.levelno == logging.INFO and 'Reranking succeeded' in record.getMessage()
            for record in caplog.records
        )

    def test_policy_exception_propagates(self):
        def broken_policy(reranker, *, error=None):
            raise KeyError('broken policy')

        processor = RerankStatements(
            ProcessorArgs(
                reranker=['bedrock', 'tfidf'],
                reranker_fallback_policy=broken_policy,
                debug_results=[],
                max_statements=10,
            ),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values_with_bedrock') as bedrock_scorer, \
             patch.object(processor, '_score_values_with_tfidf') as tfidf_scorer:
            bedrock_scorer.side_effect = RuntimeError('throttled')
            with pytest.raises(KeyError):
                processor._process_results(
                    _collection_with_statements(), QueryBundle('q'),
                )
        tfidf_scorer.assert_not_called()


class TestKnownRerankerCoverage:
    @pytest.mark.parametrize('reranker', [r for r in KNOWN_RERANKERS if r != 'none'])
    def test_every_known_reranker_dispatches_to_a_scorer(self, reranker):
        # Drift guard: a name added to KNOWN_RERANKERS without a matching entry
        # in _score_values_with_chain's scorer registry passes chain validation
        # but silently skips reranking at query time.
        processor = RerankStatements(
            ProcessorArgs(reranker=[reranker], debug_results=[], max_statements=10),
            FilterConfig(),
        )
        with patch.object(processor, '_score_values', return_value={'v': 1.0}), \
             patch.object(processor, '_score_values_with_tfidf', return_value={'v': 1.0}), \
             patch.object(processor, '_score_values_with_bedrock', return_value={'v': 1.0}):
            scored = processor._score_values_with_chain(
                ['v'], QueryBundle('q'), EntityContexts(contexts=[], keywords=[]),
            )
        assert scored == {'v': 1.0}, f'no scorer wired up for reranker "{reranker}"'
