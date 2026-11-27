# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for custom (external) retriever support: custom:<module_path> IDs in the
retriever factory and config recording in metrics_summary.json. All offline;
no AWS calls.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from benchmarks.utils.custom_retriever import (
    CustomRetriever,
    custom_retriever_kwargs_for,
    get_module_path,
    is_custom_retriever_id,
    read_custom_retriever_kwargs,
)
from benchmarks.utils.metrics_summary import compute_metrics_summary
from benchmarks.utils.retriever_factory import create_query_engine, get_retriever_config


THIS_MODULE = __name__


class FakeResponse:
    def __init__(self, response, metadata=None):
        self.response = response
        self.metadata = metadata or {}


class FakeRetriever:
    """A custom retriever with get_config()."""

    def __init__(self, graph_store, vector_store, llm=None, top_k=3):
        self.graph_store = graph_store
        self.vector_store = vector_store
        self.llm = llm
        self.top_k = top_k

    def query(self, question):
        return FakeResponse(f'answer to {question}', {'retrieve_ms': 1.5, 'answer_ms': 2.5, 'total_ms': 4.0})

    def get_config(self):
        return {'top_k': self.top_k, 'strategy': 'fake'}


class FakeRetrieverWithoutConfig:
    """A custom retriever without get_config()."""

    def __init__(self, **kwargs):
        pass

    def query(self, question):
        return FakeResponse('')


def build_fake_retriever(graph_store, vector_store, llm=None, **kwargs):
    """A plain function works as well as a class."""
    return FakeRetriever(graph_store, vector_store, llm=llm, **kwargs)


class NotARetriever:
    def __init__(self, **kwargs):
        pass


class NonCallableQuery:
    query = 'not a method'

    def __init__(self, **kwargs):
        pass


class BadConfigRetriever(FakeRetrieverWithoutConfig):
    def get_config(self):
        return ['not', 'a', 'dict']


class NanConfigRetriever(FakeRetrieverWithoutConfig):
    def get_config(self):
        return {'threshold': float('nan')}


NOT_CALLABLE = 42


def custom_id(name):
    return f'custom:{THIS_MODULE}.{name}'


class TestCustomRetrieverId:

    def test_custom_prefix_is_detected(self):
        assert is_custom_retriever_id('custom:pkg.mod.Cls')
        assert not is_custom_retriever_id('traversal')

    def test_module_path_is_extracted(self):
        assert get_module_path('custom:pkg.mod.Cls') == ('pkg.mod', 'Cls')

    @pytest.mark.parametrize('retriever_id', ['custom:', 'custom:NoModule', 'custom:.Cls', 'custom:pkg.'])
    def test_malformed_custom_id_raises(self, retriever_id):
        with pytest.raises(ValueError, match='custom:<module>.<attribute>'):
            create_query_engine(retriever_id, MagicMock(), MagicMock())


class TestCreateCustomRetriever:

    def test_class_is_imported_and_instantiated_with_stores_llm_and_kwargs(self):
        graph_store, vector_store, llm = MagicMock(), MagicMock(), MagicMock()

        with patch(
            'benchmarks.utils.retriever_factory.LexicalGraphQueryEngine.for_traversal_based_search'
        ) as mock_builtin:
            retriever = create_query_engine(
                custom_id('FakeRetriever'), graph_store, vector_store,
                llm=llm, custom_retriever_kwargs={'top_k': 7},
            )

        mock_builtin.assert_not_called()
        assert isinstance(retriever, FakeRetriever)
        assert isinstance(retriever, CustomRetriever)
        assert retriever.graph_store is graph_store
        assert retriever.vector_store is vector_store
        assert retriever.llm is llm
        assert retriever.top_k == 7
        assert retriever.query('q').response == 'answer to q'

    def test_factory_function_is_supported(self):
        retriever = create_query_engine(custom_id('build_fake_retriever'), MagicMock(), MagicMock())
        assert isinstance(retriever, FakeRetriever)

    def test_missing_module_raises_import_error(self):
        with pytest.raises(ImportError):
            create_query_engine('custom:benchmarks.no_such_module.Retriever', MagicMock(), MagicMock())

    def test_missing_attribute_raises(self):
        with pytest.raises(ValueError, match='no attribute'):
            create_query_engine(custom_id('NoSuchRetriever'), MagicMock(), MagicMock())

    def test_non_callable_attribute_raises(self):
        with pytest.raises(ValueError, match='not a class or callable'):
            create_query_engine(custom_id('NOT_CALLABLE'), MagicMock(), MagicMock())

    def test_object_without_query_method_raises(self):
        with pytest.raises(TypeError, match='query'):
            create_query_engine(custom_id('NotARetriever'), MagicMock(), MagicMock())

    def test_non_callable_query_raises(self):
        with pytest.raises(TypeError, match='callable query'):
            create_query_engine(custom_id('NonCallableQuery'), MagicMock(), MagicMock())

    def test_bad_get_config_fails_before_any_question_runs(self):
        with pytest.raises(TypeError, match='must return a dict'):
            create_query_engine(custom_id('BadConfigRetriever'), MagicMock(), MagicMock())

    def test_nan_in_get_config_fails_before_any_question_runs(self):
        with pytest.raises(ValueError, match='NaN'):
            create_query_engine(custom_id('NanConfigRetriever'), MagicMock(), MagicMock())


class TestCustomRetrieverConfig:

    def test_get_config_is_recorded_as_hyperparameters(self):
        retriever_id = custom_id('FakeRetriever')
        retriever = FakeRetriever(None, None, top_k=7)

        config = get_retriever_config(
            retriever_id, response_llm='model-x',
            query_engine=retriever, custom_retriever_kwargs={'top_k': 7},
        )

        assert config == {
            'retriever_id': retriever_id,
            'response_llm': 'model-x',
            'module_path': f'{THIS_MODULE}.FakeRetriever',
            'constructor_kwargs': {'top_k': 7},
            'hyperparameters': {'top_k': 7, 'strategy': 'fake'},
        }

    def test_constructor_kwargs_are_recorded_without_get_config(self):
        config = get_retriever_config(
            custom_id('FakeRetrieverWithoutConfig'),
            query_engine=FakeRetrieverWithoutConfig(),
            custom_retriever_kwargs={'alpha': 0.5},
        )

        assert config['hyperparameters'] == {'alpha': 0.5}
        assert config['constructor_kwargs'] == {'alpha': 0.5}
        assert config['module_path'] == f'{THIS_MODULE}.FakeRetrieverWithoutConfig'

    def test_no_kwargs_and_no_get_config_records_empty_hyperparameters(self):
        config = get_retriever_config(
            custom_id('FakeRetrieverWithoutConfig'), query_engine=FakeRetrieverWithoutConfig(),
        )
        assert config['hyperparameters'] == {}
        assert config['constructor_kwargs'] == {}

    def test_non_serializable_values_are_recorded_as_strings(self):
        retriever = FakeRetriever(None, None)
        retriever.get_config = lambda: {'reranker': object, 'top_k': 3}

        config = get_retriever_config(custom_id('FakeRetriever'), query_engine=retriever)

        assert config['hyperparameters']['top_k'] == 3
        assert isinstance(config['hyperparameters']['reranker'], str)

    def test_non_string_keys_are_recorded_as_strings(self):
        retriever = FakeRetriever(None, None)
        retriever.get_config = lambda: {1: 'one', 'nested': {2.5: 'two and a half'}}

        config = get_retriever_config(custom_id('FakeRetriever'), query_engine=retriever)

        assert config['hyperparameters'] == {'1': 'one', 'nested': {'2.5': 'two and a half'}}

    def test_get_config_runs_once_across_creation_and_recording(self):
        calls = []

        class CountingRetriever(FakeRetriever):
            def get_config(self):
                calls.append(1)
                return {'top_k': self.top_k, 'call': len(calls)}

        with patch(f'{THIS_MODULE}.FakeRetriever', CountingRetriever):
            retriever = create_query_engine(custom_id('FakeRetriever'), MagicMock(), MagicMock())
            config = get_retriever_config(custom_id('FakeRetriever'), query_engine=retriever)

        assert len(calls) == 1
        assert config['hyperparameters'] == {'top_k': 3, 'call': 1}

    def test_get_config_must_return_a_dict(self):
        retriever = FakeRetriever(None, None)
        retriever.get_config = lambda: ['not', 'a', 'dict']

        with pytest.raises(TypeError, match='must return a dict'):
            get_retriever_config(custom_id('FakeRetriever'), query_engine=retriever)

    def test_config_is_embedded_in_metrics_summary(self):
        retriever_id = custom_id('FakeRetriever')
        config = get_retriever_config(retriever_id, query_engine=FakeRetriever(None, None))

        summary = compute_metrics_summary([], retriever_id, 'cuad', 'model-x', 0, retriever_config=config)

        assert summary['retriever'] == retriever_id
        assert summary['retriever_config'] == config
        json.dumps(summary)

    def test_builtin_config_ignores_custom_arguments(self):
        assert get_retriever_config('traversal', query_engine=FakeRetriever(None, None),
                                    custom_retriever_kwargs={'top_k': 1}) == get_retriever_config('traversal')


class TestReadCustomRetrieverKwargs:

    ENV = 'BENCHMARK_CUSTOM_RETRIEVER_KWARGS'

    @pytest.mark.parametrize('value', [None, '', '   '])
    def test_unset_or_empty_is_none(self, monkeypatch, value):
        if value is None:
            monkeypatch.delenv(self.ENV, raising=False)
        else:
            monkeypatch.setenv(self.ENV, value)
        assert read_custom_retriever_kwargs() is None

    def test_json_object_is_parsed(self, monkeypatch):
        monkeypatch.setenv(self.ENV, '{"vss_top_k": 5, "reranker": "tfidf"}')
        assert read_custom_retriever_kwargs() == {'vss_top_k': 5, 'reranker': 'tfidf'}

    @pytest.mark.parametrize('value, message', [('{vss_top_k: 5}', 'not valid JSON'), ('[1, 2]', 'JSON object')])
    def test_invalid_value_raises(self, monkeypatch, value, message):
        monkeypatch.setenv(self.ENV, value)
        with pytest.raises(ValueError, match=message):
            read_custom_retriever_kwargs()

    def test_builtin_retrievers_never_read_the_variable(self, monkeypatch):
        monkeypatch.setenv(self.ENV, '{not json')

        assert custom_retriever_kwargs_for('traversal') is None
        with pytest.raises(ValueError, match='not valid JSON'):
            custom_retriever_kwargs_for(custom_id('FakeRetriever'))


class TestRunBenchmarkQueryWithCustomRetriever:
    """The query stage benchmarks a custom retriever and records its config."""

    def test_responses_and_metrics_summary_are_written(self, tmp_path, monkeypatch):
        from benchmarks.scripts import benchmark_query

        data_dir = tmp_path / 'source-data'
        (data_dir / 'cuad').mkdir(parents=True)
        (data_dir / 'cuad' / 'qa.json').write_text(json.dumps([
            {'input': 'What is the term?', 'output': [{'text': 'Two years'}]},
            {'input': 'Who are the parties?', 'output': [{'text': 'A and B'}]},
        ]))
        monkeypatch.chdir(tmp_path)

        retriever_id = custom_id('FakeRetriever')
        params = {}

        with patch.object(benchmark_query, 'sync_benchmark_data_from_s3'), \
             patch.object(benchmark_query, 'GraphRAGConfig'), \
             patch.object(benchmark_query, 'TokenTrackingLLMCache'):
            benchmark_query.run_benchmark_query(
                MagicMock(), params, dataset='cuad', data_dir=str(data_dir),
                response_llm='model-x', retriever_id=retriever_id,
                custom_retriever_kwargs={'top_k': 9},
            )

        results_dir = tmp_path / 'benchmark-results' / 'cuad' / retriever_id
        assert params['benchmark_responses_path'] == str(results_dir.relative_to(tmp_path))

        responses = [json.loads(line) for line in (results_dir / 'responses.jsonl').read_text().splitlines()]
        assert [r['response'] for r in responses] == ['answer to What is the term?', 'answer to Who are the parties?']
        assert responses[0]['total_ms'] == 4

        summary = json.loads((results_dir / 'metrics_summary.json').read_text())
        assert summary['retriever'] == retriever_id
        assert summary['num_queries'] == 2
        assert summary['retriever_config']['module_path'] == f'{THIS_MODULE}.FakeRetriever'
        assert summary['retriever_config']['constructor_kwargs'] == {'top_k': 9}
        assert summary['retriever_config']['hyperparameters'] == {'top_k': 9, 'strategy': 'fake'}

