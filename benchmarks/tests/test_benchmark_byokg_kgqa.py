# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

import pytest

from benchmarks.scripts import benchmark_byokg_kgqa as runner
from benchmarks.utils.byokg_kgqa import Question


GRAPH = {
    'Lou Seal': {'mascot of': {'triplets': [['Lou Seal', 'mascot of', 'Giants']]}},
    'Giants': {'won': {'triplets': [['Giants', 'won', '2014 World Series']]}},
    '2014 World Series': {},
}
QUESTION = Question('q1', 'Lou Seal is the mascot for the team that last won the World Series when?',
                    ['2014 World Series'], ['Lou Seal'], ['2014 World Series'],
                    schema={'graphSummary': {'edgeLabels': ['mascot of', 'won']}})


class FakeGenerator:
    model_name = 'fake-model'

    def generate(self, prompt, system_prompt=None):
        return 'unused'


class FakeLinker:
    def parse_response(self, response):
        return {'entity-extraction': ['Lou Seal'], 'path-extraction': ['mascot of -> won']}


class FakeEngine:
    """Return shapes of the real ByoKGQueryEngine: query() -> List[str],
    generate_response() -> (List[str], str)."""

    def __init__(self, counter, kg_answer='2014 World Series', vanilla_answer='2010 World Series',
                 fail=False, kg_calls=4):
        self.kg_linker, self.counter = FakeLinker(), counter
        self.kg_answer, self.vanilla_answer, self.fail, self.kg_calls = \
            kg_answer, vanilla_answer, fail, kg_calls
        self.schema = None

    def query(self, query, iterations=2):
        if self.fail:
            raise RuntimeError('ThrottlingException')
        for _ in range(self.kg_calls - 1):
            self.counter.generate('p')
        self.kg_linker.parse_response('<entities>Lou Seal</entities>')
        return ['Lou Seal -> mascot of -> Giants', 'Giants -> won -> 2014 World Series']

    def generate_response(self, query, graph_context=''):
        self.counter.generate('p')
        answer = self.kg_answer if graph_context else self.vanilla_answer
        return [answer], f'<answers>{answer}</answers>'


def _engine(**kw):
    counter = runner.CountingGenerator(FakeGenerator())
    return FakeEngine(counter, **kw), counter


class TestRunQuestion:
    def test_kg_grounded_multi_hop_win_is_showcase(self):
        engine, counter = _engine()
        r = runner.run_question(engine, counter, QUESTION, GRAPH, 'per_question', 2)
        assert (r['hit'], r['vanilla_hit'], r['answer_in_context'], r['hops']) == (1, 0, True, 2)
        assert r['showcase'] == ['multi_hop:2'] and r['error'] is None
        assert engine.schema == QUESTION.schema

    def test_llm_calls_exclude_the_vanilla_answer(self):
        engine, counter = _engine(kg_calls=4)
        assert runner.run_question(engine, counter, QUESTION, GRAPH, 'per_question', 2)['llm_calls'] == 4

    def test_artifacts_recorded_and_parse_response_restored(self):
        engine, counter = _engine()
        original = engine.kg_linker.parse_response
        r = runner.run_question(engine, counter, QUESTION, GRAPH, 'per_question', 2)
        assert r['kg_linker_artifacts'] == [{'entity-extraction': ['Lou Seal'],
                                            'path-extraction': ['mascot of -> won']}]
        assert engine.kg_linker.parse_response == original

    def test_vanilla_also_right_is_not_showcase(self):
        engine, counter = _engine(vanilla_answer='2014 World Series')
        r = runner.run_question(engine, counter, QUESTION, GRAPH, 'per_question', 2)
        assert r['hit'] == r['vanilla_hit'] == 1 and r['showcase'] == []

    def test_failure_is_recorded_as_miss_and_parse_restored(self):
        engine, counter = _engine(fail=True)
        original = engine.kg_linker.parse_response
        r = runner.run_question(engine, counter, QUESTION, GRAPH, 'per_question', 2)
        assert r['error'] == 'RuntimeError: ThrottlingException' and r['hit'] == 0
        assert engine.kg_linker.parse_response == original


class TestRunOnce:
    def test_writes_outputs(self, tmp_path):
        engine, counter = _engine()
        s = runner.run_once(engine, counter, [QUESTION], GRAPH, 'webqsp', 2, str(tmp_path))
        assert (s['hit'], s['n_showcase']) == (100.0, 1)
        assert json.loads((tmp_path / 'summary.json').read_text())['n'] == 1
        assert len((tmp_path / 'showcase_candidates.jsonl').read_text().splitlines()) == 1
        assert len((tmp_path / 'predictions.jsonl').read_text().splitlines()) == 1

    def test_aborts_when_first_n_all_fail(self, tmp_path):
        engine, counter = _engine(fail=True)
        qs = [QUESTION] * (runner.ABORT_IF_FIRST_N_FAIL + 3)
        with pytest.raises(RuntimeError, match='all failed'):
            runner.run_once(engine, counter, qs, GRAPH, 'webqsp', 2, str(tmp_path))


def test_aggregate_reports_spread():
    agg = runner.aggregate([{'hit': 80.0}, {'hit': 84.0}, {'hit': 82.0}], 'hit')
    assert (agg['mean'], agg['min'], agg['max'], agg['spread'], agg['runs']) == (82.0, 80.0, 84.0, 4.0, 3)


def test_aggregate_uses_hit_at_2_for_medqa():
    assert runner.aggregate([{'hit@2': 60.0, 'hit': 50.0}], 'hit@2')['mean'] == 60.0


def test_counting_generator_delegates_attributes():
    c = runner.CountingGenerator(FakeGenerator())
    c.generate('p')
    c.generate('p')
    assert (c.calls, c.model_name) == (2, 'fake-model')


def test_parse_args_defaults():
    a = runner.parse_args(['--dataset', 'cwq'])
    assert (a.iterations, a.runs, a.single_best_match) == (2, 3, 'both')


# --- orchestration & stack entry point ---------------------------------------

def _dataset_on_disk(tmp_path):
    """webqsp-shaped questions + graph files at the paths DATASETS names."""
    spec = runner.DATASETS['webqsp']
    (tmp_path / 'graph').mkdir()
    (tmp_path / spec.graph_file).write_text(json.dumps(GRAPH))
    (tmp_path / spec.questions_file).write_text(json.dumps([{
        'id': 'q1', 'question': QUESTION.question, 'answer': QUESTION.answers,
        'q_entity': QUESTION.q_entities, 'a_entity': QUESTION.a_entities, 'graph': [],
        'choices': [], 'current_schema': QUESTION.schema}]))
    return str(tmp_path)


@pytest.fixture
def fake_engine(monkeypatch):
    built = []

    def build(graph, model, region, sbm):
        engine, counter = _engine()
        built.append(sbm)
        return engine, counter

    monkeypatch.setattr(runner, 'build_engine', build)
    return built


def test_run_benchmark_covers_both_settings_and_writes_aggregate(tmp_path, fake_engine):
    (tmp_path / 'data').mkdir()
    data, out = _dataset_on_disk(tmp_path / 'data'), tmp_path / 'out'
    report = runner.run_benchmark('webqsp', data, str(out), runs=2)
    assert fake_engine == [False, True]
    assert set(report['settings']) == {'single_best_match-off', 'single_best_match-on'}
    assert report['settings']['single_best_match-off']['runs'] == 2
    assert json.loads((out / 'webqsp' / 'aggregate.json').read_text())['question_ids'] == ['q1']
    assert (out / 'webqsp' / 'single_best_match-on' / 'run2' / 'summary.json').exists()


def test_run_benchmark_fails_loud_on_empty_question_set(tmp_path, fake_engine):
    (tmp_path / 'data').mkdir()
    data = _dataset_on_disk(tmp_path / 'data')
    with pytest.raises(ValueError, match='no questions to run'):
        runner.run_benchmark('webqsp', data, str(tmp_path / 'out'), limit=0)
    assert fake_engine == [], 'must fail before building an engine'


class TestFetchDataset:
    def test_copies_only_the_two_dataset_files(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(runner, '_aws', lambda *a: calls.append(a))
        runner.fetch_dataset('s3://bucket/byokg-data/', 'cwq', str(tmp_path))
        assert [c[1] for c in calls] == ['s3://bucket/byokg-data/questions_cwq.json',
                                         's3://bucket/byokg-data/graph/graph_cwq.json']
        assert all(c[0] == 'cp' for c in calls)

    def test_skips_files_already_present(self, tmp_path, monkeypatch):
        calls = []
        monkeypatch.setattr(runner, '_aws', lambda *a: calls.append(a))
        _dataset_on_disk(tmp_path)
        runner.fetch_dataset('s3://bucket/byokg-data', 'webqsp', str(tmp_path))
        assert calls == []


class FakeHandler:
    """The IntegrationTestHandler surface the test uses; runs assertions for real."""

    def __init__(self):
        self.outputs, self.failures = {}, []

    def add_output(self, key, value):
        self.outputs[key] = value

    def run_assertions(self, case):
        result = unittest.TestResult()
        unittest.defaultTestLoader.loadTestsFromTestCase(case).run(result)
        self.failures = result.failures + result.errors


class TestStackEntryPoint:
    @pytest.fixture
    def stack_env(self, tmp_path, monkeypatch, fake_engine):
        data = tmp_path / 'data'
        data.mkdir()
        _dataset_on_disk(data)
        monkeypatch.setenv('BYOKG_DATA_S3_URI', 's3://bucket/byokg-data/')
        monkeypatch.setenv('BYOKG_DATA_DIR', str(data))
        monkeypatch.setenv('BYOKG_KGQA_RUNS', '1')
        monkeypatch.setattr(runner, 'RESULTS_DIR', str(tmp_path / 'results'))
        uploads = []
        monkeypatch.setattr(runner, 'upload_benchmark_results_to_s3',
                            lambda local_dir, s3_sub_path: uploads.append((local_dir, s3_sub_path)))
        return uploads

    def test_records_outputs_uploads_and_passes(self, stack_env):
        handler, params = FakeHandler(), {}
        runner.WebqspByokgKgqa()._run_test(handler, params)
        assert handler.outputs['single_best_match-off.hit'] == 100.0
        assert handler.outputs['single_best_match-on.showcase'] == 1
        assert 'per_run' not in params['byokg_kgqa']['single_best_match-off']
        assert [s for _, s in stack_env] == ['byokg-kgqa/webqsp']
        assert handler.failures == []

    def test_error_rate_assertion_fails_the_test(self, stack_env, monkeypatch):
        monkeypatch.setattr(runner, 'build_engine', lambda *a: _engine(fail=True))
        handler = FakeHandler()
        runner.run_byokg_kgqa_test(handler, {}, 'webqsp')
        assert len(handler.failures) == 1 and 'questions errored' in handler.failures[0][1]
        assert stack_env, 'results must upload before assertions run'

    def test_missing_data_uri_fails_loud(self, monkeypatch):
        monkeypatch.delenv('BYOKG_DATA_S3_URI', raising=False)
        with pytest.raises(ValueError, match='BYOKG_DATA_S3_URI'):
            runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')


def test_suite_file_resolves_like_the_stack_runner():
    """Mirror test_suite.to_test_class: benchmark_* modules load from benchmarks.scripts."""
    import importlib
    import os
    from benchmarks.scripts.integration_test_base import IntegrationTestBase
    suite = os.path.join(os.path.dirname(__file__), '..', 'datasets', 'benchmark.byokg-kgqa')
    lines = [l.strip() for l in open(suite) if l.strip()]
    datasets = []
    for line in lines:
        module, cls = line.split('.')
        assert module.startswith('benchmark_'), f'{line}: stack resolver needs a benchmark_ prefix'
        c = getattr(importlib.import_module(f'benchmarks.scripts.{module}'), cls)
        assert issubclass(c, IntegrationTestBase)
        datasets.append(c.dataset)
    assert sorted(datasets) == sorted(runner.DATASETS)
