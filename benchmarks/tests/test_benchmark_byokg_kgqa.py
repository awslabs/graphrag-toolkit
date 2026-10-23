# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

import pytest

from benchmarks.scripts import benchmark_byokg_kgqa as runner
from benchmarks.utils.byokg_kgqa import Question, hop_distance


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
                 fail=False, kg_calls=4, vanilla_fail=False):
        self.kg_linker, self.counter = FakeLinker(), counter
        self.kg_answer, self.vanilla_answer, self.fail, self.kg_calls = \
            kg_answer, vanilla_answer, fail, kg_calls
        self.vanilla_fail = vanilla_fail
        self.schema = None

    def query(self, query, iterations=2):
        if self.fail:
            raise RuntimeError('ThrottlingException')
        for _ in range(self.kg_calls - 1):
            self.counter.generate('p')
        self.kg_linker.parse_response('<entities>Lou Seal</entities>')
        return ['Lou Seal -> mascot of -> Giants', 'Giants -> won -> 2014 World Series']

    def generate_response(self, query, graph_context=''):
        if not graph_context and self.vanilla_fail:
            raise RuntimeError('ThrottlingException')
        self.counter.generate('p')
        answer = self.kg_answer if graph_context else self.vanilla_answer
        return [answer], f'<answers>{answer}</answers>'


def _precomputed(questions):
    """The schemas / hops maps run_benchmark builds once and passes to run_once."""
    return ({q.id: q.schema for q in questions},
            {q.id: hop_distance(GRAPH, q.q_entities, q.a_entities) for q in questions})


def _engine(**kw):
    counter = runner.CountingGenerator(FakeGenerator())
    return FakeEngine(counter, **kw), counter


class TestRunQuestion:
    def test_kg_grounded_multi_hop_win_is_showcase(self):
        engine, counter = _engine()
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert (r['hit'], r['vanilla_hit'], r['answer_in_context'], r['hops']) == (1, 0, True, 2)
        assert r['showcase'] == ['multi_hop:2'] and r['error'] is None
        assert engine.schema == QUESTION.schema

    def test_llm_calls_exclude_the_vanilla_answer(self):
        engine, counter = _engine(kg_calls=4)
        assert runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)['llm_calls'] == 4

    def test_artifacts_recorded_and_parse_response_restored(self):
        engine, counter = _engine()
        original = engine.kg_linker.parse_response
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert r['kg_linker_artifacts'] == [{'entity-extraction': ['Lou Seal'],
                                            'path-extraction': ['mascot of -> won']}]
        assert engine.kg_linker.parse_response == original

    def test_vanilla_also_right_is_not_showcase(self):
        engine, counter = _engine(vanilla_answer='2014 World Series')
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert r['hit'] == r['vanilla_hit'] == 1 and r['showcase'] == []

    def test_no_kg_failure_keeps_the_byokg_result(self):
        engine, counter = _engine(vanilla_fail=True)
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert (r['hit'], r['error']) == (1, None), 'a throttled no-KG call must not void BYOKG'
        assert r['vanilla_error'] == 'RuntimeError: ThrottlingException'
        assert r['vanilla_predictions'] == [] and r['vanilla_hit'] == 0

    def test_parse_response_restored_to_class_lookup(self):
        engine, counter = _engine()
        assert 'parse_response' not in vars(engine.kg_linker)
        runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert 'parse_response' not in vars(engine.kg_linker), 'no instance attribute left behind'

    def test_failure_is_recorded_as_miss_and_parse_restored(self):
        engine, counter = _engine(fail=True)
        original = engine.kg_linker.parse_response
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert r['error'] == 'RuntimeError: ThrottlingException' and r['hit'] == 0
        assert engine.kg_linker.parse_response == original


class TestRunOnce:
    def test_writes_outputs(self, tmp_path):
        engine, counter = _engine()
        s = runner.run_once(engine, counter, [QUESTION], *_precomputed([QUESTION]), 'hit', 2, str(tmp_path))
        assert (s['hit'], s['n_showcase']) == (100.0, 1)
        assert json.loads((tmp_path / 'summary.json').read_text())['n'] == 1
        assert len((tmp_path / 'showcase_candidates.jsonl').read_text().splitlines()) == 1
        assert len((tmp_path / 'predictions.jsonl').read_text().splitlines()) == 1

    def test_aborts_when_first_n_all_fail(self, tmp_path):
        engine, counter = _engine(fail=True)
        qs = [QUESTION] * (runner.ABORT_IF_FIRST_N_FAIL + 3)
        with pytest.raises(RuntimeError, match='all failed'):
            runner.run_once(engine, counter, qs, *_precomputed(qs), 'hit', 2, str(tmp_path))


def _summary(score, ids, key='hit'):
    return {key: score, 'n_showcase': len(ids), 'showcase_ids': ids}


def test_aggregate_reports_spread():
    agg = runner.aggregate([_summary(80.0, []), _summary(84.0, []), _summary(82.0, [])], 'hit')
    assert (agg['mean'], agg['min'], agg['max'], agg['spread'], agg['runs']) == (82.0, 80.0, 84.0, 4.0, 3)


def test_aggregate_counts_showcase_per_run_and_distinct_not_summed():
    # Three repeats of one question list: 2, 2 and 3 candidates, 3 distinct questions.
    agg = runner.aggregate([_summary(80.0, ['q1', 'q2']), _summary(80.0, ['q1', 'q2']),
                            _summary(80.0, ['q1', 'q2', 'q3'])], 'hit')
    assert (agg['showcase_per_run_mean'], agg['showcase_distinct']) == (2.3, 3)


def test_aggregate_uses_hit_at_2_for_medqa():
    assert runner.aggregate([{**_summary(60.0, [], 'hit@2'), 'hit': 50.0}], 'hit@2')['mean'] == 60.0


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


def test_hops_and_schema_computed_once_per_question_not_per_run(tmp_path, fake_engine, monkeypatch):
    calls = {'hops': 0, 'schema': 0}
    real_hops, real_schema = runner.hop_distance, runner.build_schema

    def count(key, fn):
        def wrapped(*a, **k):
            calls[key] += 1
            return fn(*a, **k)
        return wrapped

    monkeypatch.setattr(runner, 'hop_distance', count('hops', real_hops))
    monkeypatch.setattr(runner, 'build_schema', count('schema', real_schema))
    (tmp_path / 'data').mkdir()
    runner.run_benchmark('webqsp', _dataset_on_disk(tmp_path / 'data'), str(tmp_path / 'out'), runs=3)
    # 1 question × 2 settings × 3 runs = 6 run_question calls, but one computation each.
    assert calls == {'hops': 1, 'schema': 1}


class TestResume:
    def _run(self, tmp_path, monkeypatch, **kw):
        asked = []
        real = runner.run_question
        monkeypatch.setattr(runner, 'run_question', lambda *a, **k: asked.append(a) or real(*a, **k))
        report = runner.run_benchmark('webqsp', self.data, str(tmp_path / 'out'), runs=2, **kw)
        return report, len(asked)

    @pytest.fixture(autouse=True)
    def data(self, tmp_path, fake_engine):
        (tmp_path / 'data').mkdir()
        self.data = _dataset_on_disk(tmp_path / 'data')

    def test_rerun_reuses_completed_runs(self, tmp_path, monkeypatch):
        first, n1 = self._run(tmp_path, monkeypatch)
        second, n2 = self._run(tmp_path, monkeypatch)
        assert (n1, n2) == (4, 0), '2 settings x 2 runs, then nothing to redo'
        assert second['settings'] == first['settings']

    def test_no_resume_reruns(self, tmp_path, monkeypatch):
        self._run(tmp_path, monkeypatch)
        assert self._run(tmp_path, monkeypatch, resume=False)[1] == 4

    def test_summary_from_another_configuration_fails_loud(self, tmp_path, monkeypatch):
        self._run(tmp_path, monkeypatch)
        with pytest.raises(RuntimeError, match='different configuration'):
            self._run(tmp_path, monkeypatch, iterations=3)


class TestProvenance:
    def test_records_inference_config_and_source_hashes(self, monkeypatch):
        monkeypatch.setenv('GRAPHRAG_TOOLKIT_GIT_SHA', 'abc123-dirty')
        p = runner.provenance(runner.CountingGenerator(FakeGenerator()))
        assert p['git_sha'] == 'abc123-dirty'
        assert p['inference']['model_name'] == 'fake-model'
        assert set(p) == {'git_sha', 'byokg_rag_source_sha256', 'byokg_rag_prompts_sha256', 'inference'}

    def test_tree_hash_changes_with_content_and_ignores_other_files(self, tmp_path):
        (tmp_path / 'a.yaml').write_text('prompt: v1')
        (tmp_path / 'notes.txt').write_text('ignored')
        before = runner._tree_sha256(str(tmp_path), ('.yaml',))
        (tmp_path / 'notes.txt').write_text('still ignored')
        assert runner._tree_sha256(str(tmp_path), ('.yaml',)) == before
        (tmp_path / 'a.yaml').write_text('prompt: v2')
        assert runner._tree_sha256(str(tmp_path), ('.yaml',)) != before

    def test_written_to_aggregate(self, tmp_path, fake_engine):
        (tmp_path / 'data').mkdir()
        report = runner.run_benchmark('webqsp', _dataset_on_disk(tmp_path / 'data'),
                                      str(tmp_path / 'out'), runs=1)
        assert report['provenance']['inference']['max_retries'] is None  # FakeGenerator lacks it
        assert 'provenance' in json.loads((tmp_path / 'out' / 'webqsp' / 'aggregate.json').read_text())


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
        assert handler.outputs['single_best_match-on.showcase_distinct'] == 1
        # Deltas against each structured reference: 100.0 - 82.8 (Haiku-3.5, full system).
        assert handler.outputs['single_best_match-off.delta_vs_claude-3.5-haiku_full'] == 17.2
        assert 'inference' in handler.outputs['provenance']
        assert 'byokg_kgqa' not in params, 'shared suite params must not carry per-dataset results'
        # Each finished run is uploaded as it completes, then the dataset dir for aggregate.json.
        assert [s for _, s in stack_env] == ['byokg-kgqa/webqsp/single_best_match-off/run1',
                                             'byokg-kgqa/webqsp/single_best_match-on/run1',
                                             'byokg-kgqa/webqsp']
        assert handler.failures == []

    def test_completed_runs_are_uploaded_before_a_later_run_crashes(self, stack_env, monkeypatch):
        built = []

        def build(*a):
            built.append(a)
            # Second setting's engine dies on every question → run_once aborts.
            return _engine(fail=len(built) > 1)

        monkeypatch.setattr(runner, 'build_engine', build)
        with pytest.raises(RuntimeError, match='all failed'):
            # ABORT_IF_FIRST_N_FAIL needs 5 questions to trigger.
            monkeypatch.setattr(runner, 'sample', lambda qs, limit, seed: list(qs) * 5)
            runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert [s for _, s in stack_env] == ['byokg-kgqa/webqsp/single_best_match-off/run1']

    def test_bad_setting_fails_before_download(self, stack_env, monkeypatch):
        fetched = []
        monkeypatch.setattr(runner, 'fetch_dataset', lambda *a: fetched.append(a))
        monkeypatch.setenv('BYOKG_SINGLE_BEST_MATCH', 'true')
        with pytest.raises(ValueError, match="single_best_match must be one of"):
            runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert fetched == [], 'config must be validated before any download'

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
