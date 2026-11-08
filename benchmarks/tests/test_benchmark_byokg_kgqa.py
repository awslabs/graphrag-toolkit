# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import unittest

import pytest

from benchmarks.scripts import benchmark_byokg_kgqa as runner
from benchmarks.utils.byokg_kgqa import Question, hop_distance


@pytest.fixture(autouse=True)
def _no_ambient_benchmark_env(monkeypatch):
    # Operators `source .env` with arm flags exported (see the README); no test may
    # inherit them, or reach real S3 through an arm that needs embeddings.
    for var in list(os.environ):
        if var.startswith('BYOKG_') or var == 'BENCHMARK_QA_LIMIT':
            monkeypatch.delenv(var)
    monkeypatch.setattr(runner, '_s3_client', lambda: pytest.fail('a test reached a real S3 client'))


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


class FakeEntityLinker:
    """EntityLinker.link() return shapes: flat List[str], or one list per mention
    with group_by_mention (the single_best_match path)."""

    def __init__(self, candidates=('Lou Seal', 'Lou Seal mascot')):
        self.candidates = list(candidates)

    def link(self, mentions, return_dict=True, group_by_mention=False, **kwargs):
        if group_by_mention:
            return [list(self.candidates) for _ in mentions]
        return [self.candidates[0] for _ in mentions]


class FakeEngine:
    """Return shapes of the real ByoKGQueryEngine: query() -> List[str],
    generate_response() -> (List[str], str)."""

    def __init__(self, counter, kg_answer='2014 World Series', vanilla_answer='2010 World Series',
                 fail=False, kg_calls=4, vanilla_fail=False):
        self.kg_linker, self.counter = FakeLinker(), counter
        self.entity_linker = FakeEntityLinker()
        self.kg_answer, self.vanilla_answer, self.fail, self.kg_calls = \
            kg_answer, vanilla_answer, fail, kg_calls
        self.vanilla_fail = vanilla_fail
        self.schema = None
        self.prompts = []  # task_prompt per generate_response() call

    def query(self, query, iterations=2):
        if self.fail:
            raise RuntimeError('ThrottlingException')
        for _ in range(self.kg_calls - 1):
            self.counter.generate('p')
        self.kg_linker.parse_response('<entities>Lou Seal</entities>')
        self.entity_linker.link(['Lou Seal'], return_dict=False)  # as the engine does
        return ['Lou Seal -> mascot of -> Giants', 'Giants -> won -> 2014 World Series']

    def generate_response(self, query, graph_context='', task_prompt=None, user_input=''):
        self.prompts.append(task_prompt)
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

    def test_errored_kg_path_scores_zero_on_hit_at_2(self):
        # The no-KG guess is right, but the KG path errored: a miss, as for hit.
        engine, counter = _engine(fail=True, vanilla_answer='2014 World Series')
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert (r['vanilla_hit'], r['hit'], r['hit@2']) == (1, 0, 0)

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

    def test_aborts_when_no_kg_call_fails_on_first_n(self, tmp_path):
        engine, counter = _engine(vanilla_fail=True)
        qs = [QUESTION] * (runner.ABORT_IF_FIRST_N_FAIL + 3)
        with pytest.raises(RuntimeError, match=r'all failed \(vanilla_error\)'):
            runner.run_once(engine, counter, qs, *_precomputed(qs), 'hit', 2, str(tmp_path))

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

    def build(graph, model, region, sbm, parts=None, context_selector=None, split_entity_lines=False):
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

    def test_scorer_change_invalidates_resume(self, tmp_path, monkeypatch):
        # The reviewer's repro: same command twice with the scorer edited in between.
        code = tmp_path / 'byokg_kgqa.py'
        code.write_text('def hit(): ...')
        monkeypatch.setattr(runner, 'BENCHMARK_CODE', (str(code),))
        self._run(tmp_path, monkeypatch)
        code.write_text('def hit(): return 0')
        with pytest.raises(RuntimeError, match='different configuration'):
            self._run(tmp_path, monkeypatch)

    def test_different_graph_file_invalidates_resume(self, tmp_path, monkeypatch):
        self._run(tmp_path, monkeypatch)
        graph = tmp_path / 'data' / runner.DATASETS['webqsp'].graph_file
        g = json.loads(graph.read_text())
        g['Giants']['won']['triplets'].append(['Giants', 'won', '2010 World Series'])
        graph.write_text(json.dumps(g))  # e.g. an alternate file dropped at the expected path
        with pytest.raises(RuntimeError, match='different configuration'):
            self._run(tmp_path, monkeypatch)

    def test_unhealthy_run_is_rerun_not_reused(self, tmp_path, monkeypatch, fake_engine):
        monkeypatch.setattr(runner, 'build_engine', lambda *a, **k: _engine(vanilla_fail=True))
        self._run(tmp_path, monkeypatch)                       # every no-KG call throttled
        monkeypatch.setattr(runner, 'build_engine', lambda *a, **k: _engine())
        report, asked = self._run(tmp_path, monkeypatch)        # same key, healthy engine
        assert asked == 4, 'throttled runs must be redone, not carried forward'
        assert all(s['n_vanilla_errors'] == 0
                   for agg in report['settings'].values() for s in agg['per_run'])


class TestProvenance:
    def test_records_inference_config_and_source_hashes(self, monkeypatch):
        monkeypatch.setenv('GRAPHRAG_TOOLKIT_GIT_SHA', 'abc123-dirty')
        p = runner.provenance(runner.CountingGenerator(FakeGenerator()))
        assert p['git_sha'] == 'abc123-dirty'
        assert p['inference']['model_name'] == 'fake-model'
        assert set(p) == {'git_sha', 'byokg_rag_source_sha256', 'byokg_rag_prompts_sha256',
                          'benchmark_code_sha256', 'data_sha256', 'inference', 'retrieval', 'answer_selection'}

    def test_tree_hash_skips_pycache(self, tmp_path):
        (tmp_path / 'm.py').write_text('x = 1')
        before = runner._tree_sha256(str(tmp_path), ('.py',))
        (tmp_path / '__pycache__').mkdir()
        (tmp_path / '__pycache__' / 'stale.py').write_text('x = 2')
        assert runner._tree_sha256(str(tmp_path), ('.py',)) == before

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


class TestDatasetLimit:
    def test_per_dataset_limit_overrides_the_global_one(self, monkeypatch):
        monkeypatch.setenv('BENCHMARK_QA_LIMIT', '500')
        monkeypatch.setenv('BYOKG_CWQ_LIMIT', '200')
        assert runner.dataset_limit('cwq') == 200
        assert runner.dataset_limit('cronquestions') == 500, 'unset dataset falls back to global'

    def test_unset_means_full_set(self, monkeypatch):
        for var in ('BENCHMARK_QA_LIMIT', 'BYOKG_CRONQUESTIONS_LIMIT'):
            monkeypatch.delenv(var, raising=False)
        assert runner.dataset_limit('cronquestions') is None

    def test_sampling_one_dataset_leaves_the_other_in_full(self, tmp_path, fake_engine, monkeypatch):
        monkeypatch.delenv('BENCHMARK_QA_LIMIT', raising=False)
        monkeypatch.setenv('BYOKG_WEBQSP_LIMIT', '1')
        (tmp_path / 'data').mkdir()
        report = runner.run_benchmark('webqsp', _dataset_on_disk(tmp_path / 'data'),
                                      str(tmp_path / 'out'), runs=1,
                                      limit=runner.dataset_limit('webqsp'))
        assert (report['n_questions'], report['n_available']) == (1, 1)


def test_provenance_computed_once_for_both_settings(tmp_path, fake_engine, monkeypatch):
    calls = []
    real = runner.provenance
    monkeypatch.setattr(runner, 'provenance', lambda *a, **k: calls.append(1) or real(*a, **k))
    (tmp_path / 'data').mkdir()
    runner.run_benchmark('webqsp', _dataset_on_disk(tmp_path / 'data'), str(tmp_path / 'out'), runs=1)
    assert len(calls) == 1, 'hashing multi-GB data twice per dataset is waste'


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
        # paper_ deltas against each structured reference, keyed by the question set:
        # 100.0 - 82.8 (Haiku-3.5, full system, other 500-Q subset); 100.0 - 87.1 (full set).
        assert handler.outputs['single_best_match-off.paper_delta_vs_claude-3.5-haiku_full_ih500'] == 17.2
        assert handler.outputs['single_best_match-off.paper_delta_vs_claude-3.5-sonnet_full'] == 12.9
        assert not any(k.startswith('single_best_match-off.delta_vs') for k in handler.outputs)
        assert 'inference' in handler.outputs['provenance']
        assert 'byokg_kgqa' not in params, 'shared suite params must not carry per-dataset results'
        # Each finished run is uploaded as it completes, then the dataset dir for aggregate.json.
        assert [s for _, s in stack_env] == ['byokg-kgqa/webqsp/single_best_match-off/run1',
                                             'byokg-kgqa/webqsp/single_best_match-on/run1',
                                             'byokg-kgqa/webqsp']
        assert handler.failures == []

    def test_completed_runs_are_uploaded_before_a_later_run_crashes(self, stack_env, monkeypatch):
        built = []

        def build(*a, **k):
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
        monkeypatch.setattr(runner, 'build_engine', lambda *a, **k: _engine(fail=True))
        handler = FakeHandler()
        runner.run_byokg_kgqa_test(handler, {}, 'webqsp')
        assert len(handler.failures) == 1 and 'KG path errored on 100.0%' in handler.failures[0][1]
        assert stack_env, 'results must upload before assertions run'

    def test_throttled_no_kg_baseline_fails_the_test(self, stack_env, monkeypatch):
        # KG path healthy, every no-KG call throttled: vanilla_hit would read 0.0.
        monkeypatch.setattr(runner, 'build_engine', lambda *a, **k: _engine(vanilla_fail=True))
        handler = FakeHandler()
        runner.run_byokg_kgqa_test(handler, {}, 'webqsp')
        assert handler.outputs['single_best_match-off.hit'] == 100.0, 'KG result must survive'
        assert len(handler.failures) == 1
        assert 'no-KG baseline errored on 100.0%' in handler.failures[0][1]

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


# --- optional text-based retrieval ------------------------------------------------

TEXT = runner.RetrievalConfig(text_retrieval=True, embed_model='cohere.embed-english-v3')


class TestLinkedNodes:
    def test_flat_result_seeds_every_node(self):
        assert runner.linked_nodes(['A', 'B']) == ['A', 'B']

    def test_grouped_result_seeds_first_candidate_per_mention(self):
        # What ByoKGQueryEngine does with single_best_match: candidates[0] per mention.
        assert runner.linked_nodes([['A', 'A2'], [], ['B']]) == ['A', 'B']

    def test_return_dict_result(self):
        assert runner.linked_nodes({'hits': [{'document_id': 'A'}]}) == ['A']

    def test_unknown_shape_fails_loud(self):
        with pytest.raises(TypeError, match='unexpected EntityLinker.link'):
            runner.linked_nodes([1, 2])


class TestSeedRecall:
    def test_recorded_against_gold_and_link_restored(self):
        engine, counter = _engine()
        assert 'link' not in vars(engine.entity_linker)
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert (r['linked_seeds'], r['seed_recall']) == (['Lou Seal'], 1.0)
        assert 'link' not in vars(engine.entity_linker), 'no instance attribute left behind'

    def test_grouped_path_counts_only_the_seeds_used(self):
        engine, counter = _engine()

        def grouped_query(query, iterations=2):
            engine.entity_linker.link(['Lou Seal'], return_dict=False, group_by_mention=True)
            return []
        engine.query = grouped_query
        r = runner.run_question(engine, counter, QUESTION, QUESTION.schema, 2, 2)
        assert r['linked_seeds'] == ['Lou Seal'], 'second candidate is not a seed'

    def test_no_gold_entities_is_not_zero(self):
        engine, counter = _engine()
        q = Question('m1', 'medical question', ['Clopidogrel'])
        assert runner.run_question(engine, counter, q, {}, None, 2)['seed_recall'] is None

    def test_aggregate_mean(self):
        agg = runner.aggregate([{**_summary(80.0, []), 'seed_recall': 60.0},
                                {**_summary(80.0, []), 'seed_recall': 70.0}], 'hit')
        assert agg['seed_recall_mean'] == 65.0


class TestRetrievalConfig:
    def test_off_by_default(self):
        off = runner.RetrievalConfig()
        assert not (off.needs_embeddings or off.embedding_linking or off.keep_verbatim_names
                    or off.split_entity_lines)
        assert runner.retrieval_provenance(runner.RetrievalConfig(), None)['embed_model'] is None

    def test_cache_key_tracks_graph_and_model(self):
        assert runner.embedding_cache_key('ab' * 32, 'cohere.embed-english-v3') == \
            'abababababababab-cohere.embed-english-v3'
        assert runner.embedding_cache_key('ab' * 32, 'arn:aws:x/y') == 'abababababababab-arn_aws_x_y'

    def test_flags_change_the_fingerprint(self):
        prov = runner.provenance(runner.CountingGenerator(FakeGenerator()), data_sha256={})
        keys = set()
        for retrieval in (runner.RetrievalConfig(), TEXT,
                          runner.RetrievalConfig(text_retrieval=True, text_topk=5, embed_model='cohere.embed-english-v3')):
            p = {**prov, 'retrieval': runner.retrieval_provenance(retrieval, 1024)}
            keys.add(runner.run_key(['q1'], True, 2, p))
        assert len(keys) == 3, 'resume must never reuse a run across retrieval arms'

    def test_unused_settings_do_not_split_the_fingerprint(self):
        model = 'cohere.embed-english-v3'  # explicit: resolving the default imports byokg-rag
        off = [runner.retrieval_provenance(runner.RetrievalConfig(text_topk=k, embed_model=model), None)
               for k in (5, 10)]
        text = [runner.retrieval_provenance(
            runner.RetrievalConfig(text_retrieval=True, text_topk=k, embed_model=model), 1024) for k in (5, 10)]
        assert off[0] == off[1] and (off[0]['text_topk'], off[0]['embed_model']) == (None, None)
        assert text[0] != text[1]

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv('BYOKG_TEXT_RETRIEVAL', 'true')
        monkeypatch.setenv('BYOKG_TEXT_TOPK', '5')
        r = runner.retrieval_from_env()
        assert (r.text_retrieval, r.text_topk) == (True, 5)

    def test_parse_args(self):
        a = runner.parse_args(['--dataset', 'cronquestions', '--text-retrieval', '--text-topk', '7'])
        assert (a.text_retrieval, a.text_topk) == (True, 7)


class TestRetrievalWiring:
    @pytest.fixture
    def parts_spy(self, monkeypatch, fake_engine):
        seen = {}

        def build_parts(graph, retrieval, region, cache_dir):
            seen.update(retrieval=retrieval, cache_dir=cache_dir)
            return runner.RetrievalParts(text_triplet_retriever='TEXT', embedding_dim=1024)
        built = []
        monkeypatch.setattr(runner, 'build_retrieval_parts', build_parts)
        monkeypatch.setattr(runner, 'build_engine',
                            lambda graph, model, region, sbm, parts=None, context_selector=None,
                            split_entity_lines=False: built.append(parts) or _engine())
        return seen, built

    def _run(self, tmp_path, **kw):
        data = tmp_path / 'data'
        if not data.exists():
            data.mkdir()
            _dataset_on_disk(data)
        return runner.run_benchmark('webqsp', str(data), str(tmp_path / 'out'), runs=1, **kw)

    def test_parts_built_once_and_reach_every_engine(self, tmp_path, parts_spy):
        seen, built = parts_spy
        report = self._run(tmp_path, retrieval=TEXT)
        assert [p.text_triplet_retriever for p in built] == ['TEXT', 'TEXT'], 'both settings, one build'
        assert '/embeddings/' in seen['cache_dir'] and seen['cache_dir'].endswith('-cohere.embed-english-v3')
        assert report['provenance']['retrieval'] == {
            'text_retrieval': True, 'embedding_linking': False, 'keep_verbatim_names': False,
            'split_entity_lines': False, 'text_topk': 10,
            'embed_model': 'cohere.embed-english-v3', 'embedding_dim': 1024}

    def test_no_cache_and_no_hook_when_off(self, tmp_path, parts_spy):
        seen, _ = parts_spy
        hooked = []
        self._run(tmp_path, before_embeddings=hooked.append, after_embeddings=hooked.append)
        assert seen['cache_dir'] is None and hooked == []

    def test_hooks_bracket_the_build_with_its_cache_dir(self, tmp_path, parts_spy):
        seen, _ = parts_spy
        events = []
        self._run(tmp_path, retrieval=TEXT,
                  before_embeddings=lambda d: events.append(('before', d, 'cache_dir' in seen)),
                  after_embeddings=lambda d: events.append(('after', d, 'cache_dir' in seen)))
        assert events == [('before', seen['cache_dir'], False), ('after', seen['cache_dir'], True)]

    def test_switching_an_arm_on_refuses_the_baseline_results(self, tmp_path, parts_spy):
        self._run(tmp_path)
        with pytest.raises(RuntimeError, match='different configuration'):
            self._run(tmp_path, retrieval=TEXT)


LINKING_FLAGS = ('embedding_linking', 'keep_verbatim_names', 'split_entity_lines')


class TestEntityLinkingOptions:
    def test_only_text_retrieval_and_embedding_linking_need_embeddings(self):
        assert runner.RetrievalConfig(text_retrieval=True).needs_embeddings
        assert runner.RetrievalConfig(embedding_linking=True).needs_embeddings
        assert not runner.RetrievalConfig(keep_verbatim_names=True, split_entity_lines=True).needs_embeddings

    def test_each_option_changes_the_fingerprint(self):
        prov = runner.provenance(runner.CountingGenerator(FakeGenerator()), data_sha256={})
        model = 'cohere.embed-english-v3'
        arms = [runner.RetrievalConfig(embed_model=model)] + [
            runner.RetrievalConfig(embed_model=model, **{flag: True}) for flag in LINKING_FLAGS]
        keys = {runner.run_key(['q1'], True, 2, {**prov, 'retrieval': runner.retrieval_provenance(r, 1024)})
                for r in arms}
        assert len(keys) == len(arms), 'resume must never reuse a run across linking arms'

    def test_options_without_embeddings_record_no_model(self):
        record = runner.retrieval_provenance(
            runner.RetrievalConfig(keep_verbatim_names=True, split_entity_lines=True), None)
        assert record['embed_model'] is None and record['keep_verbatim_names'] and record['split_entity_lines']

    def test_embedding_linking_records_its_model(self):
        prov = runner.provenance(runner.CountingGenerator(FakeGenerator()), data_sha256={})
        keys = {runner.run_key(['q1'], True, 2, {**prov, 'retrieval': runner.retrieval_provenance(
            runner.RetrievalConfig(embedding_linking=True, embed_model=model), 1024)})
            for model in ('cohere.embed-english-v3', 'cohere.embed-multilingual-v3')}
        assert len(keys) == 2, 'two same-dimension models must not share runs'

    @pytest.mark.parametrize('flag', LINKING_FLAGS)
    def test_each_env_var_sets_only_its_option(self, monkeypatch, flag):
        monkeypatch.setenv(f'BYOKG_{flag.upper()}', 'true')
        r = runner.retrieval_from_env()
        assert {f: getattr(r, f) for f in LINKING_FLAGS + ('text_retrieval',)} == {
            f: f == flag for f in LINKING_FLAGS + ('text_retrieval',)}

    @pytest.mark.parametrize('flag', LINKING_FLAGS)
    def test_each_cli_flag_sets_only_its_option(self, monkeypatch, tmp_path, flag):
        ran = []
        monkeypatch.setattr(runner, 'run_benchmark', lambda *a, **k: ran.append(k))
        runner.main(['--dataset', 'cwq', '--data-dir', str(tmp_path), '--' + flag.replace('_', '-')])
        r = ran[0]['retrieval']
        assert {f: getattr(r, f) for f in LINKING_FLAGS + ('text_retrieval',)} == {
            f: f == flag for f in LINKING_FLAGS + ('text_retrieval',)}

    @pytest.mark.parametrize('flag, fetches', [('--embedding-linking', True), ('--keep-verbatim-names', False),
                                               ('--split-entity-lines', False)])
    def test_cli_fetches_the_embedding_cache_only_when_needed(self, monkeypatch, tmp_path, flag, fetches):
        ran = []
        monkeypatch.setattr(runner, 'fetch_dataset', lambda *a: None)
        monkeypatch.setattr(runner, 'run_benchmark', lambda *a, **k: ran.append(k))
        runner.main(['--dataset', 'cwq', '--data-dir', str(tmp_path), '--s3-data', 's3://b/p/', flag])
        assert (ran[0]['before_embeddings'] is not None) is fetches


class TestEntityLinkingWiring:
    @pytest.fixture
    def spy(self, monkeypatch):
        seen = {'cache_dirs': [], 'matchers': [], 'split': []}

        def build_parts(graph, retrieval, region, cache_dir):
            seen['cache_dirs'].append(cache_dir)
            return runner.RetrievalParts(entity_matcher='MATCHER')

        def build(graph, model, region, sbm, parts=None, context_selector=None, split_entity_lines=False):
            seen['matchers'].append(parts.entity_matcher)
            seen['split'].append(split_entity_lines)
            return _engine()
        monkeypatch.setattr(runner, 'build_retrieval_parts', build_parts)
        monkeypatch.setattr(runner, 'build_engine', build)
        return seen

    def _run(self, tmp_path, **kw):
        data = tmp_path / 'data'
        data.mkdir()
        _dataset_on_disk(data)
        return runner.run_benchmark('webqsp', str(data), str(tmp_path / 'out'), runs=1, **kw)

    def test_options_reach_every_engine_without_touching_embeddings(self, tmp_path, spy):
        hooked = []
        report = self._run(tmp_path, retrieval=runner.RetrievalConfig(keep_verbatim_names=True,
                                                                      split_entity_lines=True),
                           before_embeddings=hooked.append, after_embeddings=hooked.append)
        assert spy == {'cache_dirs': [None], 'matchers': ['MATCHER', 'MATCHER'], 'split': [True, True]}
        assert hooked == [] and report['provenance']['retrieval']['embed_model'] is None

    def test_split_is_off_by_default(self, tmp_path, spy):
        self._run(tmp_path)
        assert spy['split'] == [False, False]


class TestBuildWithLinkingOptions:
    """The real build_retrieval_parts / build_engine; benchmarks CI does not install byokg-rag."""

    class Emb:
        def batch_embed(self, texts):
            return [[1.0, 0.0] for _ in texts]

        def embed_query(self, text):
            return [1.0, 0.0]

    @pytest.fixture
    def byokg(self, monkeypatch):
        pytest.importorskip('graphrag_toolkit.byokg_rag')
        from graphrag_toolkit.byokg_rag import config as byokg_config
        from graphrag_toolkit.byokg_rag.llm import bedrock_llms
        embedded = []
        monkeypatch.setattr(byokg_config.ByoKGConfig, 'to_embedding',
                            lambda **kw: embedded.append(kw) or self.Emb())
        monkeypatch.setattr(bedrock_llms, 'BedrockGenerator', lambda **kw: FakeGenerator())
        return embedded

    def test_verbatim_names_alone_builds_a_fuzzy_matcher_without_embeddings(self, byokg):
        parts = runner.build_retrieval_parts(GRAPH, runner.RetrievalConfig(keep_verbatim_names=True), None, None)
        index = parts.entity_matcher.index
        assert index.keep_verbatim_names and index.vocab == sorted(GRAPH)
        assert (parts.text_triplet_retriever, parts.embedding_dim, byokg) == (None, None, [])

    @pytest.mark.parametrize('keep', [False, True])
    def test_embedding_linking_fuses_the_configured_fuzzy_index(self, byokg, tmp_path, keep):
        from graphrag_toolkit.byokg_rag.indexing import HybridEntityMatcher
        parts = runner.build_retrieval_parts(GRAPH, runner.RetrievalConfig(
            embedding_linking=True, keep_verbatim_names=keep, embed_model='cohere.embed-english-v3'),
            None, str(tmp_path))
        assert isinstance(parts.entity_matcher, HybridEntityMatcher)
        assert parts.entity_matcher.fuzzy_index.keep_verbatim_names is keep
        assert parts.entity_matcher.node_table.texts == sorted(GRAPH)
        assert parts.text_triplet_retriever is None and parts.embedding_dim == 2

    def test_text_retrieval_alone_keeps_the_default_matcher(self, byokg, tmp_path):
        parts = runner.build_retrieval_parts(GRAPH, runner.RetrievalConfig(
            text_retrieval=True, embed_model='cohere.embed-english-v3'), None, str(tmp_path))
        assert parts.entity_matcher is None and parts.text_triplet_retriever is not None

    @pytest.mark.parametrize('linking', [False, True])
    def test_text_retrieval_combined_with_linking_options_builds_both(self, byokg, tmp_path, linking):
        from graphrag_toolkit.byokg_rag.indexing import HybridEntityMatcher
        parts = runner.build_retrieval_parts(GRAPH, runner.RetrievalConfig(
            text_retrieval=True, embedding_linking=linking, keep_verbatim_names=True,
            embed_model='cohere.embed-english-v3'), None, str(tmp_path))
        assert parts.text_triplet_retriever is not None
        assert isinstance(parts.entity_matcher, HybridEntityMatcher) is linking
        index = parts.entity_matcher.fuzzy_index if linking else parts.entity_matcher.index
        assert index.keep_verbatim_names

    def test_engine_defaults_are_the_library_defaults(self, byokg):
        engine, counter = runner.build_engine(GRAPH, None, None, False)
        assert engine.entity_linker.retriever.index.keep_verbatim_names is False
        assert engine.kg_linker.split_entity_lines is False

    def test_engine_takes_the_options_under_test(self, byokg):
        from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex
        matcher = FuzzyStringIndex(keep_verbatim_names=True).as_entity_matcher()
        engine, counter = runner.build_engine(GRAPH, None, None, True, runner.RetrievalParts(entity_matcher=matcher),
                                              split_entity_lines=True)
        assert engine.entity_linker.retriever is matcher
        # The linker's LLM calls are counted like the default linker's.
        assert engine.kg_linker.split_entity_lines is True and engine.kg_linker.llm_generator is counter


class FakeS3:
    """boto3 S3 client surface used by sync_embedding_cache: upload_file(Filename, Bucket,
    Key), download_file(Bucket, Key, Filename), and list_objects_v2 pages that omit
    'Contents' when nothing matches."""

    def __init__(self, objects=None, page_size=2):
        self.objects = dict(objects or {})  # (bucket, key) -> bytes
        self.page_size, self.ops = page_size, []

    def upload_file(self, Filename, Bucket, Key):
        with open(Filename, 'rb') as f:
            self.objects[(Bucket, Key)] = f.read()
        self.ops.append(('up', Key))

    def download_file(self, Bucket, Key, Filename):
        with open(Filename, 'wb') as f:
            f.write(self.objects[(Bucket, Key)])
        self.ops.append(('down', Key))

    def get_paginator(self, name):
        assert name == 'list_objects_v2'
        return self

    def paginate(self, Bucket, Prefix):
        keys = sorted(k for b, k in self.objects if b == Bucket and k.startswith(Prefix))
        if not keys:
            yield {'KeyCount': 0, 'IsTruncated': False}
        for i in range(0, len(keys), self.page_size):
            yield {'Contents': [{'Key': k} for k in keys[i:i + self.page_size]],
                   'KeyCount': len(keys[i:i + self.page_size])}


CACHE_FILES = ['nodes/meta.json', 'nodes/texts.json', 'nodes/vectors.npy',
               'relations/meta.json', 'relations/texts.json', 'relations/vectors.npy']


def _write_cache(cache_dir, partial=True, tables=('nodes', 'relations')):
    rels = [r for r in CACHE_FILES if r.split('/')[0] in tables]
    for rel in rels + (['nodes/partial/000000-abcd.npy'] if partial else []):
        path = os.path.join(cache_dir, *rel.split('/'))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(rel.encode())


class TestSyncEmbeddingCache:
    PREFIX = 'byokg-data/embeddings/k1/'

    def test_upload_skips_checkpoints_and_puts_vectors_last(self, tmp_path, monkeypatch):
        s3 = FakeS3()
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        _write_cache(str(tmp_path / 'k1'))
        runner.sync_embedding_cache('s3://bucket/byokg-data/', str(tmp_path / 'k1'), upload=True)
        keys = [k for _, k in s3.ops]
        assert sorted(keys) == sorted(self.PREFIX + r for r in CACHE_FILES), 'no partial/ checkpoints'
        assert all(k.endswith('vectors.npy') for k in keys[-2:]), 'completion markers last'

    def test_download_round_trips_across_pages_with_vectors_last(self, tmp_path, monkeypatch):
        s3 = FakeS3({('bucket', self.PREFIX + r): r.encode() for r in CACHE_FILES}, page_size=2)
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        runner.sync_embedding_cache('s3://bucket/byokg-data', str(tmp_path / 'k1'), upload=False)
        for rel in CACHE_FILES:
            assert (tmp_path / 'k1' / rel).read_bytes() == rel.encode()
        assert all(k.endswith('vectors.npy') for _, k in s3.ops[-2:])

    def test_empty_prefix_fetches_nothing(self, tmp_path, monkeypatch):
        s3 = FakeS3({('bucket', 'byokg-data/embeddings/other/nodes/vectors.npy'): b'x'})
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        runner.sync_embedding_cache('s3://bucket/byokg-data/', str(tmp_path / 'k1'), upload=False)
        assert s3.ops == [] and not (tmp_path / 'k1').exists()

    def test_folder_markers_are_skipped(self, tmp_path, monkeypatch):
        s3 = FakeS3({('bucket', self.PREFIX): b'', ('bucket', self.PREFIX + 'nodes/'): b'',
                     ('bucket', self.PREFIX + 'nodes/texts.json'): b'[]'})
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        runner.sync_embedding_cache('s3://bucket/byokg-data/', str(tmp_path / 'k1'), upload=False)
        assert s3.ops == [('down', self.PREFIX + 'nodes/texts.json')]

    def test_key_outside_the_cache_dir_fails_loud(self, tmp_path, monkeypatch):
        s3 = FakeS3({('bucket', self.PREFIX + 'nodes/texts.json'): b'[]',
                     ('bucket', self.PREFIX + '../../escape.txt'): b'x'})
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        with pytest.raises(ValueError, match='resolves outside'):
            runner.sync_embedding_cache('s3://bucket/byokg-data/', str(tmp_path / 'k1'), upload=False)
        assert s3.ops == [], 'nothing fetched'
        assert not (tmp_path / 'escape.txt').exists()

    def test_complete_local_cache_is_not_fetched_again(self, tmp_path, monkeypatch):
        s3 = FakeS3({('bucket', self.PREFIX + r): b'remote' for r in CACHE_FILES})
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        _write_cache(str(tmp_path / 'k1'), partial=False)
        assert runner.fetch_embedding_cache('s3://bucket/byokg-data/', str(tmp_path / 'k1'))
        assert s3.ops == []

    def test_non_s3_uri_fails_loud(self, tmp_path, monkeypatch):
        monkeypatch.setattr(runner, '_s3_client', lambda: FakeS3())
        with pytest.raises(ValueError, match='expected an s3:// URI'):
            runner.sync_embedding_cache('/local/byokg-data', str(tmp_path / 'k1'), upload=False)


class TestEmbeddingCacheOnStack:
    @pytest.fixture
    def stack(self, tmp_path, monkeypatch, fake_engine):
        data = tmp_path / 'data'
        data.mkdir()
        _dataset_on_disk(data)
        for var, value in {'BYOKG_DATA_S3_URI': 's3://bucket/byokg-data/', 'BYOKG_DATA_DIR': str(data),
                           'BYOKG_KGQA_RUNS': '1', 'BYOKG_TEXT_RETRIEVAL': 'true',
                           'BYOKG_EMBED_MODEL': 'cohere.embed-english-v3'}.items():
            monkeypatch.setenv(var, value)
        monkeypatch.setattr(runner, 'RESULTS_DIR', str(tmp_path / 'results'))
        monkeypatch.setattr(runner, 'upload_benchmark_results_to_s3', lambda *a: None)
        s3 = FakeS3()
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        built = []

        def build_parts(graph, retrieval, region, cache_dir):
            # Mirrors the real one: off builds nothing; a miss leaves a full cache plus checkpoints.
            if not retrieval.needs_embeddings:
                return runner.RetrievalParts()
            missing = [t for t in ('nodes', 'relations')
                       if not os.path.exists(os.path.join(cache_dir, t, 'vectors.npy'))]
            if missing:
                _write_cache(cache_dir, tables=missing)
            built.append(missing)
            return runner.RetrievalParts(embedding_dim=1024)
        monkeypatch.setattr(runner, 'build_retrieval_parts', build_parts)
        key = runner.embedding_cache_key(
            runner._file_sha256(str(data / runner.DATASETS['webqsp'].graph_file)), 'cohere.embed-english-v3')
        return s3, built, f'byokg-data/embeddings/{key}/'

    def test_miss_builds_then_publishes_once(self, stack):
        s3, built, prefix = stack
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert built == [['nodes', 'relations']]
        assert sorted(k for _, k in s3.objects) == sorted(prefix + r for r in CACHE_FILES)

    def test_incomplete_remote_cache_is_completed_and_republished(self, stack):
        s3, built, prefix = stack
        # A stack that died between the two tables' uploads left relations without vectors.
        s3.objects = {('bucket', prefix + r): r.encode() for r in CACHE_FILES if r != 'relations/vectors.npy'}
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert built == [['relations']], 'only the missing table is rebuilt'
        assert ('up', prefix + 'relations/vectors.npy') in s3.ops

    def test_second_stack_reuses_the_published_cache(self, stack, tmp_path):
        s3, built, prefix = stack
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        import shutil
        shutil.rmtree(tmp_path / 'data' / 'embeddings')  # a fresh stack's volume
        s3.ops.clear()
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert s3.ops and all(op == 'down' for op, _ in s3.ops), 'fetched, not republished'
        assert sorted(k for _, k in s3.ops) == sorted(prefix + r for r in CACHE_FILES)

    def test_off_never_touches_the_cache(self, stack, monkeypatch):
        s3, built, _ = stack
        monkeypatch.setenv('BYOKG_TEXT_RETRIEVAL', 'false')
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert s3.ops == [] and built == []

    def test_embedding_linking_alone_builds_and_publishes_the_same_cache(self, stack, monkeypatch):
        s3, built, prefix = stack
        monkeypatch.setenv('BYOKG_TEXT_RETRIEVAL', 'false')
        monkeypatch.setenv('BYOKG_EMBEDDING_LINKING', 'true')
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert built == [['nodes', 'relations']]
        assert sorted(k for _, k in s3.objects) == sorted(prefix + r for r in CACHE_FILES)

    def test_linking_options_without_embeddings_never_touch_the_cache(self, stack, monkeypatch):
        s3, built, _ = stack
        monkeypatch.setenv('BYOKG_TEXT_RETRIEVAL', 'false')
        monkeypatch.setenv('BYOKG_KEEP_VERBATIM_NAMES', 'true')
        monkeypatch.setenv('BYOKG_SPLIT_ENTITY_LINES', 'true')
        runner.run_byokg_kgqa_test(FakeHandler(), {}, 'webqsp')
        assert s3.ops == [] and built == []


class TestReviewFixes:
    @pytest.mark.parametrize('topk', [0, -3, 2.5])
    def test_text_topk_must_be_positive(self, topk):
        with pytest.raises(ValueError, match='text_topk must be a positive integer'):
            runner.RetrievalConfig(text_retrieval=True, text_topk=topk)

    def test_cli_fetches_the_embedding_cache_with_s3_data(self, monkeypatch, tmp_path):
        key = 'k1-cohere.embed-english-v3'
        s3 = FakeS3({('bucket', f'byokg-data/embeddings/{key}/nodes/vectors.npy'): b'v'})
        ran = []
        monkeypatch.setattr(runner, '_s3_client', lambda: s3)
        monkeypatch.setattr(runner, 'fetch_dataset', lambda *a: None)
        monkeypatch.setattr(runner, 'run_benchmark', lambda *a, **k: ran.append(k))
        runner.main(['--dataset', 'cronquestions', '--data-dir', str(tmp_path), '--s3-data',
                     's3://bucket/byokg-data/', '--text-retrieval', '--embed-model', 'cohere.embed-english-v3'])
        assert ran[0]['retrieval'].text_retrieval
        ran[0]['before_embeddings'](str(tmp_path / 'embeddings' / key))  # as run_benchmark calls it
        assert (tmp_path / 'embeddings' / key / 'nodes' / 'vectors.npy').read_bytes() == b'v'

    @pytest.mark.parametrize('argv', [['--s3-data', 's3://b/p/'], ['--text-retrieval']])
    def test_cli_fetches_only_with_s3_data_and_a_flag(self, monkeypatch, tmp_path, argv):
        ran = []
        monkeypatch.setattr(runner, 'fetch_dataset', lambda *a: None)
        monkeypatch.setattr(runner, 'run_benchmark', lambda *a, **k: ran.append(k))
        runner.main(['--dataset', 'cronquestions', '--data-dir', str(tmp_path), *argv])
        assert ran[0]['before_embeddings'] is None

    def test_build_parts_records_model_and_retries(self, monkeypatch, tmp_path):
        pytest.importorskip('graphrag_toolkit.byokg_rag')  # benchmarks CI does not install it
        from graphrag_toolkit.byokg_rag import config as byokg_config
        from graphrag_toolkit.byokg_rag.indexing import graph_embeddings
        seen = {}

        class Emb:
            def batch_embed(self, texts):
                return [[1.0, 0.0] for _ in texts]

            def embed_query(self, text):
                return [1.0, 0.0]
        monkeypatch.setattr(byokg_config.ByoKGConfig, 'to_embedding', lambda **kw: seen.update(kw) or Emb())
        real_build = graph_embeddings.GraphEmbeddings.build.__func__
        monkeypatch.setattr(graph_embeddings.GraphEmbeddings, 'build', classmethod(
            lambda cls, *a, **kw: seen.update(model_id=kw.get('model_id')) or real_build(cls, *a, **kw)))
        parts = runner.build_retrieval_parts(GRAPH, runner.RetrievalConfig(
            text_retrieval=True, embed_model='cohere.embed-english-v3'), 'us-west-2', str(tmp_path))
        assert seen['model_id'] == 'cohere.embed-english-v3' and seen['region_name'] == 'us-west-2'
        assert seen['config'].retries == {'max_attempts': runner.EMBED_MAX_ATTEMPTS, 'mode': 'adaptive'}
        assert parts.text_triplet_retriever is not None
        assert parts.embedding_dim == 2


# --- optional answer selection (context reranking + grounded answer prompt) ---------

PROMPT = 'grounded: {question} | {graph_context} | {user_input}'


class TestAnswerSelectionConfig:
    def test_off_by_default_with_a_stable_record(self):
        off = runner.AnswerSelectionConfig()
        assert not off.enabled
        assert runner.answer_selection_provenance(off, None) == {
            'enabled': False, 'max_lines': None, 'reranker': None, 'prompt': None}
        assert runner.answer_selection_provenance(runner.AnswerSelectionConfig(max_lines=5), 'x') == \
            runner.answer_selection_provenance(off, None), 'unused settings never split the fingerprint'

    @pytest.mark.parametrize('max_lines', [0, -2, 1.5])
    def test_max_lines_must_be_positive(self, max_lines):
        with pytest.raises(ValueError, match='max_lines must be a positive integer'):
            runner.AnswerSelectionConfig(enabled=True, max_lines=max_lines)

    def test_from_env_and_cli(self, monkeypatch):
        monkeypatch.setenv('BYOKG_ANSWER_SELECTION', 'true')
        monkeypatch.setenv('BYOKG_CONTEXT_MAX_LINES', '12')
        assert runner.answer_selection_from_env() == runner.AnswerSelectionConfig(enabled=True, max_lines=12)
        a = runner.parse_args(['--dataset', 'cwq', '--answer-selection', '--context-max-lines', '9'])
        assert (a.answer_selection, a.context_max_lines) == (True, 9)

    def test_build_parts_off_loads_nothing(self):
        assert runner.build_answer_parts(runner.AnswerSelectionConfig()) == runner.AnswerParts()

    def test_build_parts_on_loads_the_reranker_once_and_the_grounded_prompt(self, monkeypatch):
        pytest.importorskip('graphrag_toolkit.byokg_rag')  # benchmarks CI does not install it
        from graphrag_toolkit.byokg_rag import graph_retrievers
        made = []

        class FakeReranker:
            model_name = 'BAAI/bge-reranker-base'

            def __init__(self, device):
                made.append(device)
        monkeypatch.setattr(graph_retrievers, 'LocalGReranker', FakeReranker)
        parts = runner.build_answer_parts(runner.AnswerSelectionConfig(enabled=True, max_lines=7))
        assert len(made) == 1 and made[0] in ('cpu', 'cuda')
        assert parts.context_selector.max_lines == 7 and parts.reranker_model == 'BAAI/bge-reranker-base'
        assert '{graph_context}' in parts.prompt and 'exactly as it is written in the graph context' in parts.prompt


class TestAnswerSelectionWiring:
    @pytest.fixture
    def spy(self, monkeypatch):
        engines, selectors = [], []

        def build(graph, model, region, sbm, parts=None, context_selector=None, split_entity_lines=False):
            engine, counter = _engine()
            engines.append(engine)
            selectors.append(context_selector)
            return engine, counter
        monkeypatch.setattr(runner, 'build_engine', build)
        monkeypatch.setattr(runner, 'build_answer_parts', lambda cfg: runner.AnswerParts(
            context_selector='SELECTOR', prompt=PROMPT, reranker_model='BAAI/bge-reranker-base')
            if cfg.enabled else runner.AnswerParts())
        return engines, selectors

    def _run(self, tmp_path, **kw):
        data = tmp_path / 'data'
        if not data.exists():
            data.mkdir()
            _dataset_on_disk(data)
        return runner.run_benchmark('webqsp', str(data), str(tmp_path / 'out'), runs=1, **kw)

    def test_on_reaches_every_engine_and_only_the_kg_answer(self, tmp_path, spy):
        engines, selectors = spy
        report = self._run(tmp_path, answer_selection=runner.AnswerSelectionConfig(enabled=True))
        assert selectors == ['SELECTOR', 'SELECTOR'], 'both settings, one reranker'
        # generate_response() order per question: the KG answer, then the LLM-only answer.
        assert all(e.prompts == [PROMPT, None] for e in engines)
        assert report['provenance']['answer_selection'] == {
            'enabled': True, 'max_lines': 20, 'reranker': 'BAAI/bge-reranker-base',
            'prompt': 'generate-response-qa-grounded'}

    def test_off_passes_nothing(self, tmp_path, spy):
        engines, selectors = spy
        self._run(tmp_path)
        assert selectors == [None, None] and all(e.prompts == [None, None] for e in engines)

    def test_switching_it_on_refuses_the_baseline_results(self, tmp_path, spy):
        self._run(tmp_path)
        with pytest.raises(RuntimeError, match='different configuration'):
            self._run(tmp_path, answer_selection=runner.AnswerSelectionConfig(enabled=True))
