# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os

import pytest

from benchmarks.scripts import judge_byokg_kgqa as cli
from benchmarks.utils import byokg_judge as bj

anthropic_types = pytest.importorskip('anthropic.types')  # installed with lexical-graph


def _message(text=None, stop='end_turn', stop_details=None, usage=(320, 140)):
    """A Messages response as the SDK parses it: Opus 5.5 always thinks, so a thinking
    block (its text omitted by default) comes before the structured-output text."""
    content = [{'type': 'thinking', 'thinking': '', 'signature': 'c2ln'}]
    if text is not None:
        content.append({'type': 'text', 'text': text})
    payload = {'id': 'msg_01', 'type': 'message', 'role': 'assistant', 'model': 'claude-opus-5-5',
               'content': content, 'stop_reason': stop, 'stop_sequence': None,
               'usage': {'input_tokens': usage[0], 'output_tokens': usage[1]}}
    if stop_details:
        payload['stop_details'] = stop_details
    return anthropic_types.Message.model_validate(payload)


def _verdict(correct=True, gold='Australia'):
    return json.dumps({'verdict': 'correct' if correct else 'incorrect',
                       'matched_gold': gold if correct else '', 'explanation': 'Same country.'})


class FakeClient:
    """anthropic.AnthropicBedrock's messages.create, answering by the graded answers."""

    def __init__(self, decide=lambda prompt: True, fail_with=None):
        self.decide, self.fail_with, self.requests = decide, fail_with, []
        self.messages = self

    def create(self, **kwargs):
        self.requests.append(kwargs)
        if self.fail_with:
            return self.fail_with
        return _message(_verdict(self.decide(kwargs['messages'][0]['content'])))


RECORD = {'id': 'WebQTest-1', 'question': 'what country is the buffyverse set in',
          'answers': ['United States of America'], 'predictions': ['USA', 'Canada'],
          'vanilla_predictions': [], 'error': None, 'hit': 1, 'hit@2': 1}  # stored scores are stale; never read
MEDQA = {'id': 'medqa-7', 'question': 'Which drug treats X?', 'answers': ['Metformin'],
         'predictions': ['Insulin', 'Metformin'], 'vanilla_predictions': ['metformin hydrochloride'], 'error': None}


class TestUnits:
    def test_roles_and_empty_lists(self):
        units = bj.record_units('webqsp', RECORD)
        assert set(units) == {bj.KG, bj.VANILLA} and units[bj.VANILLA] is None
        assert units[bj.KG].answers == ('USA', 'Canada') and units[bj.KG].gold == ('United States of America',)

    def test_hit_at_2_judges_each_first_answer(self):
        units = bj.record_units('medqa', MEDQA)
        assert units[bj.KG_FIRST].answers == ('Insulin',) and units[bj.VANILLA_FIRST].answers == ('metformin hydrochloride',)

    def test_key_changes_with_anything_that_changes_the_verdict(self):
        unit = bj.record_units('webqsp', RECORD)[bj.KG]
        keys = {unit.key('m', 'low'), unit.key('m', 'medium'), unit.key('other', 'low'),
                bj.Unit(unit.dataset, unit.question_id, unit.question, unit.gold, ('USA',)).key('m', 'low')}
        assert len(keys) == 4

    def test_prompt_lists_are_json(self):
        assert bj.record_units('webqsp', RECORD)[bj.KG].prompt() == (
            'Question: what country is the buffyverse set in\n'
            'Gold answers: ["United States of America"]\nAnswers to grade: ["USA", "Canada"]')


class TestParse:
    def test_ok(self):
        v = bj.parse_verdict(_message(_verdict()))
        assert (v['status'], v['correct'], v['matched_gold'], v['usage']) == ('ok', True, 'Australia', {'input': 320, 'output': 140})

    def test_refusal_is_counted_not_scored(self):
        v = bj.parse_verdict(_message(stop='refusal', stop_details={'type': 'refusal', 'category': 'bio', 'explanation': None}))
        assert (v['status'], v['correct'], v['stop_category']) == ('refusal', None, 'bio')

    def test_truncated_and_invalid(self):
        assert bj.parse_verdict(_message('{"verdict": "corr', stop='max_tokens'))['status'] == 'truncated'
        assert bj.parse_verdict(_message('{"verdict": "maybe", "matched_gold": "", "explanation": ""}'))['status'] == 'invalid'
        assert bj.parse_verdict(_message(None))['status'] == 'invalid'


class TestJudge:
    def test_request_shape_and_caching(self, tmp_path):
        client = FakeClient()
        judge = bj.Judge(client, bj.JudgeCache(str(tmp_path / 'cache.jsonl')))
        unit = bj.record_units('webqsp', RECORD)[bj.KG]
        first = judge.judge(unit)
        assert judge.judge(unit) == first and len(client.requests) == 1, 'cached'
        request = client.requests[0]
        assert request['model'] == 'global.anthropic.claude-opus-5-5' and request['system'] == bj.SYSTEM_PROMPT
        assert request['output_config'] == {'effort': 'low', 'format': {'type': 'json_schema', 'schema': bj.VERDICT_SCHEMA}}
        assert not {'temperature', 'top_p', 'top_k', 'thinking'} & set(request), 'Opus 5.5 rejects sampling parameters'
        # A new process reads the same file.
        again = bj.Judge(FakeClient(), bj.JudgeCache(str(tmp_path / 'cache.jsonl')))
        assert again.cached(unit) == first

    def test_scores_mirror_the_string_metrics(self):
        ok = {'status': 'ok', 'correct': True}
        row = bj.score_record('webqsp', RECORD, {bj.KG: ok, bj.VANILLA: None})
        assert (row['judge_hit'], row['judge_vanilla_hit'], row['hit']) == (1, 0, 0), 'string Hit recomputed, not read'
        refused = bj.score_record('webqsp', RECORD, {bj.KG: {'status': 'refusal', 'correct': None}, bj.VANILLA: None})
        assert refused['judge_hit'] is None

    def test_hit_at_2_and_errored_records(self):
        no, yes = {'status': 'ok', 'correct': False}, {'status': 'ok', 'correct': True}
        verdicts = {bj.KG: yes, bj.VANILLA: yes, bj.KG_FIRST: no, bj.VANILLA_FIRST: yes}
        assert bj.score_record('medqa', MEDQA, verdicts)['judge_hit@2'] == 1
        assert bj.score_record('medqa', {**MEDQA, 'error': 'Boom'}, verdicts)['judge_hit@2'] == 0

    def test_summary_counts_disagreements_and_unscored(self):
        rows = [{'judge_hit': 1, 'hit': 0, 'judge_vanilla_hit': 0, 'vanilla_hit': 0, 'f1': 0.0,
                 'judge': {bj.KG: {'status': 'ok'}, bj.VANILLA: None}},
                {'judge_hit': None, 'hit': 1, 'judge_vanilla_hit': 1, 'vanilla_hit': 1, 'f1': 1.0,
                 'judge': {bj.KG: {'status': 'refusal'}, bj.VANILLA: {'status': 'ok'}}}]
        s = bj.summarize_judged('webqsp', rows, {'name': bj.JUDGE_NAME})
        assert (s['judge_hit'], s['judge_only'], s['string_only'], s['agreement']) == (100.0, 1, 0, 0.0)
        assert s['n_unscored'] == {'judge_hit': 1, 'judge_vanilla_hit': 0}
        assert s['statuses'] == {'ok': 2, 'refusal': 1}


def _results(tmp_path, records=(RECORD,), dataset='webqsp', settings=('single_best_match-on', 'single_best_match-off')):
    for setting in settings:
        run = tmp_path / 'byokg-kgqa' / dataset / setting / 'run1'
        run.mkdir(parents=True)
        (run / 'predictions.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in records))
    return tmp_path


class TestCli:
    def test_dry_run_counts_distinct_lists_once(self, tmp_path, capsys):
        root = _results(tmp_path)
        assert cli.main(['--results', str(root), '--dry-run'], client=FakeClient()) == 0
        assert json.loads(capsys.readouterr().out)['calls'] == 1, 'the same list in two settings is one call'
        assert not os.path.exists(root / 'judge_cache.jsonl')

    def test_full_run_writes_per_run_scores_and_the_aggregate(self, tmp_path):
        root = _results(tmp_path)
        client = FakeClient(decide=lambda prompt: 'USA' in prompt)
        assert cli.main(['--results', str(root)], client=client) == 0
        assert len(client.requests) == 1
        run = root / 'byokg-kgqa' / 'webqsp' / 'single_best_match-on' / 'run1'
        row = json.loads((run / 'judge.jsonl').read_text())
        assert (row['judge_hit'], row['hit']) == (1, 0)
        summary = json.loads((run / 'judge_summary.json').read_text())
        assert summary['judge'] == {'name': 'Claude Opus 5.5', 'model_id': 'global.anthropic.claude-opus-5-5',
                                    'effort': 'low', 'rubric_version': bj.RUBRIC_VERSION}
        agg = json.loads((root / 'judge_aggregate.json').read_text())
        assert agg['settings']['byokg-kgqa/webqsp/single_best_match-on']['mean'] == 100.0
        # A rerun pays for nothing.
        assert cli.main(['--results', str(root)], client=FakeClient(fail_with=AssertionError)) == 0

    def test_limit_is_a_smoke_run(self, tmp_path, capsys):
        records = [{**RECORD, 'id': f'q{i}', 'predictions': [f'answer {i}']} for i in range(5)]
        root = _results(tmp_path, records=records, settings=('single_best_match-on',))
        client = FakeClient()
        assert cli.main(['--results', str(root), '--limit', '2'], client=client) == 0
        assert len(client.requests) == 2 and json.loads(capsys.readouterr().out)['ok'] == 2
        assert not list(root.rglob('judge.jsonl')), 'no scores from a partial judgment'

    def test_too_many_unscored_verdicts_stop_the_run(self, tmp_path):
        records = [{**RECORD, 'id': f'q{i}', 'predictions': [f'answer {i}']} for i in range(50)]
        root = _results(tmp_path, records=records, settings=('single_best_match-on',))
        refusal = _message(stop='refusal', stop_details={'type': 'refusal', 'category': 'bio', 'explanation': None})
        with pytest.raises(RuntimeError, match='got no verdict'):
            cli.main(['--results', str(root), '--workers', '1'], client=FakeClient(fail_with=refusal))

    def test_unexpected_layout_fails_loud(self, tmp_path):
        bad = tmp_path / 'somewhere' / 'run1'
        bad.mkdir(parents=True)
        (bad / 'predictions.jsonl').write_text(json.dumps(RECORD) + '\n')
        with pytest.raises(ValueError, match='expected <dataset>/<setting>/<run>'):
            cli.main(['--results', str(tmp_path), '--dry-run'])


class TestReviewFixes:
    def test_hit_at_2_is_an_or_over_the_first_answers_it_could_judge(self):
        hit_, miss, refused = {'status': 'ok', 'correct': True}, {'status': 'ok', 'correct': False}, {'status': 'refusal'}
        def h2(kg_first, vanilla_first):
            return bj.score_record('medqa', MEDQA, {bj.KG: miss, bj.VANILLA: miss, bj.KG_FIRST: kg_first,
                                                     bj.VANILLA_FIRST: vanilla_first})['judge_hit@2']
        assert (h2(hit_, refused), h2(refused, hit_), h2(miss, refused), h2(miss, miss)) == (1, 1, None, 0)

    def test_unscored_are_counted_for_their_own_metric(self):
        rows = [bj.score_record('medqa', MEDQA, {bj.KG: {'status': 'ok', 'correct': True}, bj.VANILLA: {'status': 'refusal'},
                                                 bj.KG_FIRST: {'status': 'ok', 'correct': False},
                                                 bj.VANILLA_FIRST: {'status': 'refusal'}})]
        assert bj.summarize_judged('medqa', rows, {})['n_unscored'] == {
            'judge_hit': 0, 'judge_vanilla_hit': 1, 'judge_hit@2': 1}

    def test_key_hashes_the_prompts_gold_and_question(self, monkeypatch):
        unit = bj.record_units('webqsp', RECORD)[bj.KG]
        base = unit.key('m', 'low')
        variants = [bj.Unit(unit.dataset, unit.question_id, unit.question, ('Canada',), unit.answers),
                    bj.Unit(unit.dataset, unit.question_id, 'another question', unit.gold, unit.answers)]
        assert all(v.key('m', 'low') != base for v in variants)
        monkeypatch.setattr(bj, 'SYSTEM_PROMPT', bj.SYSTEM_PROMPT + ' Be strict.')
        assert unit.key('m', 'low') != base, 'a rubric edit never reuses old verdicts'
        rubric = unit.key('m', 'low')
        monkeypatch.setattr(bj, 'VERDICT_SCHEMA', {**bj.VERDICT_SCHEMA, 'required': ['verdict']})
        assert unit.key('m', 'low') != rubric, 'nor does a schema edit'

    def test_stacks_with_the_same_dataset_and_setting_are_kept_apart(self, tmp_path):
        for stack, answer in (('armA', 'USA'), ('armB', 'Canada')):
            run = tmp_path / stack / 'webqsp' / 'single_best_match-on' / 'run1'
            run.mkdir(parents=True)
            (run / 'predictions.jsonl').write_text(json.dumps({**RECORD, 'predictions': [answer]}) + '\n')
        assert cli.main(['--results', str(tmp_path)], client=FakeClient(decide=lambda p: 'USA' in p)) == 0
        agg = json.loads((tmp_path / 'judge_aggregate.json').read_text())['settings']
        assert (agg['armA/webqsp/single_best_match-on']['mean'], agg['armB/webqsp/single_best_match-on']['mean']) == (
            100.0, 0.0)

    def test_a_run_missing_verdicts_fails_the_scoring_unless_allowed(self, tmp_path):
        records = [{**RECORD, 'id': f'q{i}', 'predictions': [f'answer {i}']} for i in range(10)]
        root = _results(tmp_path, records=records, settings=('single_best_match-on',))
        refuse_one = _message(stop='refusal', stop_details={'type': 'refusal', 'category': 'bio', 'explanation': None})

        class OneRefusal(FakeClient):
            def create(self, **kwargs):
                self.requests.append(kwargs)
                return refuse_one if 'answer 3' in kwargs['messages'][0]['content'] else _message(_verdict())
        with pytest.raises(RuntimeError, match=r'1/10'):  # 10% unscored; too few new units for the early stop
            cli.main(['--results', str(root)], client=OneRefusal())
        assert cli.main(['--results', str(root), '--allow-unscored'], client=FakeClient()) == 0

    @staticmethod
    def _refusing(refused, correct):
        """Refuses lists named in refused; otherwise grades by whether correct is among the answers."""
        refusal = _message(stop='refusal', stop_details={'type': 'refusal', 'category': 'bio', 'explanation': None})

        class Client(FakeClient):
            def create(self, **kwargs):
                self.requests.append(kwargs)
                graded = kwargs['messages'][0]['content'].split('Answers to grade: ')[1]
                return refusal if graded in refused else _message(_verdict(f'"{correct}"' in graded))
        return Client()

    def test_every_judged_score_is_gated_not_only_the_headline(self, tmp_path):
        records = [{**RECORD, 'id': f'q{i}', 'predictions': ['USA'], 'vanilla_predictions': [f'v{i}']} for i in range(10)]
        root = _results(tmp_path, records=records, settings=('single_best_match-on',))
        client = self._refusing({'["v0"]', '["v1"]'}, 'USA')
        with pytest.raises(RuntimeError, match=r'judge_vanilla_hit 2/10'):
            cli.main(['--results', str(root)], client=client)
        run = root / 'byokg-kgqa' / 'webqsp' / 'single_best_match-on' / 'run1'
        assert (run / 'judge.jsonl').exists(), 'the files are written before the gate fails'
        assert json.loads((run / 'judge_summary.json').read_text())['gate'] == {
            'max_unscored_share': cli.MAX_UNSCORED_SHARE, 'passed': False, 'over': ['judge_vanilla_hit']}
        agg = json.loads((root / 'judge_aggregate.json').read_text())['settings']['byokg-kgqa/webqsp/single_best_match-on']
        assert (agg['mean'], agg['n_unscored'], agg['gate_passed']) == (
            100.0, {'judge_hit': 0, 'judge_vanilla_hit': 2}, False)
        # A rerun reads the cached refusals and still fails.
        with pytest.raises(RuntimeError, match=r'judge_vanilla_hit 2/10'):
            cli.main(['--results', str(root)], client=FakeClient(fail_with=AssertionError))

    def test_medqa_hit_at_2_is_gated(self, tmp_path):
        records = [{**MEDQA, 'id': f'm{i}', 'predictions': [f'wrong {i}', 'Metformin'],
                    'vanilla_predictions': [f'other {i}']} for i in range(10)]
        root = _results(tmp_path, records=records, dataset='medqa', settings=('single_best_match-on',))
        client = self._refusing({'["wrong 0"]', '["wrong 1"]'}, 'Metformin')
        with pytest.raises(RuntimeError, match=r'judge_hit@2 2/10'):
            cli.main(['--results', str(root)], client=client)
        agg = json.loads((root / 'judge_aggregate.json').read_text())['settings']['byokg-kgqa/medqa/single_best_match-on']
        assert agg['metric'] == 'judge_hit@2' and agg['n_unscored'] == {
            'judge_hit': 0, 'judge_vanilla_hit': 0, 'judge_hit@2': 2}
        assert cli.main(['--results', str(root), '--allow-unscored'], client=FakeClient(fail_with=AssertionError)) == 0

    def test_truncated_verdicts_are_not_cached_so_a_rerun_retries_them(self, tmp_path):
        root = _results(tmp_path, settings=('single_best_match-on',))
        truncated = FakeClient(fail_with=_message('{"verdict": "corr', stop='max_tokens'))
        with pytest.raises(RuntimeError, match='no verdict'):
            cli.main(['--results', str(root)], client=truncated)
        retry = FakeClient()
        assert cli.main(['--results', str(root), '--max-tokens', '8192'], client=retry) == 0
        assert len(retry.requests) == 1 and retry.requests[0]['max_tokens'] == 8192
