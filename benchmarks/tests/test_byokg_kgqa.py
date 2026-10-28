# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from benchmarks.utils.byokg_kgqa import (
    DATASETS, Question, _quantile, build_schema, f1, filter_answer_in_kg, hit, hit_at_2,
    hop_distance, validate_graph,
    load_questions, normalize, sample, showcase_reasons, strip_choices, summarize,
)


def _graph(*triplets):
    """LocalKGStore format: {head: {relation: {"triplets": [[h, r, t]]}}}."""
    g = {}
    for h, r, t in triplets:
        g.setdefault(h, {}).setdefault(r, {'triplets': []})['triplets'].append([h, r, t])
        g.setdefault(t, {})
    return g


# Field shapes as observed in the delivered files; values are synthetic.
WEBQSP_RECORD = {
    'id': 'WebQTest-0', 'question': 'what does jamaican people speak',
    'answer': ['Jamaican English', 'Jamaican Creole English Language'],
    'q_entity': ['Jamaica'], 'a_entity': ['Jamaican English', 'Jamaican Creole English Language'],
    'graph': [['Jamaica', 'location.country.languages_spoken', 'Jamaican English']],
    'choices': [],
    'current_schema': {'graphSummary': {'edgeLabels': ['location.country.languages_spoken']}},
}
CRON_RECORD = {'answer': ['2010'], 'question': 'When was the first time X played for Y',
               'q_entity': ['X', 'Y'], 'type': 'first_last'}
MEDQA_RECORD = {'question': 'A 65-year-old man has chest pain. Which drug should be added?'
                            '\nA. Nifedipine\nB. Enoxaparin\nC. Clopidogrel\nD. Spironolactone',
                'answer': 'Clopidogrel'}


def _write(tmp_path, dataset, records):
    (tmp_path / DATASETS[dataset].questions_file).write_text(json.dumps(records))
    return str(tmp_path)


class TestLoading:
    def test_strip_choices_removes_trailing_options_only(self):
        assert strip_choices(MEDQA_RECORD['question']) == \
            'A 65-year-old man has chest pain. Which drug should be added?'

    def test_strip_choices_handles_quote_wrapped_record(self):
        # Shape of 7 delivered records: wrapped in quotes, closing quote on its own line.
        wrapped = '"A woman has hemolysis. Which is the most likely diagnosis?"\n' \
                  'A. HELLP syndrome\nB. TTP\nC. HUS\nD. AIHA\n"'
        assert strip_choices(wrapped) == 'A woman has hemolysis. Which is the most likely diagnosis?'

    def test_strip_choices_fails_loud_without_choices(self):
        with pytest.raises(ValueError, match='choice lines'):
            strip_choices('A question with no options?')

    def test_webqsp_record(self, tmp_path):
        [q] = load_questions('webqsp', _write(tmp_path, 'webqsp', [WEBQSP_RECORD]))
        assert (q.id, q.q_entities, q.answers) == ('WebQTest-0', ['Jamaica'], WEBQSP_RECORD['answer'])
        assert q.schema == WEBQSP_RECORD['current_schema']

    def test_cron_record_gets_synthetic_id_and_type(self, tmp_path):
        [q] = load_questions('cronquestions', _write(tmp_path, 'cronquestions', [CRON_RECORD]))
        assert (q.id, q.qtype, q.q_entities) == ('cron-0', 'first_last', ['X', 'Y'])

    def test_medqa_record_is_stripped_and_answer_listed(self, tmp_path):
        [q] = load_questions('medqa', _write(tmp_path, 'medqa', [MEDQA_RECORD]))
        assert q.answers == ['Clopidogrel'] and 'Nifedipine' not in q.question

    def test_unknown_dataset_raises(self, tmp_path):
        with pytest.raises(ValueError, match="unknown dataset 'ekg'"):
            load_questions('ekg', str(tmp_path))

    def test_filter_answer_in_kg(self):
        g = _graph(('Angina', 'treated by', 'Clopidogrel'))
        qs = [Question('a', 'q', ['Clopidogrel']), Question('b', 'q', ['Aspirin'])]
        assert [q.id for q in filter_answer_in_kg(qs, g)] == ['a']

    def test_sample_is_seeded_and_bounded(self):
        qs = [Question(str(i), 'q', ['a']) for i in range(20)]
        assert [q.id for q in sample(qs, 5, 0)] == [q.id for q in sample(qs, 5, 0)]
        assert len(sample(qs, None, 0)) == 20 and len(sample(qs, 99, 0)) == 20


class TestSchema:
    G = _graph(('A', 'award received (1971)', 'B'), ('A', 'award received (1997)', 'C'),
               ('B', 'member of', 'D'))

    def test_per_question_uses_record_schema(self):
        q = Question('q', 'q', ['a'], schema={'graphSummary': {'edgeLabels': ['r']}})
        assert build_schema('per_question', self.G, q) == q.schema

    def test_per_question_without_schema_fails_loud(self):
        with pytest.raises(ValueError, match='has none'):
            build_schema('per_question', self.G, Question('q', 'q', ['a']))

    def test_base_relations_strips_timestamp_suffix(self):
        s = build_schema('base_relations', self.G, Question('q', 'q', ['a']))
        assert s == {'graphSummary': {'edgeLabels': ['award received', 'member of']}}

    def test_global_is_sorted_and_complete(self):
        s = build_schema('global', self.G, Question('q', 'q', ['a']))
        assert s['graphSummary']['edgeLabels'] == \
            ['award received (1971)', 'award received (1997)', 'member of']


class TestScoring:
    """Hit / F1: a normalized gold answer must appear as whole tokens in one prediction."""

    def test_normalize_drops_case_punctuation_articles(self):
        # Only a leading article is dropped; one inside the string is part of the name.
        assert normalize('The  U.S.A.,  an Example!') == 'usa an example'
        assert normalize('the') == '' and normalize('Theodore') == 'theodore'
        assert normalize('1935-1945', punctuation_as_space=True) == '1935 1945'

    def test_hit_matches_gold_inside_a_single_prediction(self):
        assert hit(['Jamaican English', 'Spanish'], ['jamaican english']) == 1
        # gold contained in a longer prediction counts
        assert hit(['English (Jamaican English dialect)'], ['Jamaican English']) == 1
        assert hit(['English'], ['Jamaican English']) == 0

    @pytest.mark.parametrize('predictions, gold', [
        (['Hepatitis B'], 'Hepatitis A'),     # trailing single-letter disambiguator
        (['Vitamin D'], 'Vitamin A'),
        (['female'], 'male'),                 # substring of a word
        (['1920'], '92'),                     # substring of a number
        (['Henry VIII'], 'Henry VII'),        # prefix of a token
        (['Jamaican', 'English'], 'Jamaican English'),  # spans two separate answers
    ])
    def test_hit_rejects_partial_token_and_cross_answer_matches(self, predictions, gold):
        assert hit(predictions, [gold]) == 0

    @pytest.mark.parametrize('prediction, gold', [
        ('1935-1945', '1945'),                                  # range, punctuation as separator
        ('Cyclophosphamide-induced cystitis', 'Cyclophosphamide'),
        ('U.S. President', 'US President'),                     # abbreviation, punctuation deleted
        ('Post-Traumatic Stress Disorder', 'Posttraumatic stress disorder'),
        ('Kim Jong-un', 'Kim Jong Un'),
    ])
    def test_hit_tolerates_punctuation_either_way(self, prediction, gold):
        assert hit([prediction], [gold]) == 1

    def test_hit_on_empty_predictions_is_zero(self):
        assert hit([], ['x']) == 0

    def test_f1(self):
        assert f1(['Heparin', 'Aspirin'], ['Heparin']) == pytest.approx(2 * 0.5 * 1 / 1.5)
        assert f1([], ['Heparin']) == 0.0

    def test_f1_never_exceeds_one(self):
        # One prediction naming both gold answers: precision 1/1, recall 2/2.
        assert f1(['Jamaican English and Jamaican Creole'],
                  ['Jamaican English', 'Jamaican Creole']) == pytest.approx(1.0)

    def test_empty_normalized_gold_matches_nothing(self):
        assert hit(['anything'], ['']) == 0
        assert hit(['something else'], ['the']) == 0  # normalize('the') == ''
        assert hit(['the Beatles'], ['', 'Beatles']) == 1

    def test_hit_at_2_accepts_either_guess(self):
        assert hit_at_2(['Aspirin'], ['Clopidogrel'], ['Clopidogrel']) == 1
        assert hit_at_2(['Aspirin'], ['Heparin'], ['Clopidogrel']) == 0

    def test_hit_at_2_uses_only_the_first_answer_of_each_list(self):
        assert hit_at_2(['A1', 'A2', 'A3', 'A4'], ['B1', 'B2', 'Gold'], ['Gold']) == 0
        assert hit_at_2(['A1', 'Gold'], ['B1'], ['Gold']) == 0
        assert hit_at_2([], ['Gold', 'B2'], ['Gold']) == 1


class TestHopDistance:
    G = _graph(('Lou Seal', 'mascot of', 'Giants'), ('Giants', 'won', '2014 World Series'))

    def test_zero_one_two(self):
        assert hop_distance(self.G, ['Giants'], ['Giants']) == 0
        assert hop_distance(self.G, ['Giants'], ['2014 World Series']) == 1
        assert hop_distance(self.G, ['Lou Seal'], ['2014 World Series']) == 2

    def test_respects_edge_direction(self):
        assert hop_distance(self.G, ['2014 World Series'], ['Lou Seal']) is None

    def test_beyond_max_hops_is_none(self):
        assert hop_distance(self.G, ['Lou Seal'], ['2014 World Series'], max_hops=1) is None

    def test_missing_sources_or_targets(self):
        assert hop_distance(self.G, [], ['Giants']) is None
        assert hop_distance(self.G, ['Nowhere'], ['Giants']) is None


class TestShowcase:
    BASE = {'hit': 1, 'vanilla_hit': 0, 'answer_in_context': True, 'hops': 2,
            'q_entities': ['Lou Seal'], 'qtype': None}

    def test_multi_hop_kg_grounded_win(self):
        assert showcase_reasons(self.BASE) == ['multi_hop:2']

    @pytest.mark.parametrize('override', [{'hit': 0}, {'vanilla_hit': 1}, {'answer_in_context': False}])
    def test_gate(self, override):
        assert showcase_reasons({**self.BASE, **override}) == []

    def test_single_hop_single_entity_not_showcase(self):
        assert showcase_reasons({**self.BASE, 'hops': 1}) == []

    def test_failed_no_kg_call_is_not_a_wrong_answer(self):
        assert showcase_reasons({**self.BASE, 'vanilla_error': 'ThrottlingException'}) == []

    def test_all_signals(self):
        r = {**self.BASE, 'hops': 3, 'q_entities': ['A', 'B'], 'qtype': 'before_after'}
        assert showcase_reasons(r) == ['multi_hop:3', 'multi_entity', 'temporal:before_after']


class TestValidateGraph:
    def test_accepts_store_format(self):
        validate_graph(_graph(('A', 'r', 'B')), 'g.json')

    @pytest.mark.parametrize('bad', [
        {'A': {'r': {'triplets': [['A', 'r', 'B', '1999']]}}},   # 4-element triplet
        {'A': {'r': {'items': [['A', 'r', 'B']]}}},              # wrong key
        {'A': ['r']},                                            # relations not a dict
    ])
    def test_rejects_malformed_entries(self, bad):
        with pytest.raises(ValueError, match='g.json'):
            validate_graph(bad, 'g.json')


def test_quantile_interpolates_and_reports_none_when_empty():
    # Nearest-rank would return the max (19) for p95 of 20 values.
    assert _quantile(list(range(20)), 0.95) == 18.05
    assert _quantile([], 0.5) is None


class TestSummarize:
    def _rec(self, **kw):
        base = {'hit': 1, 'vanilla_hit': 0, 'f1': 1.0, 'answer_in_context': True, 'llm_calls': 4,
                'latency_s': 2.0, 'context_chars': 100, 'showcase': [], 'error': None, 'hit@2': 1,
                'predictions': ['Heparin'], 'vanilla_error': None}
        return {**base, **kw}

    def test_errors_count_as_misses_and_are_reported(self):
        recs = [self._rec(), self._rec(hit=0, f1=0.0, answer_in_context=False, llm_calls=None,
                                       latency_s=None, context_chars=0, error='boom', **{'hit@2': 0})]
        s = summarize(recs, 'hit')
        assert (s['n'], s['n_errors'], s['hit'], s['llm_calls_mean']) == (2, 1, 50.0, 4.0)
        assert 'hit@2' not in s

    def test_hit_ok_excludes_errored_and_predictions_per_question_reported(self):
        recs = [self._rec(predictions=['A1', 'A2', 'A3']),
                self._rec(hit=0, error='ThrottlingException', predictions=[], llm_calls=None,
                          latency_s=None),
                self._rec(vanilla_error='ThrottlingException')]
        s = summarize(recs, 'hit')
        assert (s['hit'], s['hit_ok'], s['n_errors'], s['n_vanilla_errors']) == (66.7, 100.0, 1, 1)
        assert s['predictions_per_q_mean'] == 2.0   # (3 + 1) over the 2 non-errored

    def test_hit_at_2_reported_for_medqa(self):
        assert summarize([self._rec()], 'hit@2')['hit@2'] == 100.0
