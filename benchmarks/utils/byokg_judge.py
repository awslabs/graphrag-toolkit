# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""LLM-judge scoring of saved BYOKG KGQA predictions, with Claude Opus 5.5 on Bedrock.

The judge sees a question, its gold answers and one answer list, and says whether any
listed answer names a gold answer: string Hit's any-match rule, with aliases and
format variants allowed. String metrics are recomputed here with the current scorer,
never read from a record's stored fields, which older runs computed differently.
"""

import hashlib
import json
import logging
import os
import threading
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from benchmarks.utils.byokg_kgqa import DATASETS, f1, hit, hit_at_2

logger = logging.getLogger(__name__)

JUDGE_NAME = 'Claude Opus 5.5'
# The global cross-region profile: the bedrock-runtime endpoint serves Opus 5.5 only
# through a geo or global profile, and global carries no regional premium.
JUDGE_MODEL_ID = 'global.anthropic.claude-opus-5-5'
# A label for the outputs; cache keys hash the prompt texts themselves, so an edit to
# them is never served old verdicts. Bump it with any change to the prompts.
RUBRIC_VERSION = 1

SYSTEM_PROMPT = """You grade answers to questions about a knowledge graph.

You get a question, its gold answers, and a list of answers to grade. The list is correct when at least one listed answer refers to the same entity, value or concept as at least one gold answer. Other answers in the list don't make it wrong.

Count as the same:
- aliases, abbreviations, alternative or translated names, and spelling or capitalization variants;
- the same date or number written another way, and a date that falls within a gold year;
- a name with or without a title, middle name or qualifier, when it still refers to the same entity.

Count as different:
- another entity, even a related one, such as a country for its city or a team for its player;
- an entity that only shares part of its name with a gold answer;
- a refusal, a hedge or an empty answer.

Grade only against the gold answers given; don't use your own knowledge to decide whether a gold answer is right. In matched_gold, copy the gold answer that was matched exactly as given, or leave it empty when the list is incorrect. Keep explanation to one sentence."""

VERDICT_SCHEMA = {
    'type': 'object',
    'properties': {
        'verdict': {'type': 'string', 'enum': ['correct', 'incorrect']},
        'matched_gold': {'type': 'string'},
        'explanation': {'type': 'string'},
    },
    'required': ['verdict', 'matched_gold', 'explanation'],
    'additionalProperties': False,
}

# Roles of the answer lists one record is judged on. H@2 (MedQA) is the first KG answer
# plus the first no-KG answer, judged separately as the string metric scores them.
KG, VANILLA, KG_FIRST, VANILLA_FIRST = 'kg', 'vanilla', 'kg_first', 'vanilla_first'


@dataclass(frozen=True)
class Unit:
    """One judge call: a question with its gold answers and one answer list."""
    dataset: str
    question_id: str
    question: str
    gold: Tuple[str, ...]
    answers: Tuple[str, ...]

    def key(self, model_id: str, effort: str) -> str:
        payload = [self.dataset, self.question_id, self.prompt(), SYSTEM_PROMPT, VERDICT_SCHEMA, model_id, effort]
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()

    def prompt(self) -> str:
        return (f'Question: {self.question}\n'
                f'Gold answers: {json.dumps(list(self.gold), ensure_ascii=False)}\n'
                f'Answers to grade: {json.dumps(list(self.answers), ensure_ascii=False)}')


def record_units(dataset: str, record: Dict[str, Any]) -> Dict[str, Optional[Unit]]:
    """The units a predictions.jsonl record is scored on, by role. An empty answer list
    maps to None: nothing to judge, and it scores as a miss, as string Hit does."""
    metric = DATASETS[dataset].metric
    lists = {KG: record['predictions'], VANILLA: record['vanilla_predictions']}
    if metric == 'hit@2':
        lists.update({KG_FIRST: record['predictions'][:1], VANILLA_FIRST: record['vanilla_predictions'][:1]})
    gold = tuple(record['answers'])
    return {role: (Unit(dataset, str(record['id']), record['question'], gold, tuple(answers))
                   if answers else None)
            for role, answers in lists.items()}


def parse_verdict(response) -> Dict[str, Any]:
    """A Messages response as a verdict. Anything but a schema-valid reply gets a status
    other than 'ok' and no verdict: it is counted, never scored as wrong."""
    usage = {'input': response.usage.input_tokens, 'output': response.usage.output_tokens}
    verdict = {'status': 'ok', 'correct': None, 'matched_gold': None, 'explanation': None,
               'stop_category': None, 'usage': usage}
    if response.stop_reason == 'refusal':
        details = getattr(response, 'stop_details', None) or (response.model_extra or {}).get('stop_details')
        category = details.get('category') if isinstance(details, dict) else getattr(details, 'category', None)
        return {**verdict, 'status': 'refusal', 'stop_category': category}
    if response.stop_reason == 'max_tokens':
        return {**verdict, 'status': 'truncated'}
    texts = [block.text for block in response.content if block.type == 'text']
    try:
        data = json.loads(texts[0])
        correct = {'correct': True, 'incorrect': False}[data['verdict']]
        matched, explanation = data['matched_gold'], data['explanation']
        if not isinstance(matched, str) or not isinstance(explanation, str):
            raise TypeError(f'non-string fields in {data!r}')
    except (IndexError, ValueError, KeyError, TypeError) as e:
        logger.warning('judge reply is not a valid verdict (%s): %r', e, texts)
        return {**verdict, 'status': 'invalid'}
    return {**verdict, 'correct': correct, 'matched_gold': matched, 'explanation': explanation}


class JudgeCache:
    """Verdicts by unit key, in an append-only JSONL file, so an interrupted rescoring
    resumes and arms that gave the same answers are judged once."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._verdicts: Dict[str, Dict[str, Any]] = {}
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line)
                        self._verdicts[entry['key']] = entry['verdict']

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self._verdicts.get(key)

    def put(self, key: str, verdict: Dict[str, Any]) -> None:
        with self._lock:
            self._verdicts[key] = verdict
            with open(self.path, 'a', encoding='utf-8') as f:
                f.write(json.dumps({'key': key, 'verdict': verdict}, ensure_ascii=False) + '\n')


class Judge:
    """Judges units through an anthropic Bedrock client, caching every verdict except a
    truncated one, which a rerun with a larger max_tokens should retry."""

    def __init__(self, client, cache: JudgeCache, model_id: str = JUDGE_MODEL_ID,
                 effort: str = 'low', max_tokens: int = 2048):
        self.client, self.cache = client, cache
        self.model_id, self.effort, self.max_tokens = model_id, effort, max_tokens
        self._uncached: Dict[str, Dict[str, Any]] = {}

    def cached(self, unit: Unit) -> Optional[Dict[str, Any]]:
        """This unit's verdict from the cache or from this process, else None."""
        key = unit.key(self.model_id, self.effort)
        return self.cache.get(key) or self._uncached.get(key)

    def judge(self, unit: Unit) -> Dict[str, Any]:
        key = unit.key(self.model_id, self.effort)
        verdict = self.cache.get(key)
        if verdict is None:
            # Opus 5.5 takes no sampling parameters and always thinks; effort sets how much.
            response = self.client.messages.create(
                model=self.model_id, max_tokens=self.max_tokens, system=SYSTEM_PROMPT,
                messages=[{'role': 'user', 'content': unit.prompt()}],
                output_config={'effort': self.effort, 'format': {'type': 'json_schema', 'schema': VERDICT_SCHEMA}})
            verdict = parse_verdict(response)
            if verdict['status'] == 'truncated':
                self._uncached[key] = verdict
            else:
                self.cache.put(key, verdict)
        return verdict


def _verdict_hit(verdict: Optional[Dict[str, Any]]) -> Optional[int]:
    """1/0 from a verdict; 0 for an empty list (no unit); None when the judge gave no verdict."""
    if verdict is None:
        return 0
    return None if verdict['status'] != 'ok' else int(verdict['correct'])


def score_record(dataset: str, record: Dict[str, Any],
                 verdicts: Dict[str, Optional[Dict[str, Any]]]) -> Dict[str, Any]:
    """One record's judge scores next to its recomputed string scores. verdicts maps each
    role from record_units() to its verdict (None for an empty list)."""
    gold = record['answers']
    row = {'id': record['id'], 'error': record.get('error'),
           'judge': verdicts,
           'judge_hit': _verdict_hit(verdicts[KG]),
           'judge_vanilla_hit': _verdict_hit(verdicts[VANILLA]),
           'hit': hit(record['predictions'], gold),
           'vanilla_hit': hit(record['vanilla_predictions'], gold),
           'f1': f1(record['predictions'], gold)}
    if DATASETS[dataset].metric == 'hit@2':
        firsts = [_verdict_hit(verdicts[KG_FIRST]), _verdict_hit(verdicts[VANILLA_FIRST])]
        # Either first answer being right decides it; an errored KG path is a miss, as
        # in the string metric.
        row['judge_hit@2'] = 0 if record.get('error') else (
            1 if 1 in firsts else None if None in firsts else 0)
        row['hit@2'] = 0 if record.get('error') else hit_at_2(
            record['predictions'], record['vanilla_predictions'], gold)
    return row


def _pct(values: Sequence[Optional[int]]) -> Optional[float]:
    scored = [v for v in values if v is not None]
    return round(100 * sum(scored) / len(scored), 1) if scored else None


def headline(dataset: str) -> str:
    return 'judge_hit@2' if DATASETS[dataset].metric == 'hit@2' else 'judge_hit'


def summarize_judged(dataset: str, rows: Sequence[Dict[str, Any]], judge: Dict[str, Any]) -> Dict[str, Any]:
    """Per-run summary. Judge percentages are over the records the judge scored; records it
    gave no verdict on are counted per metric in n_unscored, not as misses."""
    statuses = Counter(v['status'] for r in rows for v in r['judge'].values() if v is not None)
    both = [r for r in rows if r['judge_hit'] is not None]
    metrics = ['judge_hit', 'judge_vanilla_hit'] + (['judge_hit@2'] if DATASETS[dataset].metric == 'hit@2' else [])
    summary = {
        'judge': judge, 'n': len(rows),
        'judge_hit': _pct([r['judge_hit'] for r in rows]),
        'n_unscored': {m: sum(1 for r in rows if r[m] is None) for m in metrics},
        'judge_vanilla_hit': _pct([r['judge_vanilla_hit'] for r in rows]),
        'hit': _pct([r['hit'] for r in rows]), 'vanilla_hit': _pct([r['vanilla_hit'] for r in rows]),
        'f1': _pct([r['f1'] for r in rows]),
        # Where judge and string matching disagree on the KG answer, and which way.
        'agreement': _pct([int(r['judge_hit'] == r['hit']) for r in both]),
        'judge_only': sum(1 for r in both if r['judge_hit'] and not r['hit']),
        'string_only': sum(1 for r in both if r['hit'] and not r['judge_hit']),
        'statuses': dict(sorted(statuses.items())),
    }
    if DATASETS[dataset].metric == 'hit@2':
        summary['judge_hit@2'] = _pct([r['judge_hit@2'] for r in rows])
        summary['hit@2'] = _pct([r['hit@2'] for r in rows])
    return summary


def find_runs(root: str) -> List[Tuple[str, str, str, str, str]]:
    """(group, dataset, setting, run, predictions path) for every run under root, from the
    harness layout <group>/<dataset>/<setting>/<run>/predictions.jsonl. group is the path
    from root to the dataset directory, e.g. one stack's results: arms that share a
    dataset and setting name differ only there."""
    runs = []
    for directory, _, files in os.walk(root):
        if 'predictions.jsonl' not in files:
            continue
        run_dir = os.path.abspath(directory)
        setting_dir = os.path.dirname(run_dir)
        dataset = os.path.basename(os.path.dirname(setting_dir))
        if dataset not in DATASETS:
            raise ValueError(f'{run_dir}: expected <dataset>/<setting>/<run>/predictions.jsonl, '
                             f'with dataset one of {sorted(DATASETS)}')
        group = os.path.relpath(os.path.dirname(os.path.dirname(setting_dir)), os.path.abspath(root))
        runs.append((group.replace(os.sep, '/'), dataset, os.path.basename(setting_dir), os.path.basename(run_dir),
                     os.path.join(run_dir, 'predictions.jsonl')))
    return sorted(runs)


def read_records(path: str) -> List[Dict[str, Any]]:
    with open(path, encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def unique_units(units: Iterable[Optional[Unit]], model_id: str, effort: str) -> Dict[str, Unit]:
    return {u.key(model_id, effort): u for u in units if u is not None}
