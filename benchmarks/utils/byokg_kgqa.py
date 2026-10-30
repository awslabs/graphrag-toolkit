# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""KGQA evaluation for BYOKG on WebQSP, CWQ, CronQuestions and MedQA.

Pure logic only — no graphrag_toolkit.byokg_rag import — so the benchmarks CI,
which does not install byokg-rag, can test it. The runner lives in
benchmarks/scripts/benchmark_byokg_kgqa.py.
"""

import json
import os
import random
import re
import string
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from benchmarks.utils.metrics_summary import _percentile


@dataclass(frozen=True)
class DatasetSpec:
    questions_file: str
    graph_file: str
    # 'per_question' | 'base_relations' | 'global' — see build_schema()
    schema_mode: str
    # 'hit' | 'hit@2' (MedQA)
    metric: str
    # Published reference scores, for comparison.
    paper_reference: str
    # Same numbers, machine-readable: {model: {kind: score}}. kind is 'full' (full
    # system) or 'agentic' (agentic retrieval only), suffixed with the question set
    # when it differs from this suite's: '_ih500' (another 500-question subset),
    # '_227q' (MedQA's 227-question selection). No suffix = same question set.
    references: Dict[str, Dict[str, float]] = field(default_factory=dict)


# The data prefix ships alternates that must not be loaded: graph_cwq_2.json and a
# top-level graph_webqsp.json (same triplets, different node order),
# graph_tempwiki.json (every edge mirrored, doubling one-hop fan-out) and
# graph_temp.json (a preprocessing intermediate). graph_tempwiki_one.json is the
# one-direction Temp-Wikidata KG (~325K triplets).
DATASETS: Dict[str, DatasetSpec] = {
    # WebQSP / CWQ run the full test sets (or a seeded sample of them): only the
    # Sonnet-3.5 full-system scores are on that set; the rest are on a different
    # 500-question subset.
    'webqsp': DatasetSpec(
        'questions_webqsp.json', 'graph/graph_webqsp.json', 'per_question', 'hit',
        'Hit. Table 2 full set: 87.1 (Sonnet-3.5). Table 1 IH subset: '
        'Sonnet-3.5 86.6 / 86.2 agentic; Haiku-3.5 82.8 / 81.0 agentic',
        {'claude-3.5-sonnet': {'full': 87.1, 'full_ih500': 86.6, 'agentic_ih500': 86.2},
         'claude-3.5-haiku': {'full_ih500': 82.8, 'agentic_ih500': 81.0}}),
    'cwq': DatasetSpec(
        'questions_cwq.json', 'graph/graph_cwq.json', 'per_question', 'hit',
        'Hit. Table 2 full set: 71.1 (Sonnet-3.5). Table 1 IH subset: '
        'Sonnet-3.5 73.6 / 69.3 agentic; Haiku-3.5 66.8 / 64.2 agentic',
        {'claude-3.5-sonnet': {'full': 71.1, 'full_ih500': 73.6, 'agentic_ih500': 69.3},
         'claude-3.5-haiku': {'full_ih500': 66.8, 'agentic_ih500': 64.2}}),
    'cronquestions': DatasetSpec(
        'cronquestions_in_house.json', 'graph/graph_tempwiki_one.json', 'base_relations', 'hit',
        'Hit. Table 1: Sonnet-3.5 65.5 / 57.3 agentic; Haiku-3.5 61.9 / 40.8 agentic',
        {'claude-3.5-sonnet': {'full': 65.5, 'agentic': 57.3},
         'claude-3.5-haiku': {'full': 61.9, 'agentic': 40.8}}),
    'medqa': DatasetSpec(
        'medQA_in_house.json', 'graph/graph_ddb.json', 'global', 'hit@2',
        'H@2. Table 1 on 227 Q: Sonnet-3.5 65.0 / 59.2 agentic; Haiku-3.5 54.2 / 46.2 agentic. '
        'This suite runs 236 Q (answer is an exact KG node), so not like-for-like',
        {'claude-3.5-sonnet': {'full_227q': 65.0, 'agentic_227q': 59.2},
         'claude-3.5-haiku': {'full_227q': 54.2, 'agentic_227q': 46.2}}),
}

CRON_TEMPORAL_TYPES = {'first_last', 'before_after', 'time_join'}


@dataclass
class Question:
    id: str
    question: str
    answers: List[str]
    q_entities: List[str] = field(default_factory=list)
    a_entities: List[str] = field(default_factory=list)
    schema: Optional[Dict[str, Any]] = None
    qtype: Optional[str] = None


# --- loading -----------------------------------------------------------------

_CHOICE_LINE = re.compile(r'^\s*[A-E][.)]\s+\S')


def strip_choices(question: str) -> str:
    """Drop MedQA's trailing multiple-choice lines, so questions are open-ended.

    7 of the 277 delivered records are wrapped in double quotes, leaving a lone '"'
    line after the choices; the wrapping quotes are removed with them.
    """
    lines = question.strip().strip('"').rstrip().split('\n')
    end = len(lines)
    while end > 0 and _CHOICE_LINE.match(lines[end - 1]):
        end -= 1
    if end == len(lines):
        raise ValueError(f'expected trailing A-E choice lines, found none: {question[-200:]!r}')
    return '\n'.join(lines[:end]).strip().strip('"').rstrip()


def load_questions(dataset: str, data_dir: str) -> List[Question]:
    if dataset not in DATASETS:
        raise ValueError(f'unknown dataset {dataset!r}; expected one of {sorted(DATASETS)}')
    spec = DATASETS[dataset]
    with open(os.path.join(data_dir, spec.questions_file), encoding='utf-8') as f:
        records = json.load(f)
    if dataset in ('webqsp', 'cwq'):
        return [Question(r['id'], r['question'], list(r['answer']), list(r['q_entity']),
                         list(r['a_entity']), r['current_schema']) for r in records]
    if dataset == 'cronquestions':
        return [Question(f'cron-{i}', r['question'], list(r['answer']), list(r['q_entity']),
                         list(r['answer']), qtype=r['type']) for i, r in enumerate(records)]
    if dataset == 'medqa':
        return [Question(f'medqa-{i}', strip_choices(r['question']), [r['answer']],
                         a_entities=[r['answer']]) for i, r in enumerate(records)]
    raise AssertionError(f'DATASETS has {dataset!r} but load_questions does not handle it')


def filter_answer_in_kg(questions: Sequence[Question], graph: Dict[str, Any]) -> List[Question]:
    """Keep MedQA questions whose answer is a KG node (exact match: 236 of 277).

    A question whose answer is absent from the KG cannot be answered from it. The
    published 227 used an entity-matching filter that isn't specified, so the sets differ.
    """
    return [q for q in questions if any(a in graph for a in q.answers)]


def validate_graph(graph: Dict[str, Any], graph_file: str) -> None:
    """Fail before any LLM call if the graph isn't {head: {rel: {"triplets": [[h, r, t]]}}}.

    hop_distance and LocalKGStore both index this shape directly; a malformed entry
    is a data error to stop on, not a per-question miss to record thousands of times.
    """
    for head, rels in graph.items():
        if not isinstance(rels, dict):
            raise ValueError(f'{graph_file}: node {head!r} maps to {type(rels).__name__}, not a dict')
        for rel, entry in rels.items():
            triplets = entry.get('triplets') if isinstance(entry, dict) else None
            if not isinstance(triplets, list) or any(len(t) != 3 for t in triplets):
                raise ValueError(f'{graph_file}: {head!r} -> {rel!r} is not '
                                 f'{{"triplets": [[head, relation, tail], ...]}}: {str(entry)[:200]}')


def sample(questions: Sequence[Question], limit: Optional[int], seed: int) -> List[Question]:
    """Seeded subset, so a --limit run mixes question types instead of taking a block."""
    if limit is None or limit >= len(questions):
        return list(questions)
    return random.Random(seed).sample(list(questions), limit)


# --- schema ------------------------------------------------------------------

_TIMESTAMP_SUFFIX = re.compile(r'\s*\([^()]*\)$')


def relation_labels(graph: Dict[str, Any]) -> List[str]:
    return sorted({r for rels in graph.values() for r in rels})


def build_schema(mode: str, graph: Dict[str, Any], question: Question) -> Dict[str, Any]:
    """Schema handed to KG-Linker.

    The global schema is unusable for Freebase (5,419 labels, ~246K chars per call)
    and Temp-Wikidata (35,770 labels, ~1.07M chars — beyond the context window), so:
      per_question   — the dataset's own `current_schema` (20 relations per question)
      base_relations — labels with the folded "(timestamp)" suffix removed (203 for Cron)
      global         — every label, sorted (LocalKGStore's own get_schema() is unordered)
    """
    if mode == 'per_question':
        if not question.schema:
            raise ValueError(f'{question.id}: per_question schema requested but record has none')
        return question.schema
    if mode == 'base_relations':
        labels = sorted({_TIMESTAMP_SUFFIX.sub('', r) for r in relation_labels(graph)})
    elif mode == 'global':
        labels = relation_labels(graph)
    else:
        raise ValueError(f'unknown schema mode {mode!r}')
    return {'graphSummary': {'edgeLabels': labels}}


# --- scoring: KGQA Hit / F1 with whole-token matching --------------------------

_PUNCTUATION = set(string.punctuation)
_PUNCTUATION_RE = re.compile('[' + re.escape(string.punctuation) + ']')


def normalize(s: str, punctuation_as_space: bool = False) -> str:
    """Lowercase, handle punctuation, collapse whitespace, drop a *leading* article.

    Only a leading article goes: stripping a/an/the anywhere (the SQuAD-style rule)
    turns 'Hepatitis A' into 'hepatitis', which then matches 'Hepatitis B'. A bare
    'the' normalizes to '' and matches nothing; a bare 'a' or 'an' is kept, since 'A'
    is a real answer (blood type, vitamin, grade). No gold answer in the four
    datasets is a bare article, so this changes no recorded score.
    """
    s = s.lower()
    if punctuation_as_space:
        s = _PUNCTUATION_RE.sub(' ', s)
    else:
        s = ''.join(ch for ch in s if ch not in _PUNCTUATION)
    s = re.sub(r'\b(<pad>)\b', ' ', s)
    s = ' '.join(s.split())
    if s == 'the':
        return ''
    return re.sub(r'^(?:a|an|the) ', '', s)


def _match(prediction: str, answer: str) -> bool:
    """Gold answer appears in one prediction as whole tokens.

    Whole tokens, so 'male' does not match 'female' nor '92' match '1920'. Punctuation
    is tried both deleted ('U.S.' = 'US', 'Post-Traumatic' = 'Posttraumatic') and as a
    separator ('1935-1945' contains '1945'), the same way on both sides. A gold answer
    that normalizes to '' ('the') matches nothing.
    """
    for as_space in (False, True):
        gold = normalize(answer, as_space)
        if gold and re.search(rf'(?<!\S){re.escape(gold)}(?!\S)', normalize(prediction, as_space)):
            return True
    return False


def hit(predictions: Sequence[str], answers: Iterable[str]) -> int:
    """1 if any gold answer matches any single prediction.

    Predictions are matched one at a time: joining them would let ['Jamaican',
    'English'] match 'Jamaican English' across two separate answers.
    """
    answers = list(answers)
    return int(any(_match(p, a) for p in predictions for a in answers))


def f1(predictions: Sequence[str], answers: Sequence[str]) -> float:
    """Precision counts predictions matching a gold answer, recall counts gold answers
    matched by some prediction — so one prediction naming two gold answers cannot push
    F1 past 1."""
    if not predictions or not answers:
        return 0.0
    recall = sum(1 for a in answers if any(_match(p, a) for p in predictions)) / len(answers)
    precision = sum(1 for p in predictions if any(_match(p, a) for a in answers)) / len(predictions)
    return 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)


def hit_at_2(kg_predictions: Sequence[str], vanilla_predictions: Sequence[str],
             answers: Iterable[str]) -> int:
    """MedQA H@2: the first KG answer plus the first LLM-only answer — two guesses,
    however many answers each list holds."""
    return hit(list(kg_predictions[:1]) + list(vanilla_predictions[:1]), answers)


# --- showcase signals --------------------------------------------------------

def hop_distance(graph: Dict[str, Any], sources: Iterable[str], targets: Iterable[str],
                 max_hops: int = 3) -> Optional[int]:
    """Fewest outgoing-edge hops from any source to any target, or None if > max_hops.

    Outgoing only, because that is what LocalKGStore.get_one_hop_edges lets BYOKG
    traverse — a target reachable only against edge direction is not one hop away
    for the engine either.
    """
    targets = set(targets)
    frontier = [s for s in sources if s in graph]
    if not targets or not frontier:
        return None
    if targets.intersection(frontier):
        return 0
    seen = set(frontier)
    queue = deque((s, 0) for s in frontier)
    while queue:
        node, depth = queue.popleft()
        if depth == max_hops:
            continue
        for rel in graph.get(node, {}).values():
            for _, _, tail in rel['triplets']:
                if tail in targets:
                    return depth + 1
                if tail not in seen:
                    seen.add(tail)
                    queue.append((tail, depth + 1))
    return None


def showcase_reasons(record: Dict[str, Any]) -> List[str]:
    """Why a scored question is a demo candidate; empty if it is not one.

    Gate: BYOKG right, the no-KG LLM wrong (an errored no-KG call is not "wrong" —
    it never answered), and the gold answer present in the retrieved context — so
    the KG demonstrably supplied it. Then at least one signal that the question is
    hard to write as a direct lookup query.
    """
    if not (record['hit'] and not record['vanilla_hit'] and not record.get('vanilla_error')
            and record['answer_in_context']):
        return []
    reasons = []
    hops = record.get('hops')
    if hops is not None and hops >= 2:
        reasons.append(f'multi_hop:{hops}')
    if len(record.get('q_entities') or []) >= 2:
        reasons.append('multi_entity')
    if record.get('qtype') in CRON_TEMPORAL_TYPES:
        reasons.append(f"temporal:{record['qtype']}")
    return reasons


def answer_in_context(context: Sequence[str], answers: Iterable[str]) -> bool:
    return hit(context, answers) == 1


# --- aggregation -------------------------------------------------------------

def _pct(values: Sequence[float]) -> Optional[float]:
    return round(100 * sum(values) / len(values), 1) if values else None


def _quantile(values: Sequence[float], q: float) -> Optional[float]:
    """Interpolated percentile (q in 0–1); None rather than 0.0 for an empty run."""
    if not values:
        return None
    return round(_percentile(sorted(values), 100 * q), 2)


def summarize(records: Sequence[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    """Per-run summary.

    Errored questions count as misses in `hit` and `hit@2`; `hit_ok` is over the non-errored ones,
    so throttling noise can be told apart from a quality change. `vanilla_*` is the
    BYOKG answer prompt with an empty graph context, not a separate LLM-only prompt.
    """
    ok = [r for r in records if not r.get('error')]
    vanilla_ok = [r for r in records if not r.get('vanilla_error')]
    summary = {
        'n': len(records),
        'n_errors': len(records) - len(ok),
        'n_vanilla_errors': len(records) - len(vanilla_ok),
        'hit': _pct([r['hit'] for r in records]),
        'hit_ok': _pct([r['hit'] for r in ok]),
        'predictions_per_q_mean': round(sum(len(r['predictions']) for r in ok) / len(ok), 2) if ok else None,
        'vanilla_hit': _pct([r['vanilla_hit'] for r in records]),
        'f1': _pct([r['f1'] for r in records]),
        'answer_in_context': _pct([r['answer_in_context'] for r in records]),
        'llm_calls_mean': round(sum(r['llm_calls'] for r in ok) / len(ok), 2) if ok else None,
        'latency_s_p50': _quantile([r['latency_s'] for r in ok], 0.5),
        'latency_s_p95': _quantile([r['latency_s'] for r in ok], 0.95),
        'context_chars_median': _quantile([r['context_chars'] for r in ok], 0.5),
        'n_showcase': sum(1 for r in records if r.get('showcase')),
        'showcase_ids': [r['id'] for r in records if r.get('showcase')],
    }
    if metric == 'hit@2':
        summary['hit@2'] = _pct([r['hit@2'] for r in records])
    return summary
