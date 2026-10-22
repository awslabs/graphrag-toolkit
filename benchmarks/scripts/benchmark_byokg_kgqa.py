# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""BYOKG benchmark on the WebQSP, CWQ, CronQuestions and MedQA KGQA datasets.

Measures the toolkit's *current defaults* (Hit / H@2 scoring, 2 retrieval
iterations), so later optimizations are measured against it. Requires
byokg-rag installed and Bedrock access. Two entry points:

  stack:  integration-tests/build-tests.sh --test-file benchmark.byokg-kgqa ...
          (the *ByokgKgqa classes below; config via BYOKG_* env vars)
  local:  pip install -e lexical-graph -e byokg-rag
          python -m benchmarks.scripts.benchmark_byokg_kgqa --dataset webqsp --limit 50

Per run it writes predictions.jsonl, summary.json and showcase_candidates.jsonl;
aggregate.json summarizes across runs.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import unittest
from typing import Any, Callable, Dict, List, Optional, Sequence

from benchmarks.scripts.integration_test_base import IntegrationTestBase
from benchmarks.scripts.integration_test_handler import IntegrationTestHandler
from benchmarks.utils.benchmark_env import env_int, env_string
from benchmarks.utils.s3_utils import upload_benchmark_results_to_s3
from benchmarks.utils.byokg_kgqa import (
    DATASETS, Question, answer_in_context, build_schema, f1, filter_answer_in_kg,
    hit, hit_at_2, hop_distance, load_questions, sample, showcase_reasons, summarize,
    validate_graph,
)

logger = logging.getLogger(__name__)

# Relative to the working directory, matching the stack's BYOKG_DATA_DIR default.
# Never default inside benchmarks/: build-tests.sh packages that tree with cp -r,
# which ignores .gitignore, so data placed there would ship with every stack.
DEFAULT_DATA_DIR = 'byokg-data'
# A run whose first N questions all fail is misconfigured (credentials, model id),
# not unlucky — stop before burning the rest of the budget.
ABORT_IF_FIRST_N_FAIL = 5


class CountingGenerator:
    """Delegates to a BYOKG generator and counts generate() calls per question."""

    def __init__(self, inner):
        self._inner = inner
        self.calls = 0

    def generate(self, *args, **kwargs):
        self.calls += 1
        return self._inner.generate(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def build_engine(graph: Dict[str, Any], model: Optional[str], region: Optional[str],
                 single_best_match: bool):
    """Engine with library defaults; only the graph store and LLM are supplied."""
    from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine
    from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
    from graphrag_toolkit.byokg_rag.llm.bedrock_llms import BedrockGenerator

    counter = CountingGenerator(BedrockGenerator(model_name=model, region_name=region))
    engine = ByoKGQueryEngine(graph_store=LocalKGStore(graph), llm_generator=counter,
                              single_best_match=single_best_match)
    return engine, counter


def run_question(engine, counter: CountingGenerator, question: Question,
                 schema: Dict[str, Any], hops: Optional[int], iterations: int) -> Dict[str, Any]:
    """Score one question. Exceptions are recorded, not raised, and count as misses.

    schema and hops are precomputed by run_benchmark, so nothing here can raise
    outside the try.
    """
    artifacts: List[Dict[str, List[str]]] = []
    parse = engine.kg_linker.parse_response

    def recording_parse(response):
        parsed = parse(response)
        artifacts.append(parsed)
        return parsed

    record: Dict[str, Any] = {
        'id': question.id, 'question': question.question, 'answers': question.answers,
        'q_entities': question.q_entities, 'qtype': question.qtype, 'hops': hops,
    }
    engine.kg_linker.parse_response = recording_parse
    try:
        engine.schema = schema
        counter.calls = 0
        start = time.monotonic()
        context = engine.query(question.question, iterations=iterations)
        predictions, _ = engine.generate_response(question.question, '\n'.join(context))
        record.update(latency_s=round(time.monotonic() - start, 2), llm_calls=counter.calls)
        # No-KG answer: the LLM-only baseline, MedQA's H@2 second guess, and the
        # showcase gate. Outside the timer and the call count.
        vanilla, _ = engine.generate_response(question.question, '')
        record.update(predictions=predictions, vanilla_predictions=vanilla,
                      context=context, context_chars=sum(len(c) for c in context),
                      kg_linker_artifacts=artifacts, error=None)
    except Exception as e:  # recorded per question; the run continues
        logger.exception('question %s failed', question.id)
        record.update(predictions=[], vanilla_predictions=[], context=[], context_chars=0,
                      kg_linker_artifacts=artifacts, latency_s=None, llm_calls=None,
                      error=f'{type(e).__name__}: {e}')
    finally:
        engine.kg_linker.parse_response = parse

    record['hit'] = hit(record['predictions'], question.answers)
    record['vanilla_hit'] = hit(record['vanilla_predictions'], question.answers)
    record['hit@2'] = hit_at_2(record['predictions'], record['vanilla_predictions'],
                               question.answers)
    record['f1'] = f1(record['predictions'], question.answers)
    record['answer_in_context'] = answer_in_context(record['context'], question.answers)
    record['showcase'] = showcase_reasons(record)
    return record


def run_once(engine, counter, questions: Sequence[Question], schemas: Dict[str, Any],
             hops: Dict[str, Optional[int]], metric: str, iterations: int,
             out_dir: str) -> Dict[str, Any]:
    os.makedirs(out_dir, exist_ok=True)
    records = []
    with open(os.path.join(out_dir, 'predictions.jsonl'), 'w', encoding='utf-8') as preds, \
            open(os.path.join(out_dir, 'showcase_candidates.jsonl'), 'w', encoding='utf-8') as show:
        for i, q in enumerate(questions, 1):
            record = run_question(engine, counter, q, schemas[q.id], hops[q.id], iterations)
            records.append(record)
            preds.write(json.dumps(record) + '\n')
            if record['showcase']:
                show.write(json.dumps(record) + '\n')
            if i == ABORT_IF_FIRST_N_FAIL and all(r['error'] for r in records):
                raise RuntimeError(f'first {i} questions all failed; last error: {record["error"]}')
            if i % 25 == 0:
                logger.info('%s: %d/%d, running Hit %.1f', out_dir, i, len(questions),
                            100 * sum(r['hit'] for r in records) / i)
    summary = summarize(records, metric)
    with open(os.path.join(out_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    if summary['n_errors']:
        logger.warning('%s: %d/%d questions errored — see "error" in predictions.jsonl',
                       out_dir, summary['n_errors'], summary['n'])
    return summary


def aggregate(summaries: Sequence[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    key = 'hit@2' if metric == 'hit@2' else 'hit'
    scores = [s[key] for s in summaries]
    # The repeats share one question list, so showcase counts are per run (mean) or
    # distinct questions across runs — never summed, which multiplies by the run count.
    distinct = set().union(*(s['showcase_ids'] for s in summaries))
    return {'metric': key, 'runs': len(scores), 'mean': round(sum(scores) / len(scores), 1),
            'min': min(scores), 'max': max(scores), 'spread': round(max(scores) - min(scores), 1),
            'showcase_per_run_mean': round(sum(s['n_showcase'] for s in summaries) / len(summaries), 1),
            'showcase_distinct': len(distinct),
            'per_run': list(summaries)}


SINGLE_BEST_MATCH_SETTINGS = {'off': [False], 'on': [True], 'both': [False, True]}


def validate_config(single_best_match: str, runs: int, iterations: int) -> None:
    """Checked before any download or graph load, so a typo costs seconds, not ~7 GB."""
    if single_best_match not in SINGLE_BEST_MATCH_SETTINGS:
        raise ValueError(f'single_best_match must be one of {sorted(SINGLE_BEST_MATCH_SETTINGS)}, '
                         f'got {single_best_match!r}')
    for name, value in (('runs', runs), ('iterations', iterations)):
        if value < 1:
            raise ValueError(f'{name} must be >= 1, got {value}')


def _aws(*args: str) -> None:
    try:
        subprocess.run(['aws', 's3', *args], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f'aws s3 {" ".join(args)} failed: {e.stderr}') from e


def fetch_dataset(s3_uri: str, dataset: str, data_dir: str) -> None:
    """Download only this dataset's question and graph files.

    The prefix also holds ~5.5 GB of N-Triples and embedding pickles the runner
    never reads; a full sync would overflow the stack notebook's default 5 GB volume.
    """
    spec = DATASETS[dataset]
    for rel in (spec.questions_file, spec.graph_file):
        local = os.path.join(data_dir, rel)
        if os.path.exists(local):
            logger.info('already present: %s', local)
            continue
        os.makedirs(os.path.dirname(local), exist_ok=True)
        _aws('cp', f"{s3_uri.rstrip('/')}/{rel}", local)


def run_benchmark(dataset: str, data_dir: str, out: str, limit: Optional[int] = None,
                  seed: int = 0, runs: int = 3, single_best_match: str = 'both',
                  iterations: int = 2, model: Optional[str] = None, region: Optional[str] = None,
                  medqa_kg_filter: bool = True,
                  after_run: Optional[Callable[[str, str], None]] = None) -> Dict[str, Any]:
    """Run every (single_best_match setting × repeat) and write aggregate.json.

    after_run(run_dir, relative_path) is called as each run finishes, so callers can
    persist completed runs before a later one crashes.
    """
    validate_config(single_best_match, runs, iterations)
    spec = DATASETS[dataset]
    with open(os.path.join(data_dir, spec.graph_file), encoding='utf-8') as f:
        graph = json.load(f)
    validate_graph(graph, spec.graph_file)
    questions = load_questions(dataset, data_dir)
    if dataset == 'medqa' and medqa_kg_filter:
        before = len(questions)
        questions = filter_answer_in_kg(questions, graph)
        logger.info('medqa: kept %d/%d questions whose answer is a KG node', len(questions), before)
    questions = sample(questions, limit, seed)
    if not questions:
        raise ValueError(f'{dataset}: no questions to run (limit={limit})')
    logger.info('%s: %d questions, graph %s, schema %s, reference: %s', dataset,
                len(questions), spec.graph_file, spec.schema_mode, spec.paper_reference)

    # Both depend only on fixed inputs: compute once, not once per setting × run. Only
    # per_question schemas differ by question.
    shared = None if spec.schema_mode == 'per_question' else build_schema(
        spec.schema_mode, graph, questions[0])
    schemas = {q.id: shared or build_schema(spec.schema_mode, graph, q) for q in questions}
    hops = {q.id: hop_distance(graph, q.q_entities, q.a_entities) for q in questions}

    settings = SINGLE_BEST_MATCH_SETTINGS[single_best_match]
    report = {'dataset': dataset, 'n_questions': len(questions),
              'question_ids': [q.id for q in questions], 'paper_reference': spec.paper_reference,
              'config': {'limit': limit, 'seed': seed, 'runs': runs, 'iterations': iterations,
                         'single_best_match': single_best_match, 'region': region,
                         'medqa_kg_filter': medqa_kg_filter},
              'settings': {}}
    for sbm in settings:
        # Drop the previous engine (and its node index) before building the next.
        engine = counter = None
        engine, counter = build_engine(graph, model, region, sbm)
        tag = f"single_best_match-{'on' if sbm else 'off'}"
        report['config']['model'] = counter.model_name
        summaries = []
        for r in range(1, runs + 1):
            rel = os.path.join(dataset, tag, f'run{r}')
            summaries.append(run_once(engine, counter, questions, schemas, hops, spec.metric,
                                      iterations, os.path.join(out, rel)))
            if after_run:
                after_run(os.path.join(out, rel), rel)
        report['settings'][tag] = aggregate(summaries, spec.metric)

    agg_path = os.path.join(out, dataset, 'aggregate.json')
    with open(agg_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    for tag, agg in report['settings'].items():
        logger.info('%s %s: %s %.1f (min %.1f, max %.1f over %d runs) | %s', dataset, tag,
                    agg['metric'], agg['mean'], agg['min'], agg['max'], agg['runs'],
                    spec.paper_reference)
    return report


# --- integration-tests stack entry point -------------------------------------

# More than this share of errored questions in any run fails the test: the
# aggregate would be measuring throttling or misconfiguration, not BYOKG.
MAX_ERROR_RATE = 0.05
RESULTS_DIR = os.path.join('benchmark-results', 'byokg-kgqa')


def run_byokg_kgqa_test(handler: IntegrationTestHandler, params: Dict[str, Any], dataset: str):
    s3_uri = env_string('BYOKG_DATA_S3_URI')  # required: no silent local fallback on the stack
    data_dir = env_string('BYOKG_DATA_DIR', 'byokg-data')
    config = dict(
        limit=env_int('BENCHMARK_QA_LIMIT', None),
        runs=env_int('BYOKG_KGQA_RUNS', 3),
        single_best_match=env_string('BYOKG_SINGLE_BEST_MATCH', 'both'),
        iterations=env_int('BYOKG_MAX_ITERATIONS', 2),
    )
    validate_config(config['single_best_match'], config['runs'], config['iterations'])
    fetch_dataset(s3_uri, dataset, data_dir)

    def upload_run(run_dir: str, rel: str) -> None:
        # Each finished run is persisted now: the notebook volume is ephemeral, and a
        # crash in a later run must not take the completed ones with it.
        upload_benchmark_results_to_s3(run_dir, f'byokg-kgqa/{rel}')

    report = run_benchmark(
        dataset, data_dir, RESULTS_DIR, **config,
        model=env_string('TEST_RESPONSE_LLM', None),
        region=env_string('AWS_REGION_NAME', None),
        after_run=upload_run,
    )
    for tag, agg in report['settings'].items():
        handler.add_output(f"{tag}.{agg['metric']}", agg['mean'])
        handler.add_output(f'{tag}.spread', agg['spread'])
        handler.add_output(f'{tag}.showcase_distinct', agg['showcase_distinct'])

    # Adds aggregate.json; re-uploading the already-persisted runs is idempotent.
    upload_benchmark_results_to_s3(os.path.join(RESULTS_DIR, dataset), f'byokg-kgqa/{dataset}')

    runs = [(tag, i, s) for tag, agg in report['settings'].items()
            for i, s in enumerate(agg['per_run'], 1)]

    class ByokgKgqaAssertions(unittest.TestCase):
        def test_error_rate_is_low(self):
            """Every run's errored-question share is within MAX_ERROR_RATE"""
            for tag, i, s in runs:
                self.assertLessEqual(s['n_errors'] / s['n'], MAX_ERROR_RATE,
                                     f"{tag} run{i}: {s['n_errors']}/{s['n']} questions errored")

        def test_scores_are_plausible(self):
            """Aggregate scores are percentages"""
            for tag, agg in report['settings'].items():
                self.assertTrue(0.0 <= agg['mean'] <= 100.0, f"{tag}: mean {agg['mean']}")

    handler.run_assertions(ByokgKgqaAssertions)


class _ByokgKgqaBaseline(IntegrationTestBase):
    dataset: str = ''

    @property
    def description(self):
        return (f'BYOKG KGQA baseline on {self.dataset}; '
                f'reference: {DATASETS[self.dataset].paper_reference}')

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        run_byokg_kgqa_test(handler, params, self.dataset)


class WebqspByokgKgqa(_ByokgKgqaBaseline):
    dataset = 'webqsp'


class CwqByokgKgqa(_ByokgKgqaBaseline):
    dataset = 'cwq'


class CronquestionsByokgKgqa(_ByokgKgqaBaseline):
    dataset = 'cronquestions'


class MedqaByokgKgqa(_ByokgKgqaBaseline):
    dataset = 'medqa'


# --- local CLI ---------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--dataset', required=True, choices=sorted(DATASETS))
    p.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    p.add_argument('--s3-data', help="fetch this dataset's files from this S3 prefix into --data-dir")
    p.add_argument('--out', default=RESULTS_DIR)
    p.add_argument('--s3-out', help='sync --out to this S3 prefix when done')
    p.add_argument('--limit', type=int, help='seeded random subset size (default: all)')
    p.add_argument('--seed', type=int, default=0)
    p.add_argument('--runs', type=int, default=3, help='repeats per setting, for the noise floor')
    p.add_argument('--single-best-match', choices=['off', 'on', 'both'], default='both')
    p.add_argument('--iterations', type=int, default=2, help='retrieval iterations (library default 2)')
    p.add_argument('--model', help='Bedrock model id (default: ByoKGConfig.llm_model)')
    p.add_argument('--region')
    p.add_argument('--no-medqa-kg-filter', action='store_true',
                   help='keep MedQA questions whose answer is not a KG node')
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    args = parse_args(argv)
    validate_config(args.single_best_match, args.runs, args.iterations)
    if args.s3_data:
        fetch_dataset(args.s3_data, args.dataset, args.data_dir)
    run_benchmark(args.dataset, args.data_dir, args.out, limit=args.limit, seed=args.seed,
                  runs=args.runs, single_best_match=args.single_best_match,
                  iterations=args.iterations, model=args.model, region=args.region,
                  medqa_kg_filter=not args.no_medqa_kg_filter)
    if args.s3_out:
        _aws('sync', args.out, args.s3_out)
    return 0


if __name__ == '__main__':
    sys.exit(main())
