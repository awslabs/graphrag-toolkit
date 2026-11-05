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
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
import unittest
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

from benchmarks.scripts.integration_test_base import IntegrationTestBase
from benchmarks.scripts.integration_test_handler import IntegrationTestHandler
from benchmarks.utils.benchmark_env import env_bool, env_int, env_string
from benchmarks.utils.s3_utils import upload_benchmark_results_to_s3
from benchmarks.utils.byokg_kgqa import (
    DATASETS, Question, answer_in_context, build_schema, f1, filter_answer_in_kg,
    hit, hit_at_2, hop_distance, load_questions, sample, seed_recall, showcase_reasons,
    summarize, validate_graph,
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


@dataclass(frozen=True)
class RetrievalConfig:
    """Optional retrieval changes under test; all off reproduces the library defaults."""
    text_retrieval: bool = False      # TextTripletRetriever merged with agentic retrieval
    text_topk: int = 10
    embed_model: Optional[str] = None  # None: ByoKGConfig.embed_model

    def __post_init__(self):
        if not isinstance(self.text_topk, int) or self.text_topk < 1:
            raise ValueError(f'text_topk must be a positive integer, got {self.text_topk!r}')

    @property
    def enabled(self) -> bool:
        return self.text_retrieval

    def resolved_model(self) -> str:
        if self.embed_model:
            return self.embed_model
        from graphrag_toolkit.byokg_rag.config import ByoKGConfig
        return ByoKGConfig.embed_model


EMBED_MAX_ATTEMPTS = 10


def embedding_cache_key(graph_sha256: str, model: str) -> str:
    """Directory name for a graph's embeddings: changes with the graph or the model."""
    return f"{graph_sha256[:16]}-{re.sub(r'[^A-Za-z0-9._-]', '_', model)}"


@dataclass
class RetrievalParts:
    """Built once per dataset and shared by every engine: embedding a graph is costly."""
    text_triplet_retriever: Any = None
    embedding_dim: Optional[int] = None


def build_retrieval_parts(graph: Dict[str, Any], retrieval: RetrievalConfig, region: Optional[str],
                          cache_dir: Optional[str]) -> RetrievalParts:
    if not retrieval.enabled:
        return RetrievalParts()
    from graphrag_toolkit.byokg_rag.config import ByoKGConfig
    from graphrag_toolkit.byokg_rag.graph_retrievers import TextTripletRetriever
    from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
    from graphrag_toolkit.byokg_rag.indexing import GraphEmbeddings

    from botocore.config import Config

    model_id = retrieval.resolved_model()
    # A graph is thousands of embedding calls: adaptive retries ride out throttling
    # that the default 5 attempts would turn into a failed build.
    kwargs = {'model_id': model_id,
              'config': Config(retries={'max_attempts': EMBED_MAX_ATTEMPTS, 'mode': 'adaptive'})}
    if region:
        kwargs['region_name'] = region
    embedding = ByoKGConfig.to_embedding(**kwargs)
    store = LocalKGStore(graph)
    triplets = store.get_triplets()
    # Every named node, not only those in triplets, so one cached node table serves
    # any consumer of the graph's embeddings.
    embeddings = GraphEmbeddings.build(triplets, embedding, cache_dir, extra_nodes=store.nodes(),
                                       model_id=model_id)
    return RetrievalParts(
        text_triplet_retriever=TextTripletRetriever(triplets, embeddings, embedding,
                                                    topk=retrieval.text_topk),
        embedding_dim=embeddings.dim,
    )


def build_engine(graph: Dict[str, Any], model: Optional[str], region: Optional[str],
                 single_best_match: bool, parts: Optional[RetrievalParts] = None):
    """Engine with library defaults; only the graph store, LLM and any parts under
    test are supplied."""
    from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine
    from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
    from graphrag_toolkit.byokg_rag.llm.bedrock_llms import BedrockGenerator

    parts = parts or RetrievalParts()
    counter = CountingGenerator(BedrockGenerator(model_name=model, region_name=region))
    engine = ByoKGQueryEngine(
        graph_store=LocalKGStore(graph), llm_generator=counter, single_best_match=single_best_match,
        text_triplet_retriever=parts.text_triplet_retriever)
    return engine, counter


def _tree_sha256(root: str, suffixes: Sequence[str]) -> str:
    h = hashlib.sha256()
    # Pruning and sorting dirnames in place makes os.walk skip __pycache__ and visit
    # directories in a fixed order (sorting the walk itself would consume it first).
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != '__pycache__')
        for name in sorted(filenames):
            if name.endswith(tuple(suffixes)):
                path = os.path.join(dirpath, name)
                h.update(os.path.relpath(path, root).encode())
                with open(path, 'rb') as f:
                    h.update(f.read())
    return h.hexdigest()


def _file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


# The benchmark's own code: scoring, preprocessing and orchestration. It lives in this
# repo, not in byokg_rag, so the package hash cannot see a scorer change.
BENCHMARK_CODE = (os.path.abspath(__file__),
                  os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'utils',
                               'byokg_kgqa.py'))


def provenance(counter, data_files: Optional[Dict[str, str]] = None,
               retrieval: Optional[RetrievalConfig] = None,
               data_sha256: Optional[Dict[str, str]] = None,
               embedding_dim: Optional[int] = None) -> Dict[str, Any]:
    """What produced a result, so two runs can be shown to differ only in the code
    under test.

    Content hashes are the fingerprint: of the byokg-rag package and its prompts
    (the stack copies it in as source, with no version or .git), of this benchmark's
    own code, and of the data files actually loaded (data_files: {label: path}), so
    a scorer change or a wrong graph file at the expected path is visible. The git
    SHA (GRAPHRAG_TOOLKIT_GIT_SHA from build-tests.sh, else a local checkout) is
    recorded for humans but not trusted: '-dirty' stays the same across edits.

    'retrieval' records the optional retrieval changes, and the embedding model only
    when one is in use, so baseline and optimized runs fingerprint differently.
    data_sha256 skips re-hashing files the caller already hashed.
    """
    git_sha = os.environ.get('GRAPHRAG_TOOLKIT_GIT_SHA')
    if not git_sha:
        try:
            git_sha = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=os.path.dirname(__file__),
                                     check=True, capture_output=True, text=True).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            git_sha = None
    try:
        import graphrag_toolkit.byokg_rag as byokg_rag
        root = os.path.dirname(byokg_rag.__file__)
        source, prompts = _tree_sha256(root, ('.py', '.yaml')), _tree_sha256(root, ('.yaml',))
    except ImportError:
        source = prompts = None
    code = hashlib.sha256()
    for path in BENCHMARK_CODE:
        code.update(_file_sha256(path).encode())
    return {
        'git_sha': git_sha,
        'byokg_rag_source_sha256': source,
        'byokg_rag_prompts_sha256': prompts,
        'benchmark_code_sha256': code.hexdigest(),
        'data_sha256': data_sha256 if data_sha256 is not None else {
            label: _file_sha256(path) for label, path in (data_files or {}).items()},
        'inference': {name: getattr(counter, name, None) for name in (
            'model_name', 'max_new_tokens', 'inference_config', 'reasoning_config', 'max_retries')},
        'retrieval': retrieval_provenance(retrieval or RetrievalConfig(), embedding_dim),
    }


def retrieval_provenance(retrieval: RetrievalConfig, embedding_dim: Optional[int]) -> Dict[str, Any]:
    record = asdict(retrieval)
    # Settings only count when they are used, so unused ones never split the fingerprint.
    record['embed_model'] = retrieval.resolved_model() if retrieval.enabled else None
    record['text_topk'] = retrieval.text_topk if retrieval.text_retrieval else None
    record['embedding_dim'] = embedding_dim
    return record


def linked_nodes(result) -> List[str]:
    """The nodes ByoKGQueryEngine seeds from one EntityLinker.link() result.

    Grouped results (List[List[str]], the single_best_match path) seed the first
    candidate per mention; flat results (List[str]) seed every node; a return_dict
    result seeds its hits.
    """
    if isinstance(result, dict):
        return [hit['document_id'] for hit in result['hits']]
    if isinstance(result, list) and result and all(isinstance(c, list) for c in result):
        return [candidates[0] for candidates in result if candidates]
    if isinstance(result, list) and all(isinstance(n, str) for n in result):
        return list(result)
    raise TypeError(f'unexpected EntityLinker.link() result: {result!r}')


def run_question(engine, counter: CountingGenerator, question: Question,
                 schema: Dict[str, Any], hops: Optional[int], iterations: int) -> Dict[str, Any]:
    """Score one question. Exceptions are recorded, not raised, and count as misses.

    schema and hops are precomputed by run_benchmark, so nothing here can raise
    outside the try.
    """
    artifacts: List[Dict[str, List[str]]] = []
    linker = engine.kg_linker
    shadowed = 'parse_response' in vars(linker)  # normally a class attribute
    parse = linker.parse_response
    seeds: List[str] = []
    entity_linker = engine.entity_linker
    link_shadowed = 'link' in vars(entity_linker)
    link = entity_linker.link

    def recording_parse(response):
        parsed = parse(response)
        artifacts.append(parsed)
        return parsed

    def recording_link(*args, **kwargs):
        result = link(*args, **kwargs)
        seeds.extend(linked_nodes(result))
        return result

    record: Dict[str, Any] = {
        'id': question.id, 'question': question.question, 'answers': question.answers,
        'q_entities': question.q_entities, 'qtype': question.qtype, 'hops': hops,
    }
    linker.parse_response = recording_parse
    entity_linker.link = recording_link
    try:
        engine.schema = schema
        counter.calls = 0
        start = time.monotonic()
        context = engine.query(question.question, iterations=iterations)
        predictions, _ = engine.generate_response(question.question, '\n'.join(context))
        record.update(latency_s=round(time.monotonic() - start, 2), llm_calls=counter.calls,
                      predictions=predictions, context=context,
                      context_chars=sum(len(c) for c in context), error=None)
    except Exception as e:  # recorded per question; the run continues
        logger.exception('question %s failed', question.id)
        record.update(predictions=[], context=[], context_chars=0, latency_s=None,
                      llm_calls=None, error=f'{type(e).__name__}: {e}')
    finally:
        if shadowed:
            linker.parse_response = parse
        else:
            del linker.parse_response  # back to the class lookup, exactly as before
        if link_shadowed:
            entity_linker.link = link
        else:
            del entity_linker.link
    record['kg_linker_artifacts'] = artifacts
    # Mentions and draft answers both seed retrieval, so both count.
    record['linked_seeds'] = list(dict.fromkeys(seeds))
    record['seed_recall'] = seed_recall(question.q_entities, record['linked_seeds'])

    # No-KG answer: the BYOKG answer prompt with an empty graph context. Feeds the
    # LLM-only baseline, MedQA's H@2 second guess and the showcase gate. Its own try,
    # so a throttle here cannot turn a BYOKG success into a miss; outside the timer
    # and the call count.
    try:
        record['vanilla_predictions'], _ = engine.generate_response(question.question, '')
        record['vanilla_error'] = None
    except Exception as e:
        logger.exception('question %s: no-KG answer failed', question.id)
        record.update(vanilla_predictions=[], vanilla_error=f'{type(e).__name__}: {e}')

    record['hit'] = hit(record['predictions'], question.answers)
    record['vanilla_hit'] = hit(record['vanilla_predictions'], question.answers)
    # An errored KG path is a miss here too: H@2 must not fall back to the no-KG guess alone.
    record['hit@2'] = 0 if record['error'] else hit_at_2(
        record['predictions'], record['vanilla_predictions'], question.answers)
    record['f1'] = f1(record['predictions'], question.answers)
    record['answer_in_context'] = answer_in_context(record['context'], question.answers)
    record['showcase'] = showcase_reasons(record)
    return record


def run_once(engine, counter, questions: Sequence[Question], schemas: Dict[str, Any],
             hops: Dict[str, Optional[int]], metric: str, iterations: int,
             out_dir: str, run_key: Optional[str] = None) -> Dict[str, Any]:
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
            if i == ABORT_IF_FIRST_N_FAIL:
                # Either path failing throughout makes the run unusable (the error gate
                # would reject it at the end), so stop before paying for the rest.
                for field_ in ('error', 'vanilla_error'):
                    if all(r[field_] for r in records):
                        raise RuntimeError(f'first {i} questions all failed ({field_}); '
                                           f'last: {record[field_]}')
            if i % 25 == 0:
                logger.info('%s: %d/%d, running Hit %.1f', out_dir, i, len(questions),
                            100 * sum(r['hit'] for r in records) / i)
    summary = summarize(records, metric)
    summary['run_key'] = run_key
    with open(os.path.join(out_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    for count, field_ in (('n_errors', 'error'), ('n_vanilla_errors', 'vanilla_error')):
        if summary[count]:
            logger.warning('%s: %d/%d questions errored — see "%s" in predictions.jsonl',
                           out_dir, summary[count], summary['n'], field_)
    return summary


def aggregate(summaries: Sequence[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    key = 'hit@2' if metric == 'hit@2' else 'hit'
    scores = [s[key] for s in summaries]
    # The repeats share one question list, so showcase counts are per run (mean) or
    # distinct questions across runs — never summed, which multiplies by the run count.
    distinct = set().union(*(s['showcase_ids'] for s in summaries))
    recalls = [s['seed_recall'] for s in summaries if s.get('seed_recall') is not None]
    return {'metric': key, 'runs': len(scores), 'mean': round(sum(scores) / len(scores), 1),
            'seed_recall_mean': round(sum(recalls) / len(recalls), 1) if recalls else None,
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


# More than this share of errored questions in any run — on the KG path or the no-KG
# baseline — fails the test, and keeps resume from reusing the run: the result would
# be measuring throttling or misconfiguration, not BYOKG.
MAX_ERROR_RATE = 0.05


def error_rates(summary: Dict[str, Any]) -> Dict[str, float]:
    return {'KG path': summary['n_errors'] / summary['n'],
            'no-KG baseline': summary['n_vanilla_errors'] / summary['n']}


def run_key(question_ids: Sequence[str], single_best_match: bool, iterations: int,
            prov: Dict[str, Any]) -> str:
    """Fingerprint of every input that determines a run's result.

    Inputs only: two runs with the same key can still differ by sampling or
    throttling; that is what the repeats and the error-rate gate are for. Content
    hashes, not git_sha, cover the code and data — a '-dirty' SHA does not change
    with further edits.
    """
    payload = {'question_ids': list(question_ids), 'single_best_match': single_best_match,
               'iterations': iterations, 'inference': prov['inference'],
               'byokg_rag_source': prov['byokg_rag_source_sha256'],
               'benchmark_code': prov['benchmark_code_sha256'], 'data': prov['data_sha256'],
               'retrieval': prov['retrieval']}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def completed_run(run_dir: str, key: str) -> Optional[Dict[str, Any]]:
    """The summary of a finished, healthy run with this exact fingerprint, or None
    to run it.

    A summary.json left by a different configuration fails loudly rather than being
    reused or silently overwritten. One whose error rates exceed MAX_ERROR_RATE is
    rerun, so a throttled run is never carried forward.
    """
    path = os.path.join(run_dir, 'summary.json')
    if not os.path.exists(path):
        return None
    with open(path, encoding='utf-8') as f:
        summary = json.load(f)
    if summary.get('run_key') != key:
        raise RuntimeError(f'{path} was produced by a different configuration '
                           f'(run_key {summary.get("run_key")!r} != {key!r}); '
                           f'use a fresh output directory or --no-resume')
    unhealthy = {k: v for k, v in error_rates(summary).items() if v > MAX_ERROR_RATE}
    if unhealthy:
        logger.warning('%s: rerunning, error rates over %.0f%%: %s', run_dir,
                       100 * MAX_ERROR_RATE, unhealthy)
        return None
    return summary


def embedding_cache_dir(data_dir: str, graph_sha256: str, retrieval: RetrievalConfig) -> str:
    return os.path.join(data_dir, 'embeddings',
                        embedding_cache_key(graph_sha256, retrieval.resolved_model()))


def embedding_cache_complete(cache_dir: str) -> bool:
    # GraphEmbeddings writes vectors.npy last in each table.
    return all(os.path.exists(os.path.join(cache_dir, table, 'vectors.npy'))
               for table in ('nodes', 'relations'))


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
                  after_run: Optional[Callable[[str, str], None]] = None,
                  resume: bool = True,
                  retrieval: Optional[RetrievalConfig] = None,
                  before_embeddings: Optional[Callable[[str], None]] = None,
                  after_embeddings: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Run every (single_best_match setting × repeat) and write aggregate.json.

    after_run(run_dir, relative_path) is called as each run finishes, so callers can
    persist completed runs before a later one crashes. With resume, a run whose
    summary.json already exists for the same fingerprint is reused, not rerun.
    retrieval switches on the optional retrieval changes; their embeddings are
    cached under data_dir/embeddings/<embedding_cache_key>. before_embeddings(cache_dir)
    runs before they are loaded or built (to fetch a shared cache) and
    after_embeddings(cache_dir) once they exist, before any question runs.
    """
    validate_config(single_best_match, runs, iterations)
    retrieval = retrieval or RetrievalConfig()
    spec = DATASETS[dataset]
    data_sha256 = {'questions': _file_sha256(os.path.join(data_dir, spec.questions_file)),
                   'graph': _file_sha256(os.path.join(data_dir, spec.graph_file))}
    with open(os.path.join(data_dir, spec.graph_file), encoding='utf-8') as f:
        graph = json.load(f)
    validate_graph(graph, spec.graph_file)
    questions = load_questions(dataset, data_dir)
    if dataset == 'medqa' and medqa_kg_filter:
        before = len(questions)
        questions = filter_answer_in_kg(questions, graph)
        logger.info('medqa: kept %d/%d questions whose answer is a KG node', len(questions), before)
    n_available = len(questions)
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
    cache_dir = (embedding_cache_dir(data_dir, data_sha256['graph'], retrieval)
                 if retrieval.enabled else None)
    if cache_dir and before_embeddings:
        before_embeddings(cache_dir)
    parts = build_retrieval_parts(graph, retrieval, region, cache_dir)
    if cache_dir and after_embeddings:
        after_embeddings(cache_dir)

    settings = SINGLE_BEST_MATCH_SETTINGS[single_best_match]
    report = {'dataset': dataset, 'n_questions': len(questions),
              # n_questions < n_available means a seeded sample of the set.
              'n_available': n_available,
              'question_ids': [q.id for q in questions], 'paper_reference': spec.paper_reference,
              'config': {'limit': limit, 'seed': seed, 'runs': runs, 'iterations': iterations,
                         'single_best_match': single_best_match, 'region': region,
                         'medqa_kg_filter': medqa_kg_filter},
              'settings': {}}
    for sbm in settings:
        # Drop the previous engine (and its node index) before building the next.
        engine = counter = None
        engine, counter = build_engine(graph, model, region, sbm, parts)
        tag = f"single_best_match-{'on' if sbm else 'off'}"
        report['config']['model'] = counter.model_name
        if 'provenance' not in report:  # once: it hashes the byokg-rag package
            report['provenance'] = provenance(counter, retrieval=retrieval, data_sha256=data_sha256,
                                              embedding_dim=parts.embedding_dim)
        prov = report['provenance']
        key = run_key(report['question_ids'], sbm, iterations, prov)
        summaries = []
        for r in range(1, runs + 1):
            rel = os.path.join(dataset, tag, f'run{r}')
            done = completed_run(os.path.join(out, rel), key) if resume else None
            if done:
                logger.info('%s: already complete with this configuration, reusing', rel)
                summaries.append(done)
                continue
            summaries.append(run_once(engine, counter, questions, schemas, hops, spec.metric,
                                      iterations, os.path.join(out, rel), run_key=key))
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

RESULTS_DIR = os.path.join('benchmark-results', 'byokg-kgqa')


def dataset_limit(dataset: str) -> Optional[int]:
    """BYOKG_<DATASET>_LIMIT, else the harness-wide BENCHMARK_QA_LIMIT.

    One suite run covers all four datasets, and the global limit samples every one of
    them — including CronQuestions, whose references are on its full 600. The
    per-dataset variable samples WebQSP and CWQ while the others run in full.
    """
    specific = env_int(f'BYOKG_{dataset.upper()}_LIMIT', None)
    return specific if specific is not None else env_int('BENCHMARK_QA_LIMIT', None)


def retrieval_from_env() -> RetrievalConfig:
    return RetrievalConfig(
        text_retrieval=env_bool('BYOKG_TEXT_RETRIEVAL', False),
        text_topk=env_int('BYOKG_TEXT_TOPK', 10),
        embed_model=env_string('BYOKG_EMBED_MODEL', None),
    )


def _s3_client():
    import boto3
    return boto3.client('s3')


def _split_s3_uri(uri: str):
    if not uri.startswith('s3://'):
        raise ValueError(f'expected an s3:// URI, got {uri!r}')
    bucket, _, key = uri[len('s3://'):].partition('/')
    return bucket, key


def _vectors_last(path: str):
    # vectors.npy marks a cache complete, so it moves last in both directions.
    return (path.endswith('vectors.npy'), path)


def sync_embedding_cache(s3_uri: str, local_dir: str, upload: bool) -> None:
    """Fetch (or publish) one graph's embedding cache under <s3_uri>/embeddings/<key>/.

    Embedding a graph is thousands of model calls, so later stacks reuse the first
    stack's vectors instead of recomputing them. A prefix with no objects yet fetches
    nothing, and the cache is built locally. In-progress checkpoints are not published.
    """
    bucket, prefix = _split_s3_uri(f"{s3_uri.rstrip('/')}/embeddings/{os.path.basename(local_dir)}/")
    client = _s3_client()
    if upload:
        files = [os.path.join(root, name) for root, _, names in os.walk(local_dir) for name in names]
        rels = [os.path.relpath(f, local_dir).replace(os.sep, '/') for f in files]
        for rel in sorted((r for r in rels if 'partial' not in r.split('/')), key=_vectors_last):
            client.upload_file(os.path.join(local_dir, *rel.split('/')), bucket, prefix + rel)
        return
    keys = [obj['Key'] for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get('Contents', []) if not obj['Key'].endswith('/')]  # skip folder markers
    root = os.path.abspath(local_dir)
    downloads = []
    for key in sorted(keys, key=_vectors_last):
        dest = os.path.abspath(os.path.join(root, *key[len(prefix):].split('/')))
        if os.path.commonpath([root, dest]) != root:  # e.g. a '..' segment in the key
            raise ValueError(f'S3 key {key!r} resolves outside {root}')
        downloads.append((key, dest))
    for key, dest in downloads:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        client.download_file(bucket, key, dest)
    logger.info('fetched %d embedding cache files from s3://%s/%s', len(keys), bucket, prefix)


def fetch_embedding_cache(s3_uri: str, cache_dir: str) -> bool:
    """Fetch the shared cache unless a complete one is already local; return whether
    a complete cache is now local."""
    if not embedding_cache_complete(cache_dir):
        sync_embedding_cache(s3_uri, cache_dir, upload=False)
    return embedding_cache_complete(cache_dir)


def run_byokg_kgqa_test(handler: IntegrationTestHandler, params: Dict[str, Any], dataset: str):
    s3_uri = env_string('BYOKG_DATA_S3_URI')  # required: no silent local fallback on the stack
    data_dir = env_string('BYOKG_DATA_DIR', 'byokg-data')
    config = dict(
        limit=dataset_limit(dataset),
        runs=env_int('BYOKG_KGQA_RUNS', 3),
        single_best_match=env_string('BYOKG_SINGLE_BEST_MATCH', 'both'),
        iterations=env_int('BYOKG_MAX_ITERATIONS', 2),
    )
    validate_config(config['single_best_match'], config['runs'], config['iterations'])
    retrieval = retrieval_from_env()
    fetch_dataset(s3_uri, dataset, data_dir)
    shared = {}

    def fetch_embeddings(cache_dir: str) -> None:
        shared['complete'] = fetch_embedding_cache(s3_uri, cache_dir)

    def publish_embeddings(cache_dir: str) -> None:
        # Published as soon as it exists: a later crash must not cost the embedding calls.
        if not shared.get('complete'):
            sync_embedding_cache(s3_uri, cache_dir, upload=True)

    def upload_run(run_dir: str, rel: str) -> None:
        # Each finished run is persisted now: the notebook volume is ephemeral, and a
        # crash in a later run must not take the completed ones with it.
        upload_benchmark_results_to_s3(run_dir, f'byokg-kgqa/{rel}')

    report = run_benchmark(
        dataset, data_dir, RESULTS_DIR, **config,
        model=env_string('TEST_RESPONSE_LLM', None),
        region=env_string('AWS_REGION_NAME', None),
        after_run=upload_run,
        retrieval=retrieval,
        before_embeddings=fetch_embeddings,
        after_embeddings=publish_embeddings,
    )
    for tag, agg in report['settings'].items():
        handler.add_output(f"{tag}.{agg['metric']}", agg['mean'])
        handler.add_output(f'{tag}.spread', agg['spread'])
        handler.add_output(f'{tag}.showcase_distinct', agg['showcase_distinct'])
        handler.add_output(f'{tag}.seed_recall', agg['seed_recall_mean'])
        # Published scores were measured with other models (and, where the reference
        # key is suffixed, other question sets): context for a reader, not a
        # like-for-like comparison, hence the paper_ prefix.
        for model, scores in DATASETS[dataset].references.items():
            for kind, value in scores.items():
                handler.add_output(f'{tag}.paper_delta_vs_{model}_{kind}',
                                   round(agg['mean'] - value, 1))
    handler.add_output('provenance', report['provenance'])

    # Adds aggregate.json; re-uploading the already-persisted runs is idempotent.
    upload_benchmark_results_to_s3(os.path.join(RESULTS_DIR, dataset), f'byokg-kgqa/{dataset}')

    runs = [(tag, i, s) for tag, agg in report['settings'].items()
            for i, s in enumerate(agg['per_run'], 1)]

    class ByokgKgqaAssertions(unittest.TestCase):
        def test_error_rate_is_low(self):
            """Every run's errored share is within MAX_ERROR_RATE, on the KG path and on
            the no-KG baseline the KG-vs-no-KG comparison rests on"""
            for tag, i, s in runs:
                for path, rate in error_rates(s).items():
                    self.assertLessEqual(rate, MAX_ERROR_RATE,
                                         f'{tag} run{i}: {path} errored on {rate:.1%} of questions')

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
    p.add_argument('--no-resume', action='store_true',
                   help='rerun runs that already have a summary.json for this configuration')
    p.add_argument('--text-retrieval', action='store_true',
                   help='merge text-based triplet retrieval with agentic retrieval')
    p.add_argument('--text-topk', type=int, default=10, help='triplets from text retrieval')
    p.add_argument('--embed-model', help='embedding model id (default: ByoKGConfig.embed_model)')
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    args = parse_args(argv)
    validate_config(args.single_best_match, args.runs, args.iterations)
    retrieval = RetrievalConfig(text_retrieval=args.text_retrieval,
                                text_topk=args.text_topk, embed_model=args.embed_model)
    if args.s3_data:
        fetch_dataset(args.s3_data, args.dataset, args.data_dir)
    # Reuse vectors a stack already paid for instead of re-embedding the graph.
    fetch = ((lambda cache_dir: fetch_embedding_cache(args.s3_data, cache_dir))
             if args.s3_data and retrieval.enabled else None)
    run_benchmark(args.dataset, args.data_dir, args.out, limit=args.limit, seed=args.seed,
                  runs=args.runs, single_best_match=args.single_best_match,
                  iterations=args.iterations, model=args.model, region=args.region,
                  medqa_kg_filter=not args.no_medqa_kg_filter, resume=not args.no_resume,
                  retrieval=retrieval, before_embeddings=fetch)
    if args.s3_out:
        _aws('sync', args.out, args.s3_out)
    return 0


if __name__ == '__main__':
    sys.exit(main())
