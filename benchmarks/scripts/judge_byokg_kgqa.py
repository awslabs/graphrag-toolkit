# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Score saved BYOKG KGQA runs with an LLM judge (Claude Opus 5.5 on Bedrock).

Reads every <dataset>/<setting>/<run>/predictions.jsonl under --results and writes,
next to each, judge.jsonl (per-question verdicts beside the recomputed string scores)
and judge_summary.json, plus judge_aggregate.json at --results. Verdicts are cached in
--cache, so a rerun pays only for answers not judged before.

  python -m benchmarks.scripts.judge_byokg_kgqa --results ~/byokg-baseline --dry-run
  python -m benchmarks.scripts.judge_byokg_kgqa --results ~/byokg-baseline --limit 50
  python -m benchmarks.scripts.judge_byokg_kgqa --results ~/byokg-baseline

Needs the anthropic SDK with Bedrock support (installed with lexical-graph) and
bedrock:InvokeModel on the judge's inference profile.
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence

from benchmarks.utils.byokg_judge import (
    JUDGE_MODEL_ID, JUDGE_NAME, RUBRIC_VERSION, SYSTEM_PROMPT, Judge, JudgeCache,
    find_runs, headline, read_records, record_units, score_record, summarize_judged, unique_units,
)
from benchmarks.utils.byokg_kgqa import DATASETS

logger = logging.getLogger(__name__)

# Rough figures for --dry-run: global-profile list prices per MTok, and an assumed
# output size (thinking at low effort plus the verdict). The --limit smoke run measures both.
PRICE_PER_MTOK = {'input': 4.0, 'output': 20.0}
ASSUMED_OUTPUT_TOKENS = 300
# More than this share without a verdict (refusal, truncated, invalid) stops judging
# early and, per run, fails the scoring: the scores would describe the judge's failures.
MAX_UNSCORED_SHARE = 0.05


def make_client(region: str):
    import anthropic
    # Structured output's first call can take ~90 s while its grammar compiles.
    return anthropic.AnthropicBedrock(aws_region=region, timeout=180.0, max_retries=8)


def judge_units(judge: Judge, units: Sequence, workers: int, max_unscored_share: float = MAX_UNSCORED_SHARE,
                min_before_abort: int = 40) -> Dict[str, int]:
    """Judge every unit not yet cached; returns the statuses of the new verdicts and the
    tokens they used. Raises once the unscored share passes max_unscored_share."""
    totals = defaultdict(int)
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(judge.judge, unit) for unit in units]
        try:
            for future in as_completed(futures):
                verdict = future.result()  # API errors past the SDK's retries stop the run here
                done += 1
                totals[verdict['status']] += 1
                totals['input_tokens'] += verdict['usage']['input']
                totals['output_tokens'] += verdict['usage']['output']
                unscored = done - totals['ok']
                if done >= min_before_abort and unscored / done > max_unscored_share:
                    raise RuntimeError(f'{unscored}/{done} judged units got no verdict '
                                       f'({dict(totals)}); stopping before paying for the rest')
                if done % 200 == 0:
                    logger.info('judged %d/%d new units', done, len(units))
        except BaseException:
            for f in futures:
                f.cancel()
            raise
    return dict(totals)


def write_outputs(results: str, runs, judge: Judge, allow_unscored: bool = False) -> Dict[str, Any]:
    """Score every run from its verdicts; write per-run files and the aggregate. Raises
    after writing them (marked gate.passed false) when any judged metric of a run lacks a
    verdict for more than MAX_UNSCORED_SHARE of its questions, unless allow_unscored."""
    meta = {'name': JUDGE_NAME, 'model_id': judge.model_id, 'effort': judge.effort,
            'rubric_version': RUBRIC_VERSION}
    by_setting: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    over = []
    for group, dataset, setting, run, path in runs:
        rows = []
        for record in read_records(path):
            units = record_units(dataset, record)
            verdicts = {role: (judge.cached(unit) if unit is not None else None) for role, unit in units.items()}
            missing = [role for role, unit in units.items() if unit is not None and verdicts[role] is None]
            if missing:
                raise RuntimeError(f'{path}: {record["id"]} has no cached verdict for {missing}')
            rows.append(score_record(dataset, record, verdicts))
        run_dir = os.path.dirname(path)
        with open(os.path.join(run_dir, 'judge.jsonl'), 'w', encoding='utf-8') as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        summary = summarize_judged(dataset, rows, meta)
        failing = [m for m, n in summary['n_unscored'].items() if rows and n / len(rows) > MAX_UNSCORED_SHARE]
        summary['gate'] = {'max_unscored_share': MAX_UNSCORED_SHARE, 'passed': not failing, 'over': failing}
        if failing:
            over.append(f'{path}: ' + ', '.join(f'{m} {summary["n_unscored"][m]}/{len(rows)}' for m in failing))
        with open(os.path.join(run_dir, 'judge_summary.json'), 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        by_setting[(group, dataset, setting)].append({'run': run, **summary})

    aggregate = {'judge': meta, 'settings': {}}
    for (group, dataset, setting), summaries in sorted(by_setting.items()):
        key = headline(dataset)
        string_key = key.replace('judge_', '')
        scores = [s[key] for s in summaries if s[key] is not None]
        name = '/'.join(part for part in (group, dataset, setting) if part != '.')
        aggregate['settings'][name] = {
            'metric': key, 'runs': len(summaries),
            'mean': round(sum(scores) / len(scores), 1) if scores else None,
            'min': min(scores, default=None), 'max': max(scores, default=None),
            'string_mean': round(sum(s[string_key] for s in summaries) / len(summaries), 1),
            'n_unscored': {m: sum(s['n_unscored'][m] for s in summaries) for m in summaries[0]['n_unscored']},
            'gate_passed': all(s['gate']['passed'] for s in summaries),
            'per_run': summaries}
    with open(os.path.join(results, 'judge_aggregate.json'), 'w', encoding='utf-8') as f:
        json.dump(aggregate, f, indent=2)
    if over and not allow_unscored:
        raise RuntimeError(f'more than {MAX_UNSCORED_SHARE:.0%} of questions have no verdict in: {"; ".join(over)} '
                           f'(rerun with a larger --max-tokens to retry truncated ones, or pass --allow-unscored)')
    return aggregate


def estimate(units: Sequence) -> Dict[str, Any]:
    input_tokens = sum((len(SYSTEM_PROMPT) + len(u.prompt())) / 4 for u in units)
    output_tokens = ASSUMED_OUTPUT_TOKENS * len(units)
    cost = (input_tokens * PRICE_PER_MTOK['input'] + output_tokens * PRICE_PER_MTOK['output']) / 1e6
    return {'calls': len(units), 'input_tokens': round(input_tokens), 'output_tokens': output_tokens,
            'usd_rough': round(cost, 2)}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--results', required=True, help='directory holding <dataset>/<setting>/<run>/predictions.jsonl')
    p.add_argument('--cache', help='verdict cache (default: <results>/judge_cache.jsonl)')
    p.add_argument('--model', default=JUDGE_MODEL_ID)
    p.add_argument('--region', default='us-west-2')
    p.add_argument('--effort', default='low', choices=['low', 'medium', 'high', 'xhigh', 'max'])
    p.add_argument('--max-tokens', type=int, default=2048)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--dataset', action='append', choices=sorted(DATASETS), help='only these datasets')
    p.add_argument('--setting', action='append', help='only these settings, e.g. single_best_match-on')
    p.add_argument('--limit', type=int, help='judge at most this many new units (a smoke run); writes no scores')
    p.add_argument('--dry-run', action='store_true', help='count the calls needed and estimate their cost')
    p.add_argument('--allow-unscored', action='store_true',
                   help=f'write scores even when over {MAX_UNSCORED_SHARE:.0%} of a run has no verdict')
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None, client=None) -> int:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    args = parse_args(argv)
    runs = [r for r in find_runs(args.results)
            if (not args.dataset or r[1] in args.dataset) and (not args.setting or r[2] in args.setting)]
    if not runs:
        raise SystemExit(f'no predictions.jsonl under {args.results} for the selected datasets/settings')
    cache = JudgeCache(args.cache or os.path.join(args.results, 'judge_cache.jsonl'))
    judge = Judge(client, cache, model_id=args.model, effort=args.effort, max_tokens=args.max_tokens)
    units = unique_units((u for _, dataset, _, _, path in runs for record in read_records(path)
                          for u in record_units(dataset, record).values()), args.model, args.effort)
    todo = [u for key, u in sorted(units.items()) if cache.get(key) is None]
    logger.info('%d runs, %d distinct answer lists, %d not judged yet', len(runs), len(units), len(todo))
    if args.dry_run:
        print(json.dumps(estimate(todo), indent=2))
        return 0
    if args.limit is not None:
        todo = todo[:args.limit]
    if todo:
        judge.client = judge.client or make_client(args.region)
        totals = judge_units(judge, todo, args.workers)
        logger.info('new verdicts: %s', totals)
        if args.limit is not None:
            print(json.dumps(totals, indent=2))
            return 0
    aggregate = write_outputs(args.results, runs, judge, allow_unscored=args.allow_unscored)
    for name, agg in aggregate['settings'].items():
        logger.info('%s: %s %s (string %s) over %d runs, unscored %s', name, agg['metric'], agg['mean'],
                    agg['string_mean'], agg['runs'], agg['n_unscored'])
    return 0


if __name__ == '__main__':
    sys.exit(main())
