# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def _compute_cost_per_query(metrics_summary: Dict[str, Any]) -> Optional[float]:
    """Compute cost per query from metrics summary.

    Returns None if estimated_cost_usd is null or num_queries is zero/null.
    """
    estimated_cost = metrics_summary.get('estimated_cost_usd')
    num_queries = metrics_summary.get('num_queries')

    if estimated_cost is None or num_queries is None or num_queries <= 0:
        return None

    return estimated_cost / num_queries


def _compute_cost_efficiency(correctness: float, cost_per_query: Optional[float]) -> Optional[float]:
    """Compute cost-efficiency: correctness / cost_per_query.

    Returns None if:
    - cost_per_query is zero or null
    - correctness is zero or negative
    """
    if cost_per_query is None or cost_per_query == 0:
        return None
    if correctness is None or correctness <= 0:
        return None

    return correctness / cost_per_query


def _compute_latency_efficiency(correctness: float, avg_total_ms: Optional[float]) -> Optional[float]:
    """Compute latency-efficiency: correctness / (avg_total_ms / 1000).

    Returns None if:
    - avg_total_ms is zero or null
    - correctness is zero or negative
    """
    if avg_total_ms is None or avg_total_ms == 0:
        return None
    if correctness is None or correctness <= 0:
        return None

    return correctness / (avg_total_ms / 1000.0)


def _rank_by_efficiency(retrievers: List[Dict[str, Any]], key: str) -> List[str]:
    """Rank retrievers by an efficiency metric (descending), nulls last.

    Args:
        retrievers: List of retriever result dicts.
        key: The efficiency key to rank by (e.g., 'cost_efficiency', 'latency_efficiency').

    Returns:
        List of retriever names sorted by the efficiency metric descending, nulls last.
    """
    with_values = []
    without_values = []

    for r in retrievers:
        if r.get(key) is not None:
            with_values.append(r)
        else:
            without_values.append(r)

    # Sort non-null values descending
    with_values.sort(key=lambda x: x[key], reverse=True)

    ranked = [r['retriever'] for r in with_values] + [r['retriever'] for r in without_values]
    return ranked


def _compute_multi_hop_breakdown(
    dataset: str,
    results_dir: str,
    retriever_dirs: List[str],
) -> Optional[Dict[str, Any]]:
    """Compute multi-hop breakdown for the dataset.

    For each retriever, reads responses.jsonl and correctness_evals.json to compute
    multi-hop correctness. Includes warning flag when fewer than 10 multi-hop questions.

    Returns:
        Dict with multi-hop breakdown or None if no hop data is available.
    """
    dataset_dir = os.path.join(results_dir, dataset)
    breakdown_retrievers = {}
    multi_hop_count = 0
    has_any_data = False

    for retriever_name in retriever_dirs:
        retriever_path = os.path.join(dataset_dir, retriever_name)
        responses_path = os.path.join(retriever_path, 'responses.jsonl')
        correctness_evals_path = os.path.join(retriever_path, 'correctness_evals.json')

        if not os.path.isfile(responses_path) or not os.path.isfile(correctness_evals_path):
            continue

        # Read responses to get hop classifications
        responses = []
        try:
            with open(responses_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        responses.append(json.loads(line))
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to read responses.jsonl for {retriever_name}: {e}")
            continue

        # Read correctness evaluations
        try:
            with open(correctness_evals_path, 'r') as f:
                correctness_evals = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to read correctness_evals.json for {retriever_name}: {e}")
            continue

        if len(responses) != len(correctness_evals):
            logger.warning(
                f"Mismatch between responses ({len(responses)}) and "
                f"correctness_evals ({len(correctness_evals)}) for {retriever_name}"
            )
            continue

        # Compute multi-hop correctness
        multi_hop_correct = 0
        multi_hop_total = 0

        for resp, eval_item in zip(responses, correctness_evals):
            hop = resp.get('hop_classification')
            if hop == 'multi-hop':
                multi_hop_total += 1
                if eval_item.get('llmCorrectnessGrade') == 'correct':
                    multi_hop_correct += 1

        if multi_hop_total > 0:
            has_any_data = True
            multi_hop_correctness = multi_hop_correct / multi_hop_total
            breakdown_retrievers[retriever_name] = {
                'multi_hop_correctness': round(multi_hop_correctness, 4),
            }
            # Track the max multi_hop_count across retrievers (should be same for all)
            if multi_hop_total > multi_hop_count:
                multi_hop_count = multi_hop_total

    if not has_any_data:
        return None

    # Compute delta between agentic and deterministic retrievers if both exist
    agentic_ids = {'agentic', 'byokg_agentic'}
    agentic_scores = []
    deterministic_scores = []

    for name, data in breakdown_retrievers.items():
        if name in agentic_ids:
            agentic_scores.append(data['multi_hop_correctness'])
        else:
            deterministic_scores.append(data['multi_hop_correctness'])

    delta = None
    if agentic_scores and deterministic_scores:
        avg_agentic = sum(agentic_scores) / len(agentic_scores)
        avg_deterministic = sum(deterministic_scores) / len(deterministic_scores)
        delta = round(avg_agentic - avg_deterministic, 4)

    # Warning flag when fewer than 10 multi-hop questions
    warning = None
    if multi_hop_count < 10:
        warning = f"Insufficient multi-hop sample size: {multi_hop_count} questions (minimum 10 recommended)"

    return {
        'multi_hop_count': multi_hop_count,
        'warning': warning,
        'retrievers': breakdown_retrievers,
        'delta': delta,
    }


def generate_comparison_report(dataset: str, results_dir: str) -> Optional[Dict[str, Any]]:
    """Generate a cross-retriever comparison report for a dataset.

    Reads metrics_summary.json and correctness.json from each retriever subdirectory
    under results_dir/{dataset}/ and produces comparison_report.json.

    Args:
        dataset: The dataset name (e.g., 'cuad', 'pga', 'wikihow', 'concurrentqa').
        results_dir: Root results directory (e.g., 'benchmark-results').

    Returns:
        The comparison report dict, or None if fewer than 2 retriever results exist.
    """
    dataset_dir = os.path.join(results_dir, dataset)

    if not os.path.isdir(dataset_dir):
        logger.warning(f"Dataset directory not found: {dataset_dir}")
        return None

    # Scan for retriever subdirectories with both metrics_summary.json and correctness.json
    retriever_results = []
    retriever_dirs = []

    for entry in sorted(os.listdir(dataset_dir)):
        entry_path = os.path.join(dataset_dir, entry)
        if not os.path.isdir(entry_path):
            continue

        metrics_path = os.path.join(entry_path, 'metrics_summary.json')
        correctness_path = os.path.join(entry_path, 'correctness.json')

        if not os.path.isfile(metrics_path) or not os.path.isfile(correctness_path):
            continue

        try:
            with open(metrics_path, 'r') as f:
                metrics_summary = json.load(f)
            with open(correctness_path, 'r') as f:
                correctness_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to read data for retriever '{entry}': {e}")
            continue

        correctness = correctness_data.get('correctness')
        if correctness is None:
            logger.warning(f"No correctness score found for retriever '{entry}'")
            continue

        retriever_dirs.append(entry)

        # Extract metrics
        latency = metrics_summary.get('latency', {})
        total_ms_stats = latency.get('total_ms')
        avg_total_ms = total_ms_stats.get('avg') if total_ms_stats else None

        cost_per_query = _compute_cost_per_query(metrics_summary)
        cost_efficiency = _compute_cost_efficiency(correctness, cost_per_query)
        latency_efficiency = _compute_latency_efficiency(correctness, avg_total_ms)

        retriever_results.append({
            'retriever': entry,
            'correctness': correctness,
            'avg_total_ms': avg_total_ms,
            'cost_per_query_usd': round(cost_per_query, 6) if cost_per_query is not None else None,
            'cost_efficiency': round(cost_efficiency, 4) if cost_efficiency is not None else None,
            'latency_efficiency': round(latency_efficiency, 4) if latency_efficiency is not None else None,
        })

    # Skip generation if fewer than 2 retriever results exist
    if len(retriever_results) < 2:
        logger.info(
            f"Fewer than 2 retriever results for dataset '{dataset}' "
            f"({len(retriever_results)} found). Skipping comparison report generation."
        )
        return None

    # Compute rankings
    rankings = {
        'by_cost_efficiency': _rank_by_efficiency(retriever_results, 'cost_efficiency'),
        'by_latency_efficiency': _rank_by_efficiency(retriever_results, 'latency_efficiency'),
    }

    # Compute multi-hop breakdown
    multi_hop_breakdown = _compute_multi_hop_breakdown(dataset, results_dir, retriever_dirs)

    # Build the report
    report = {
        'dataset': dataset,
        'retrievers': retriever_results,
        'rankings': rankings,
    }

    if multi_hop_breakdown is not None:
        report['multi_hop_breakdown'] = {
            dataset: multi_hop_breakdown,
        }

    # Write comparison_report.json to the dataset directory
    report_path = os.path.join(dataset_dir, 'comparison_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"Comparison report written to {report_path}")
    return report
