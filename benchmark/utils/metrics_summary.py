# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# Bedrock model pricing as of May 2026 (USD per 1M tokens).
# Source: https://aws.amazon.com/bedrock/pricing/
BEDROCK_PRICING = {
    'us.anthropic.claude-haiku-4-5-20251001-v1:0': {
        'input_per_million': 1.00,
        'output_per_million': 5.00,
    },
    'us.anthropic.claude-sonnet-4-6': {
        'input_per_million': 3.00,
        'output_per_million': 15.00,
    },
}


def _percentile(sorted_values: List[float], p: float) -> float:
    """Compute the p-th percentile of a sorted list of values using linear interpolation."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return float(sorted_values[0])

    rank = (p / 100.0) * (n - 1)
    lower = int(rank)
    upper = lower + 1
    fraction = rank - lower

    if upper >= n:
        return float(sorted_values[-1])

    return sorted_values[lower] + fraction * (sorted_values[upper] - sorted_values[lower])


def _compute_latency_stats(values: List[int]) -> Optional[Dict[str, float]]:
    """Compute avg, p50, p95 for a list of non-null latency values."""
    if not values:
        return None

    sorted_values = sorted(values)
    n = len(sorted_values)

    avg = round(sum(sorted_values) / n, 2)
    p50 = round(_percentile(sorted_values, 50), 2)
    p95 = round(_percentile(sorted_values, 95), 2)

    return {'avg': avg, 'p50': p50, 'p95': p95}


def compute_metrics_summary(
    per_query_data: List[Dict[str, Any]],
    retriever_id: str,
    dataset: str,
    model_id: str,
    num_empty: int,
) -> Dict[str, Any]:
    """Compute aggregate latency, tokens, estimated cost for a benchmark run."""
    # Extract non-null latency values
    retrieve_ms_values = [
        entry['retrieve_ms'] for entry in per_query_data
        if entry.get('retrieve_ms') is not None
    ]
    answer_ms_values = [
        entry['answer_ms'] for entry in per_query_data
        if entry.get('answer_ms') is not None
    ]
    total_latency_ms_values = [
        entry['total_latency_ms'] for entry in per_query_data
        if entry.get('total_latency_ms') is not None
    ]

    num_excluded_latency = sum(
        1 for entry in per_query_data
        if entry.get('retrieve_ms') is None or entry.get('answer_ms') is None
    )

    latency = {
        'retrieval_ms': _compute_latency_stats(retrieve_ms_values),
        'response_ms': _compute_latency_stats(answer_ms_values),
        'total_ms': _compute_latency_stats(total_latency_ms_values),
    }

    # Compute total tokens (excluding null entries)
    total_input_tokens = 0
    total_output_tokens = 0
    total_retrieval_context_tokens = 0
    num_missing_token_metadata = 0
    num_missing_context_token_metadata = 0

    for entry in per_query_data:
        input_tokens = entry.get('input_tokens')
        output_tokens = entry.get('output_tokens')
        retrieval_context_tokens = entry.get('retrieval_context_tokens')

        if input_tokens is None or output_tokens is None:
            num_missing_token_metadata += 1
        else:
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens

        if retrieval_context_tokens is None:
            num_missing_context_token_metadata += 1
        else:
            total_retrieval_context_tokens += retrieval_context_tokens

    num_with_prompt_tokens = len(per_query_data) - num_missing_token_metadata
    num_with_context_tokens = len(per_query_data) - num_missing_context_token_metadata

    avg_prompt_tokens_per_query = round(
        total_input_tokens / num_with_prompt_tokens, 2
    ) if num_with_prompt_tokens > 0 else None

    avg_retrieval_context_tokens_per_query = round(
        total_retrieval_context_tokens / num_with_context_tokens, 2
    ) if num_with_context_tokens > 0 else None

    # Compute estimated cost
    estimated_cost_usd: Optional[float] = None
    if model_id in BEDROCK_PRICING:
        pricing = BEDROCK_PRICING[model_id]
        input_cost = (total_input_tokens / 1_000_000) * pricing['input_per_million']
        output_cost = (total_output_tokens / 1_000_000) * pricing['output_per_million']
        estimated_cost_usd = round(input_cost + output_cost, 2)
    else:
        logger.warning(
            f"Model ID '{model_id}' not found in pricing table. "
            f"Estimated cost will be null."
        )

    return {
        'retriever': retriever_id,
        'dataset': dataset,
        'model_id': model_id,
        'num_queries': len(per_query_data),
        'num_empty_responses': num_empty,
        'num_missing_token_metadata': num_missing_token_metadata,
        'num_missing_context_token_metadata': num_missing_context_token_metadata,
        'num_excluded_latency': num_excluded_latency,
        'latency': latency,
        'tokens': {
            'total_input_tokens': total_input_tokens,
            'total_output_tokens': total_output_tokens,
            'total_retrieval_context_tokens': total_retrieval_context_tokens,
            'avg_prompt_tokens_per_query': avg_prompt_tokens_per_query,
            'avg_retrieval_context_tokens_per_query': avg_retrieval_context_tokens_per_query,
        },
        'estimated_cost_usd': estimated_cost_usd,
    }
