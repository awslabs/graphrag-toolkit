# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Valid return values
VALID_CLASSIFICATIONS = {'single-hop', 'multi-hop', 'unknown'}

# Multi-hop keyword patterns
# Conjunctions requiring multiple facts
_MULTI_HOP_CONJUNCTIONS = [
    r'\band\b.*\band\b',       # multiple "and" suggesting multiple facts
    r'\bboth\b',
    r'\bas well as\b',
]

# Temporal markers suggesting multi-step reasoning
_MULTI_HOP_TEMPORAL = [
    r'\bbefore\b',
    r'\bafter\b',
    r'\bwhile\b',
    r'\bduring\b',
]

# Comparison language
_MULTI_HOP_COMPARISON = [
    r'\bcompared to\b',
    r'\bversus\b',
    r'\bmore than\b',
    r'\bless than\b',
    r'\bdifference between\b',
]

# Multi-step reasoning patterns
_MULTI_HOP_REASONING = [
    r'\bhow many\b.*\bthat\b',
    r'\bwhich\b.*\balso\b',
    r'\bwhat\b.*\band\b.*\b(what|who|where|when|how)\b',
]

# Single-hop indicators: simple factual questions without multi-hop markers
_SINGLE_HOP_PATTERNS = [
    r'^what is\b',
    r'^who is\b',
    r'^when was\b',
    r'^where is\b',
    r'^what was\b',
    r'^who was\b',
    r'^where was\b',
    r'^when is\b',
    r'^what are\b',
    r'^who are\b',
    r'^where are\b',
    r'^when did\b',
    r'^what does\b',
    r'^who does\b',
]


def classify_hop(question: str, dataset_metadata: Optional[Dict[str, Any]] = None) -> str:
    """Classify a question as single-hop, multi-hop, or unknown.

    Uses dataset metadata annotations when available. Falls back to keyword
    heuristics (conjunctions, temporal markers, comparison language) when
    metadata is unavailable. Returns 'unknown' if heuristics are inconclusive.

    Args:
        question: The question string to classify.
        dataset_metadata: Optional dict that may contain hop annotations
            (e.g., 'hop', 'hop_count', 'hops', 'num_hops', 'type').

    Returns:
        Exactly one of: 'single-hop', 'multi-hop', 'unknown'.
    """
    # Step 1: Check dataset metadata annotations
    if dataset_metadata is not None:
        classification = _classify_from_metadata(dataset_metadata)
        if classification is not None:
            return classification

    # Step 2: Fall back to keyword heuristics
    return _classify_from_heuristics(question)


def _classify_from_metadata(metadata: Dict[str, Any]) -> Optional[str]:
    """Attempt to classify using dataset metadata annotations.

    Looks for common annotation keys: 'hop', 'hop_count', 'hops',
    'num_hops', 'type', 'complexity'.

    Returns:
        A classification string or None if metadata doesn't contain
        usable hop annotations.
    """
    # Check for direct hop count fields
    for key in ('hop_count', 'hops', 'num_hops', 'hop'):
        if key in metadata:
            value = metadata[key]
            if isinstance(value, (int, float)):
                if value <= 1:
                    return 'single-hop'
                else:
                    return 'multi-hop'
            elif isinstance(value, str):
                lower_val = value.lower().strip()
                if lower_val in ('single-hop', 'single_hop', 'single', '1'):
                    return 'single-hop'
                elif lower_val in ('multi-hop', 'multi_hop', 'multi', 'multiple'):
                    return 'multi-hop'
                # Try parsing as integer
                try:
                    num = int(lower_val)
                    if num <= 1:
                        return 'single-hop'
                    else:
                        return 'multi-hop'
                except (ValueError, TypeError):
                    pass

    # Check for 'type' or 'complexity' fields that might indicate hop count
    for key in ('type', 'complexity', 'question_type'):
        if key in metadata:
            value = metadata[key]
            if isinstance(value, str):
                lower_val = value.lower().strip()
                if 'multi' in lower_val or 'complex' in lower_val:
                    return 'multi-hop'
                elif 'single' in lower_val or 'simple' in lower_val:
                    return 'single-hop'

    return None


def _classify_from_heuristics(question: str) -> str:
    """Classify using keyword-based heuristics.

    Returns 'multi-hop', 'single-hop', or 'unknown'.
    """
    lower_question = question.lower().strip()

    if not lower_question:
        return 'unknown'

    # Check for multi-hop indicators
    has_multi_hop_signal = False

    for pattern in _MULTI_HOP_CONJUNCTIONS:
        if re.search(pattern, lower_question):
            has_multi_hop_signal = True
            break

    if not has_multi_hop_signal:
        for pattern in _MULTI_HOP_TEMPORAL:
            if re.search(pattern, lower_question):
                has_multi_hop_signal = True
                break

    if not has_multi_hop_signal:
        for pattern in _MULTI_HOP_COMPARISON:
            if re.search(pattern, lower_question):
                has_multi_hop_signal = True
                break

    if not has_multi_hop_signal:
        for pattern in _MULTI_HOP_REASONING:
            if re.search(pattern, lower_question):
                has_multi_hop_signal = True
                break

    if has_multi_hop_signal:
        return 'multi-hop'

    # Check for single-hop indicators
    for pattern in _SINGLE_HOP_PATTERNS:
        if re.search(pattern, lower_question):
            return 'single-hop'

    # Heuristics are inconclusive
    return 'unknown'
