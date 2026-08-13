# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Statement reranker chain configuration and fallback policies.

A reranker chain is an ordered list of reranking strategies. Processors such as
``RerankStatements`` try each strategy in turn, consulting a fallback policy to
decide whether a failure should be recovered by moving to the next strategy.
Entity reranking is a separate TF-IDF stage and is not controlled by this chain.

A fallback policy is a callable ``policy(reranker, *, error=None) -> bool``,
consulted when a strategy raises an exception and a next strategy remains.
When no policy is configured, the chain falls back on any exception. An
exception from the final (or only) strategy always propagates. Before any next
entry is reached, including ``'none'``, the preceding failure must be permitted
by the fallback policy. Once reached, ``'none'`` performs no scoring and cannot
raise. Fallback is per-query and stateless: a strategy that failed on one query
is attempted again on the next, so transient conditions such as throttling
recover automatically once they clear server-side.
"""

KNOWN_RERANKERS = (
    'model',
    'tfidf',
    'bedrock',
    'none',
)


def normalize_reranker_chain(reranker):
    """Normalize a statement reranker configuration into strategy names.

    Any falsy value returns an empty chain before type validation. Strings are
    stripped and lowercased. Lists are stripped, lowercased, and emptied of
    blank entries; their names and placement of ``'none'`` are validated. The
    legacy single-string form is not validated against ``KNOWN_RERANKERS``,
    preserving the historical silently-skip behaviour for unknown names.

    Raises:
        TypeError: If a truthy value is neither a string nor a list.
        ValueError: If a list contains a non-string entry, an unknown strategy,
            or ``'none'`` anywhere except the final position.
    """
    if not reranker:
        return []

    if isinstance(reranker, str):
        return [reranker.strip().lower()] if reranker.strip() else []

    if not isinstance(reranker, list):
        raise TypeError(
            f'reranker must be a string or list of strings, got {type(reranker).__name__}'
        )

    for r in reranker:
        if not isinstance(r, str):
            raise ValueError(
                f'reranker list entries must be strings, got {type(r).__name__}: {r!r}'
            )
    chain = [r.strip().lower() for r in reranker if r.strip()]
    unknown = [r for r in chain if r not in KNOWN_RERANKERS]
    if unknown:
        raise ValueError(
            f'Unknown reranker(s) in chain: {unknown}. '
            f'Expected values from: {list(KNOWN_RERANKERS)}'
        )
    if 'none' in chain[:-1]:
        raise ValueError('none can only be used alone or as the final fallback in a reranker chain')
    return chain
