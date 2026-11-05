# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from typing import Dict, List, Sequence

from ..indexing.graph_embeddings import is_opaque_id

__all__ = ['ContextSelector']

_DELIMITERS = re.compile(r'\s*(?:->|>|\|)\s*')


def _head(line: str) -> str:
    return line.split(' -> ', 1)[0].strip()


def _opaque_ids(line: str) -> List[str]:
    return sorted({token for token in _DELIMITERS.split(line) if is_opaque_id(token)})


class ContextSelector:
    """Reranks retrieved context lines against the question and keeps the most relevant.

    The top max_lines lines by reranker score are kept, plus every line that continues
    a chain through an opaque id (m.0abc) a kept line mentions, up to chain_depth hops:
    a fact through an intermediate node is unreadable without the line that joins it.
    Kept lines come best first, each chain line right after the line that led to it,
    so the result is a reordered subset of the input and never larger than it.
    """

    def __init__(self, reranker, max_lines: int = 20, chain_depth: int = 2):
        if not isinstance(max_lines, int) or max_lines < 1:
            raise ValueError(f'max_lines must be a positive integer, got {max_lines!r}')
        if not isinstance(chain_depth, int) or chain_depth < 0:
            raise ValueError(f'chain_depth must be a non-negative integer, got {chain_depth!r}')
        self.reranker = reranker
        self.max_lines = max_lines
        self.chain_depth = chain_depth

    def _ranking(self, query: str, lines: List[str]) -> List[int]:
        # GReranker.rerank_input_with_query returns (reranked lines, indices into lines).
        _, ids = self.reranker.rerank_input_with_query(query, lines, topk=len(lines))[:2]
        order = [int(i) for i in ids]
        if sorted(order) != list(range(len(lines))):
            raise ValueError(f'{type(self.reranker).__name__} returned indices {order} '
                             f'for {len(lines)} lines; expected a ranking of every line')
        return order

    def select(self, query: str, context: Sequence[str]) -> List[str]:
        lines = list(dict.fromkeys(context))
        if not lines:
            return []
        by_head: Dict[str, List[int]] = {}
        for i, line in enumerate(lines):
            by_head.setdefault(_head(line), []).append(i)

        kept, depth_of = [], {}

        def keep(i: int, depth: int) -> None:
            # Revisit a line reached by a shorter chain, so its own chains get followed.
            if depth_of.get(i, depth + 1) <= depth:
                return
            if i not in depth_of:
                kept.append(lines[i])
            depth_of[i] = depth
            if depth < self.chain_depth:
                # A line's own head leads to its siblings, which its parent already reached.
                for opaque_id in _opaque_ids(lines[i]):
                    if opaque_id != _head(lines[i]):
                        for j in by_head.get(opaque_id, ()):
                            keep(j, depth + 1)

        for i in self._ranking(query, lines)[:self.max_lines]:
            keep(i, 0)
        return kept
