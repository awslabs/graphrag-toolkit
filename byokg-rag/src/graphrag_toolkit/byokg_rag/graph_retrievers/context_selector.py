# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from typing import Dict, List, Optional, Sequence

from ..indexing.graph_embeddings import is_opaque_id

__all__ = ['ContextSelector']

_DELIMITERS = re.compile(r'\s*(?:->|>|\|)\s*')


def _head(line: str) -> str:
    return _DELIMITERS.split(line, 1)[0].strip()


def _opaque_ids(line: str) -> List[str]:
    return sorted({token for token in _DELIMITERS.split(line) if is_opaque_id(token)})


class ContextSelector:
    """Reranks retrieved context lines against the question and keeps the most relevant.

    The top max_lines lines by reranker score are kept, plus up to max_chain_lines lines
    chained to them through an opaque id (m.0abc), up to chain_depth hops: a fact through
    an intermediate node is unreadable without the line that joins it. Chains are followed
    both ways, to the lines an id leads to and to the line that leads to it, best-ranked
    first. Kept lines come best first, each chain line right after the line that led to
    it, so the result is a reordered subset of the input of at most
    max_lines + max_chain_lines lines, whatever the graph's fan-out.
    """

    def __init__(self, reranker, max_lines: int = 20, chain_depth: int = 2,
                 max_chain_lines: Optional[int] = None):
        if not isinstance(max_lines, int) or max_lines < 1:
            raise ValueError(f'max_lines must be a positive integer, got {max_lines!r}')
        if not isinstance(chain_depth, int) or chain_depth < 0:
            raise ValueError(f'chain_depth must be a non-negative integer, got {chain_depth!r}')
        if max_chain_lines is None:
            max_chain_lines = max_lines
        if not isinstance(max_chain_lines, int) or max_chain_lines < 0:
            raise ValueError(f'max_chain_lines must be a non-negative integer, got {max_chain_lines!r}')
        self.reranker = reranker
        self.max_lines = max_lines
        self.chain_depth = chain_depth
        self.max_chain_lines = max_chain_lines

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
        ranking = self._ranking(query, lines)
        heads = [_head(line) for line in lines]
        ids = [_opaque_ids(line) for line in lines]
        # Built in rank order, so a capped chain keeps its best-ranked lines.
        starting_at: Dict[str, List[int]] = {}  # lines whose head is the id
        reaching: Dict[str, List[int]] = {}     # lines that lead to the id
        for i in ranking:
            for opaque_id in ids[i]:
                (starting_at if opaque_id == heads[i] else reaching).setdefault(opaque_id, []).append(i)

        kept: List[str] = []
        depth_of: Dict[int, int] = {}
        chain_lines = 0

        def linked(i: int) -> List[int]:
            out: List[int] = []
            for opaque_id in ids[i]:
                # A line's own head leads back to its parent, whose siblings it reaches next.
                out.extend(reaching.get(opaque_id, ()) if opaque_id == heads[i]
                           else starting_at.get(opaque_id, ()))
            return out

        def keep(i: int, depth: int) -> None:
            nonlocal chain_lines
            # Revisit a line reached by a shorter chain, so its own chains get followed.
            if depth_of.get(i, depth + 1) <= depth:
                return
            if i not in depth_of:
                if depth > 0:
                    if chain_lines >= self.max_chain_lines:
                        return
                    chain_lines += 1
                kept.append(lines[i])
            depth_of[i] = depth
            if depth < self.chain_depth:
                for j in linked(i):
                    keep(j, depth + 1)

        for i in ranking[:self.max_lines]:
            keep(i, 0)
        return kept
