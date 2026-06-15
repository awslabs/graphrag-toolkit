# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pipe import Pipe
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Optional, Sequence, Any, Callable, Generator, Union


class _Pipeline:
    """Minimal pipeline container holding a list of transforms."""
    def __init__(self, transformations, disable_cache=True):
        self.transformations = transformations
        self.disable_cache = disable_cache


def _run_transformations(nodes, transformations, **kwargs):
    """Run transforms sequentially (replaces llama_index run_transformations)."""
    for transform in transformations:
        nodes = transform(nodes, **kwargs)
    return nodes


def _sink():
    def _sink_from(generator):
        for item in generator:
            continue
    return Pipe(_sink_from)

sink = _sink()

def run_pipeline(
    pipeline:_Pipeline,
    node_batches:List[List[Any]],
    cache_collection: Optional[str] = None,
    in_place: bool = True,
    num_workers: int = 1,
    **kwargs: Any,
) -> Sequence[Any]:
    transform: Callable[[List[Any]], List[Any]] = partial(
        _run_transformations,
        transformations=pipeline.transformations,
        **kwargs
    )

    with ProcessPoolExecutor(max_workers=num_workers) as p:
        processed_node_batches = p.map(transform, node_batches)
        
    for processed_node_batch in processed_node_batches:
        for processed_node in processed_node_batch:
            yield processed_node

def node_batcher(
        num_batches: int, nodes: Sequence[Any]
    ) -> Generator[Sequence[Any], Any, Any]:
        num_nodes = len(nodes)
        batch_size = max(1, int(num_nodes / num_batches))
        if batch_size * num_batches < num_nodes:
             batch_size += 1
        for i in range(0, num_nodes, batch_size):
            yield nodes[i : i + batch_size]
