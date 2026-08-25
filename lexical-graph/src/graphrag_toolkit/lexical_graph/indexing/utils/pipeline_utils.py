# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pipe import Pipe
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Optional, Sequence, Any, cast, Callable, Generator, Union


from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.ingestion.pipeline import run_transformations
from llama_index.core.schema import BaseNode, Document

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig


def _init_worker(config_snapshot):
    """Re-apply the parent's GraphRAGConfig scalars in a spawn-started worker.

    spawn re-imports config.py in a clean interpreter, so the GraphRAGConfig
    singleton loses any programmatically-set value and would silently fall back
    to env/None - widening effective permissions (a scoped aws_profile becomes
    the ambient role) and mis-placing data (s3_chunk_store -> None falls back to
    the in-graph chunk store, dropping the intended KMS CMK). Re-applying the
    snapshot keeps workers consistent with the parent.
    """
    GraphRAGConfig.apply_config_snapshot(config_snapshot)


def _sink():
    def _sink_from(generator):
        for item in generator:
            continue
    return Pipe(_sink_from)

sink = _sink()

def run_pipeline(
    pipeline:IngestionPipeline,
    node_batches:List[List[BaseNode]],
    cache_collection: Optional[str] = None,
    in_place: bool = True,
    num_workers: int = 1,
    **kwargs: Any,
) -> Sequence[BaseNode]:
    transform: Callable[[List[BaseNode]], List[BaseNode]] = partial(
        run_transformations,
        transformations=pipeline.transformations,
        in_place=in_place,
        cache=pipeline.cache if not pipeline.disable_cache else None,
        cache_collection=cache_collection,
        **kwargs
    )

    # Use "spawn": a forked worker can inherit a held lock (e.g. a logging
    # thread's) and deadlock. Spawn starts workers from a clean interpreter,
    # which also drops the GraphRAGConfig singleton's programmatically-set
    # values - so propagate a picklable snapshot via the worker initializer.
    config_snapshot = GraphRAGConfig.get_config_snapshot()
    with ProcessPoolExecutor(
        max_workers=num_workers,
        mp_context=multiprocessing.get_context('spawn'),
        initializer=_init_worker,
        initargs=(config_snapshot,),
    ) as p:
        processed_node_batches = p.map(transform, node_batches)
        
    for processed_node_batch in processed_node_batches:
        for processed_node in processed_node_batch:
            yield processed_node

def node_batcher(
        num_batches: int, nodes: Union[Sequence[BaseNode], List[Document]]
    ) -> Generator[Union[Sequence[BaseNode], List[Document]], Any, Any]:
        num_nodes = len(nodes)
        batch_size = max(1, int(num_nodes / num_batches))
        if batch_size * num_batches < num_nodes:
             batch_size += 1
        for i in range(0, num_nodes, batch_size):
            yield nodes[i : i + batch_size]
