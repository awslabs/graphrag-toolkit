# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, List

from graphrag_toolkit.lexical_graph.storage.chunk.chunk_store import ChunkStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result, to_params

logger = logging.getLogger(__name__)


class InGraphChunkStore(ChunkStore):
    """
    ChunkStore backed by the `chunk.value` property on `__Chunk__` graph
    nodes, preserving today's chunk-text read/write behavior behind the
    ChunkStore interface so other backends (e.g. S3) can be swapped in
    without changing callers.

    Scope: this store only reads and writes chunk text (`chunk.value`).
    get_batch() matches the existence semantics of
    retrieval.utils.chunk_utils.get_chunks_query() (a chunk without a
    valid `__EXTRACTED_FROM__` link to a `__Source__` is excluded), but it
    does not return other chunk properties or source metadata, and put()
    does not set the arbitrary per-chunk metadata properties
    ChunkGraphBuilder.build() writes. Those remain the concern of
    ChunkGraphBuilder and get_chunks_query, not of ChunkStore.
    """

    def __init__(self, graph_client: GraphStore):
        self.graph_client = graph_client

    def get_batch(self, chunk_ids: List[str]) -> Dict[str, str]:
        if not chunk_ids:
            return {}

        query = f'''
        MATCH (chunk:`__Chunk__`)-[:`__EXTRACTED_FROM__`]->(:`__Source__`) WHERE {self.graph_client.node_id("chunk.chunkId")} in $chunk_ids
        RETURN {{
            {node_result('chunk', self.graph_client.node_id("chunk.chunkId"), properties=['value'])}
        }} AS result
        '''
        params = {'chunk_ids': chunk_ids}

        rows = self.graph_client.execute_query(query, params)

        return {
            row['result']['chunk']['chunkId']: row['result']['chunk']['value']
            for row in rows
            if row['result']['chunk'].get('value') is not None
        }

    def put(self, chunk_id: str, text: str) -> None:
        logger.debug(f'Writing chunk text to graph [chunk_id: {chunk_id}]')

        query = f'''
        // write chunk value
        UNWIND $params AS params
        MERGE (chunk:`__Chunk__`{{{self.graph_client.node_id("chunkId")}: params.chunk_id}})
        ON CREATE SET chunk.value = params.text
        ON MATCH SET chunk.value = params.text
        '''
        params = to_params({'chunk_id': chunk_id, 'text': text})

        self.graph_client.execute_query_with_retry(query, params, max_attempts=5, max_wait=7)
