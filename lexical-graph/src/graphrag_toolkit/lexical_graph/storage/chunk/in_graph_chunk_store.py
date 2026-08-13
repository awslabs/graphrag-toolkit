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

    Scope: this store only reads and writes chunk text (`chunk.value`). A
    chunk with no stored value is omitted from get_batch(). Chunks are
    matched on chunk id alone, deliberately: put() writes only the text
    and never creates the `__EXTRACTED_FROM__` link to a `__Source__`, so
    requiring that link on read would make text written through this
    store invisible to a read from it. Callers that need source-linked
    chunks specifically should filter on their own query.

    This store does not return other chunk properties or source metadata,
    and put() does not set the arbitrary per-chunk metadata properties
    ChunkGraphBuilder.build() writes. Those remain the concern of
    ChunkGraphBuilder and callers that need them, not of ChunkStore.
    """

    def __init__(self, graph_client: GraphStore):
        self.graph_client = graph_client

    def get_batch(self, chunk_ids: List[str]) -> Dict[str, str]:
        if not chunk_ids:
            return {}

        query = f'''
        MATCH (chunk:`__Chunk__`) WHERE {self.graph_client.node_id("chunk.chunkId")} in $chunk_ids
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
        self.put_batch({chunk_id: text})

    def put_batch(self, chunks: Dict[str, str]) -> None:
        """
        Write every chunk in a single query.

        The statement already UNWINDs its parameters, so a batch is the same
        query over a longer list rather than one round trip per chunk.
        """
        if not chunks:
            return

        logger.debug(f'Writing chunk text to graph [num_chunks: {len(chunks)}]')

        query = f'''
        // write chunk value
        UNWIND $params AS params
        MERGE (chunk:`__Chunk__`{{{self.graph_client.node_id("chunkId")}: params.chunk_id}})
        ON CREATE SET chunk.value = params.text
        ON MATCH SET chunk.value = params.text
        '''

        # Built directly rather than via to_params(), which wraps a single dict.
        params = {
            'params': [
                {'chunk_id': chunk_id, 'text': text}
                for chunk_id, text in chunks.items()
            ]
        }

        self.graph_client.execute_query_with_retry(query, params, max_attempts=5, max_wait=7)
