# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore, ChunkStoreFactoryMethod
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore

logger = logging.getLogger(__name__)

class InGraphChunkStoreFactory(ChunkStoreFactoryMethod):
    """
    Default factory: creates an InGraphChunkStore when no chunk_info is
    given, preserving today's behavior for callers that don't configure a
    chunk store. Returns None for any non-empty chunk_info, so it never
    swallows a backend URI that a registered factory was meant to handle.
    """
    def try_create(self, chunk_info: str, **kwargs) -> Optional[ChunkStore]:
        if chunk_info:
            return None

        graph_store = kwargs.get('graph_store')
        if graph_store is None:
            raise ValueError('InGraphChunkStoreFactory requires a graph_store keyword argument.')

        return InGraphChunkStore(graph_store)
