# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc
from typing import Optional

from graphrag_toolkit.lexical_graph.storage.chunk.chunk_store import ChunkStore


class ChunkStoreFactoryMethod(abc.ABC):
    """
    Factory method pattern for creating ChunkStore instances.

    Mirrors GraphStoreFactoryMethod: subclasses attempt to create a
    ChunkStore from the given chunk_info, returning None if they don't
    recognize it so ChunkStoreFactory can move on to the next registered
    factory.
    """
    @abc.abstractmethod
    def try_create(self, chunk_info: str, **kwargs) -> Optional[ChunkStore]:
        """
        Attempt to create a ChunkStore from chunk_info. Return None if this
        factory doesn't recognize chunk_info.
        """
        raise NotImplementedError
