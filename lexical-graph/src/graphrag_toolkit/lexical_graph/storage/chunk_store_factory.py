# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Union, Type, Dict

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore, ChunkStoreFactoryMethod
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

logger = logging.getLogger(__name__)

ChunkStoreType = Union[str, ChunkStore]
ChunkStoreFactoryMethodType = Union[ChunkStoreFactoryMethod, Type[ChunkStoreFactoryMethod]]


class InGraphChunkStoreFactory(ChunkStoreFactoryMethod):
    """
    Fallback factory: creates an InGraphChunkStore when no other chunk
    store backend recognizes chunk_info, preserving today's behavior.
    Always tried last by ChunkStoreFactory, the same way
    DummyGraphStoreFactory is the last-resort fallback for GraphStoreFactory.
    """
    def try_create(self, chunk_info: str, **kwargs) -> ChunkStore:
        if chunk_info:
            return None

        graph_store = kwargs.get('graph_store')
        if not isinstance(graph_store, GraphStore):
            raise ValueError('InGraphChunkStoreFactory requires a graph_store keyword argument.')

        return InGraphChunkStore(graph_store)


_chunk_store_factories: Dict[str, ChunkStoreFactoryMethod] = {}
_default_chunk_store_factory = InGraphChunkStoreFactory()


class ChunkStoreFactory():
    """
    Factory class for registering and creating ChunkStore objects.

    Mirrors GraphStoreFactory: registered factory methods are tried in
    order until one recognizes the given chunk_info and returns a
    ChunkStore. InGraphChunkStoreFactory is always tried last, as the
    fallback of last resort, regardless of registration order.
    """
    @staticmethod
    def register(factory_type: ChunkStoreFactoryMethodType):
        """
        Register a ChunkStoreFactoryMethod subclass or instance.
        """
        if isinstance(factory_type, type):
            if not issubclass(factory_type, ChunkStoreFactoryMethod):
                raise ValueError(f'Invalid factory_type argument: {factory_type.__name__} must inherit from ChunkStoreFactoryMethod.')
            _chunk_store_factories[factory_type.__name__] = factory_type()
        else:
            factory_type_name = type(factory_type).__name__
            if not isinstance(factory_type, ChunkStoreFactoryMethod):
                raise ValueError(f'Invalid factory_type argument: {factory_type_name} must inherit from ChunkStoreFactoryMethod.')
            _chunk_store_factories[factory_type_name] = factory_type

    @staticmethod
    def for_chunk_store(chunk_info: ChunkStoreType = None, **kwargs) -> ChunkStore:
        """
        Create a ChunkStore from chunk_info, or return chunk_info directly
        if it's already a ChunkStore instance.
        """
        if chunk_info and isinstance(chunk_info, ChunkStore):
            return chunk_info

        for factory in _chunk_store_factories.values():
            chunk_store = factory.try_create(chunk_info, **kwargs)
            if chunk_store:
                return chunk_store

        if not chunk_info:
            return _default_chunk_store_factory.try_create(chunk_info, **kwargs)

        raise ValueError(f'Unrecognized chunk store info: {chunk_info}. Check that an appropriate chunk store factory method is registered with ChunkStoreFactory.')
