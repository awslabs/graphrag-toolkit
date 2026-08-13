# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Union, Type, Dict, Optional

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore, ChunkStoreFactoryMethod
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store_factory import InGraphChunkStoreFactory
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store_factory import S3ChunkStoreFactory

logger = logging.getLogger(__name__)

ChunkStoreType = Union[str, ChunkStore]
ChunkStoreFactoryMethodType = Union[ChunkStoreFactoryMethod, Type[ChunkStoreFactoryMethod]]





_chunk_store_factories: Dict[str, ChunkStoreFactoryMethod] = {
    c.__name__: c() for c in [
        S3ChunkStoreFactory,
    ]
}
_default_chunk_store_factory = InGraphChunkStoreFactory()


class ChunkStoreFactory():
    """
    Factory class for registering and creating ChunkStore objects.

    Registered factory methods are tried in registration order until one
    recognizes the given chunk_info and returns a ChunkStore. If none do
    and chunk_info is empty, InGraphChunkStoreFactory supplies the
    default in-graph store; if none do and chunk_info is non-empty, that
    is an error and ValueError is raised.

    Note this differs from GraphStoreFactory, which has no default at all
    and raises for empty graph_info as well as unrecognized graph_info.
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
