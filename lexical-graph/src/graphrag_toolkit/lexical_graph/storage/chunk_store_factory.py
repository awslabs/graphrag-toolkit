# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Union, Type, Dict, Optional
from urllib.parse import urlparse, parse_qs

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore, ChunkStoreFactoryMethod
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import S3ChunkStore

logger = logging.getLogger(__name__)

ChunkStoreType = Union[str, ChunkStore]
ChunkStoreFactoryMethodType = Union[ChunkStoreFactoryMethod, Type[ChunkStoreFactoryMethod]]

S3 = 's3://'


def parse_s3_connection_string(connection_string):
    parsed = urlparse(connection_string)

    bucket_name = parsed.hostname

    # `s3:///prefix` parses with no hostname and would otherwise build a store
    # against bucket None, failing later at the first call with an error that
    # says nothing about the connection string.
    if not bucket_name:
        raise ValueError(
            f'Invalid S3 connection string, no bucket name: {connection_string}. '
            'Expected s3://bucket/prefix.'
        )

    prefix = parsed.path[1:] if parsed.path else None
    if prefix:
        while prefix.endswith('/'):
            prefix = prefix[:-1]
    prefix = prefix if prefix else None

    # parse_qs always returns a list for a present key, so take the first value.
    kms_key_arns = parse_qs(parsed.query).get('kmsKeyArn') if parsed.query else None
    kms_key_arn = kms_key_arns[0] if kms_key_arns else None

    return (bucket_name, prefix, kms_key_arn)


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


class S3ChunkStoreFactory(ChunkStoreFactoryMethod):
    """
    Creates an S3ChunkStore from an `s3://bucket/prefix` connection string,
    optionally with a `?kmsKeyArn=` query parameter.

    The store is given an InGraphChunkStore fallback when a graph_store is
    available, so chunk text written before a migration - still inline on the
    graph node - is still readable.
    """
    def try_create(self, chunk_info: str, **kwargs) -> Optional[ChunkStore]:
        if not chunk_info or not chunk_info.startswith(S3):
            return None

        (bucket_name, prefix, kms_key_arn) = parse_s3_connection_string(chunk_info)

        graph_store = kwargs.get('graph_store')
        fallback = InGraphChunkStore(graph_store) if graph_store is not None else None

        logger.debug(f'Opening S3 chunk store [bucket: {bucket_name}, prefix: {prefix}, dual_read: {fallback is not None}]')

        return S3ChunkStore(
            bucket_name=bucket_name,
            prefix=prefix,
            kms_key_arn=kms_key_arn,
            fallback=fallback
        )


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
