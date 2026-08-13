# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore, ChunkStoreFactoryMethod
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import (
    S3ChunkStore,
    S3_URI_SCHEME,
    parse_s3_connection_string,
)

logger = logging.getLogger(__name__)

class S3ChunkStoreFactory(ChunkStoreFactoryMethod):
    """
    Creates an S3ChunkStore from an `s3://bucket/prefix` connection string,
    optionally with a `?kmsKeyArn=` query parameter.

    The store is given an InGraphChunkStore fallback when a graph_store is
    available, so chunk text written before a migration - still inline on the
    graph node - is still readable.
    """
    def try_create(self, chunk_info: str, **kwargs) -> Optional[ChunkStore]:
        if not chunk_info or not chunk_info.startswith(S3_URI_SCHEME):
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
