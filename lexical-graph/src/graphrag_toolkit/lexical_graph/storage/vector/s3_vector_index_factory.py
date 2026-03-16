# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List
from urllib.parse import urlparse, parse_qs

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod, to_embedded_query

logger = logging.getLogger(__name__)

S3_VECTORS = 's3vectors://'

def parse_s3_vectors_connection_string(connection_string):
    parsed = urlparse(connection_string)
    
    bucket_name = parsed.hostname

    prefix = parsed.path[1:] if parsed.path else None
    if prefix:
        while prefix.endswith('/'):
            prefix = prefix[:-1]
    prefix = prefix if prefix else None

    kms_key_arn = parse_qs(parsed.query).get('kmsKeyArn', None) if parsed.query else None
    if kms_key_arn:
        kms_key_arn = kms_key_arn if not isinstance(kms_key_arn, list) else kms_key_arn[0]

    return (bucket_name, prefix, kms_key_arn)

class S3VectorIndexFactory(VectorIndexFactoryMethod):
    
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:  # pragma: no cover

        bucket_and_prefix = None

        if vector_index_info.startswith(S3_VECTORS):
            (bucket_name, prefix, kms_key_arn) = parse_s3_vectors_connection_string(vector_index_info)
            try:
                from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes import S3VectorIndex
                logger.debug(f'Opening S3 vector indexes [index_names: {index_names}, bucket: {bucket_name}, prefix: {prefix}]')
                return [S3VectorIndex.for_index(index_name, bucket_name, prefix=prefix, kms_key_arn=kms_key_arn, **kwargs) for index_name in index_names]
            except ImportError as e:
                raise e
                  
        else:
            return None