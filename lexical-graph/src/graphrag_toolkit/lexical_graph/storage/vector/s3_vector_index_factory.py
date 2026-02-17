# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod, to_embedded_query

logger = logging.getLogger(__name__)

S3_VECTORS = 's3vectors://'

class S3VectorIndexFactory(VectorIndexFactoryMethod):
    
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:

        bucket_and_prefix = None

        if vector_index_info.startswith(S3_VECTORS):
            bucket_and_prefix = vector_index_info[len(S3_VECTORS):]
        
        if bucket_and_prefix:
            parts = bucket_and_prefix.split('/')
            if len(parts) > 1:
                bucket_name = parts[0]
                prefix = parts[1]
            else:
                bucket_name = parts[0]
                prefix = None
            try:
                from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes import S3VectorIndex
                logger.debug(f'Opening S3 vector indexes [index_names: {index_names}, bucket: {bucket_name}, prefix: {prefix}]')
                return [S3VectorIndex.for_index(index_name, bucket_name, prefix=prefix, **kwargs) for index_name in index_names]
            except ImportError as e:
                raise e
                  
        else:
            return None