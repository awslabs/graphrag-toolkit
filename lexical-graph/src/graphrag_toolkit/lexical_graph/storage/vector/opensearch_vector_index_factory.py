# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod, to_embedded_query

logger = logging.getLogger(__name__)

OPENSEARCH_SERVERLESS = 'aoss://'
OPENSEARCH_SERVERLESS_DNS = 'aoss.amazonaws.com'

class OpenSearchVectorIndexFactory(VectorIndexFactoryMethod):
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
        endpoint = None
        if vector_index_info.startswith(OPENSEARCH_SERVERLESS):
            endpoint = vector_index_info[len(OPENSEARCH_SERVERLESS):]
        elif vector_index_info.startswith('https://') and vector_index_info.endswith(OPENSEARCH_SERVERLESS_DNS):
            endpoint = vector_index_info
        if endpoint:
            try:
                from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes import OpenSearchIndex
                logger.debug(f'Opening OpenSearch vector indexes [index_names: {index_names}, endpoint: {endpoint}]')
                return [OpenSearchIndex.for_index(index_name, endpoint, **kwargs) for index_name in index_names]
            except ImportError as e:
                raise e
                  
        else:
            return None