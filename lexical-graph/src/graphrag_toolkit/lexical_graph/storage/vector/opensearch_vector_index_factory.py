# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod, to_embedded_query

logger = logging.getLogger(__name__)

OPENSEARCH_SERVERLESS = 'aoss://'
OPENSEARCH_SERVERLESS_DNS = 'aoss.amazonaws.com'
OPENSEARCH = 'opensearch://'

class OpenSearchVectorIndexFactory(VectorIndexFactoryMethod):
    """Factory class for creating OpenSearch vector indexes.

    This class is responsible for creating OpenSearch vector indexes based
    on provided index names and vector index connection information. It
    detects whether the connection information corresponds to an OpenSearch
    Serverless endpoint or a traditional OpenSearch endpoint and constructs
    the corresponding vector indexes.

    Attributes:
        No specific attributes are directly defined in this class. The class
        relies on the methods and details passed during the instantiation and
        method calls.
    """
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
        """
        Attempts to create a list of vector indexes using the provided index names and vector
        index information. This method checks if a supported endpoint configuration is
        provided and uses it to initialize appropriate vector indexes. Raises ImportError
        if the required module is not available.

        Args:
            index_names (List[str]): List of index names to create vector indexes for.
            vector_index_info (str): Information defining the type and endpoint of the vector
                index, such as an OpenSearch Serverless endpoint.
            **kwargs: Additional keyword arguments passed when creating the vector indexes.

        Returns:
            List[VectorIndex]: A list of vector index objects created for the provided index
                names and endpoint configuration, or None if no suitable endpoint is found.
        """
        endpoint = None
        is_sigv4_auth = True
        if vector_index_info.startswith(OPENSEARCH_SERVERLESS):
            endpoint = vector_index_info[len(OPENSEARCH_SERVERLESS):]
            if not endpoint.startswith('https://') and not endpoint.startswith('http://'):
                endpoint = f'https://{endpoint}'
        elif vector_index_info.startswith(OPENSEARCH):
            endpoint = vector_index_info[len(OPENSEARCH):]
            if not endpoint:
                raise ValueError(f'Empty endpoint in OpenSearch vector store connection info: {vector_index_info}')
            is_sigv4_auth = False
        elif vector_index_info.startswith('https://') and vector_index_info.endswith(OPENSEARCH_SERVERLESS_DNS):
            endpoint = vector_index_info
        if endpoint:
            try:
                from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes import OpenSearchIndex
                logger.debug(f'Opening OpenSearch vector indexes [index_names: {index_names}, endpoint: {endpoint}, is_sigv4_auth: {is_sigv4_auth}]')
                return [OpenSearchIndex.for_index(index_name, endpoint, is_sigv4_auth=is_sigv4_auth, **kwargs) for index_name in index_names]
            except ImportError as e:
                raise e

        else:
            return None
