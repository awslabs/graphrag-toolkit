# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import queue
from typing import Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

def get_diverse_vss_elements(index_name:str, query_bundle: QueryBundle, vector_store:VectorStore, diversity_factor:int, vss_top_k:int, filter_config:Optional[FilterConfig]):
    """
    Retrieve diverse elements from a vector search system (VSS) by applying a diversity
    factor to limit redundancy among results.

    This function queries a vector store using the provided query, index, and filter
    configuration, then applies a diversity mechanism to return results with more
    heterogeneity. The diversity factor determines the level of diversification among
    the results.

    Args:
        index_name (str): Name of the index to search in the vector store.
        query_bundle (QueryBundle): Query object containing the necessary details for
            executing the search.
        vector_store (VectorStore): Vector store instance to query for retrieving the
            elements.
        filter_config (Optional[FilterConfig]): Optional filter configuration to
            refine the query results.

    Returns:
        list: A list of diverse elements from the vector store result set.
    """
    if not diversity_factor or diversity_factor < 1:
        return vector_store.get_index(index_name).top_k(query_bundle, top_k=vss_top_k, filter_config=filter_config)

    top_k = vss_top_k * diversity_factor
        
    elements = vector_store.get_index(index_name).top_k(query_bundle, top_k=top_k, filter_config=filter_config)
        
    source_map = {}
        
    for element in elements:
        source_id = element['source']['sourceId']
        if source_id not in source_map:
            source_map[source_id] = queue.Queue()
        source_map[source_id].put(element)
            
    elements_by_source = queue.Queue()
        
    for source_elements in source_map.values():
        elements_by_source.put(source_elements)
        
    diverse_elements = []
        
    while (not elements_by_source.empty()) and len(diverse_elements) < vss_top_k:
        source_elements = elements_by_source.get()
        diverse_elements.append(source_elements.get())
        if not source_elements.empty():
            elements_by_source.put(source_elements)

    logger.debug(f'Diverse {index_name}s:\n' + '\n--------------\n'.join([str(element) for element in diverse_elements]))

    return diverse_elements