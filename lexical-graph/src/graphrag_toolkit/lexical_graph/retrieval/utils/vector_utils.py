# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import queue
import threading
from collections import OrderedDict
from typing import Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

# Bounded LRU cache: topicId -> sourceId. A topic's source never changes, so entries are
# safe to cache indefinitely; the size cap keeps memory flat in long-running processes
# (this cache is process-global, not per-query).
_TOPIC_SOURCE_CACHE_MAXSIZE = 50000
_topic_source_cache = OrderedDict()
_topic_source_cache_lock = threading.Lock()


def _cache_get(topic_id):
    with _topic_source_cache_lock:
        if topic_id in _topic_source_cache:
            _topic_source_cache.move_to_end(topic_id)
            return _topic_source_cache[topic_id]
    return None


def _cache_put(topic_id, source_id):
    with _topic_source_cache_lock:
        _topic_source_cache[topic_id] = source_id
        _topic_source_cache.move_to_end(topic_id)
        while len(_topic_source_cache) > _TOPIC_SOURCE_CACHE_MAXSIZE:
            _topic_source_cache.popitem(last=False)


def _resolve_source_ids(graph_store, index_name, elements):
    """Backfill the 'source' key for elements that lack it.

    Correctly-built elements already carry source (the node builder sets it at index time):
    we first lift it from the element's own metadata, which involves no I/O. Only elements
    with no source anywhere are resolved from the graph (Topic -> Chunk -> Source). A topic's
    source is immutable, so resolutions are memoised in a bounded LRU cache - resolved at
    most once, without unbounded memory growth in long-running processes.
    """
    id_key = f'{index_name}Id'
    ids_to_resolve = []

    for element in elements:
        if 'source' in element:
            continue
        # 1) Prefer source already present in the element's metadata (no graph call).
        meta_source = (element.get('metadata') or {}).get('source')
        if meta_source and meta_source.get('sourceId'):
            element['source'] = {'sourceId': meta_source['sourceId']}
            continue
        # 2) Fall back to the bounded cache.
        node_id = element.get(index_name, {}).get(id_key)
        if not node_id:
            continue
        cached = _cache_get(node_id)
        if cached is not None:
            element['source'] = {'sourceId': cached}
        else:
            ids_to_resolve.append(node_id)

    # 3) Resolve any still-missing ids from the graph in a single query.
    if ids_to_resolve:
        cypher = f"""
        MATCH (t:`__Topic__`)-[:`__MENTIONED_IN__`]->(c:`__Chunk__`)-[:`__EXTRACTED_FROM__`]->(s:`__Source__`)
        WHERE {graph_store.node_id('t.topicId')} IN $topicIds
        RETURN DISTINCT {graph_store.node_id('t.topicId')} AS topicId,
               {graph_store.node_id('s.sourceId')} AS sourceId
        """
        try:
            results = graph_store.execute_query(cypher, {'topicIds': ids_to_resolve})
            for r in results:
                _cache_put(r['topicId'], r['sourceId'])
        except Exception as e:
            logger.warning(f'Failed to resolve topic sources: {e}')

        for element in elements:
            if 'source' in element:
                continue
            node_id = element.get(index_name, {}).get(id_key)
            if node_id:
                cached = _cache_get(node_id)
                if cached is not None:
                    element['source'] = {'sourceId': cached}


def get_diverse_vss_elements(index_name:str, query_bundle: QueryBundle, vector_store:VectorStore, diversity_factor:int, vss_top_k:int, filter_config:Optional[FilterConfig], graph_store=None):
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
        source = element.get('source')
        if source is None and graph_store is not None:
            # Resolve sources via graph lookup + cache
            _resolve_source_ids(graph_store, index_name, elements)
            break
    
    for element in elements:
        source = element.get('source')
        if source is None:
            # No graph_store available or resolution failed — skip diversity
            return elements[:vss_top_k]
        source_id = source['sourceId']
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