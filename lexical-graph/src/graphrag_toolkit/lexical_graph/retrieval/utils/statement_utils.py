# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import numpy as np
import threading
import logging
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result

logger = logging.getLogger(__name__)

def cosine_similarity(query_embedding, statement_embeddings):
    if not statement_embeddings:
        return np.array([]), []

    query_embedding = np.array(query_embedding)
    statement_ids, statement_embeddings = zip(*statement_embeddings.items())
    statement_embeddings = np.array(statement_embeddings)

    dot_product = np.dot(statement_embeddings, query_embedding)
    norms = np.linalg.norm(statement_embeddings, axis=1) * np.linalg.norm(query_embedding)
    
    similarities = dot_product / norms
    return similarities, statement_ids

def get_top_k(query_embedding, statement_embeddings, top_k):

    logger.debug(f'statement_embeddings: {statement_embeddings}')

    if not statement_embeddings:
        return []  
    
    similarities, statement_ids = cosine_similarity(query_embedding, statement_embeddings)

    logger.debug(f'similarities: {similarities}')
    
    if len(similarities) == 0:
        return []

    top_k = min(top_k, len(similarities))
    top_indices = np.argsort(similarities)[::-1][:top_k]

    top_statement_ids = [statement_ids[idx] for idx in top_indices]
    top_similarities = similarities[top_indices]
    return list(zip(top_similarities, top_statement_ids))

def get_statements_query(graph_store, statement_ids):
    cypher = f'''
    MATCH (statement:`__Statement__`)-[:`__MENTIONED_IN__`]->(chunk:`__Chunk__`)-[:`__EXTRACTED_FROM__`]->(source:`__Source__`) WHERE {graph_store.node_id("statement.statementId")} in $statement_ids
    RETURN {{
        {node_result('statement', graph_store.node_id("statement.statementId"))},
        source: {{
            sourceId: {graph_store.node_id("source.sourceId")},
            {node_result('source', key_name='metadata')}
        }},
        {node_result('chunk', graph_store.node_id("chunk.chunkId"))}
    }} AS result
    '''
    params = {'statement_ids': statement_ids}
    statements = graph_store.execute_query(cypher, params)
    results = []
    for statement_id in statement_ids:
                for statement in statements:
                    if statement['result']['statement']['statementId'] == statement_id:
                        results.append(statement)
    return results

def get_free_memory(gpu_index):
    try:
        import pynvml
        pynvml.nvmlInit()
        handle = pynvml.nvmlDeviceGetHandleByIndex(int(gpu_index))
        mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
        return mem_info.free // 1024 ** 2
    except ImportError as e:
        raise ImportError(
                "pynvml package not found, install with 'pip install pynvml'"
            ) from e

def get_top_free_gpus(n=2):

    try:
        import torch
        free_memory = []
        for i in range(torch.cuda.device_count()):
            free_memory.append(get_free_memory(i))
        top_indices = sorted(range(len(free_memory)), key=lambda i: free_memory[i], reverse=True)[:n]
        return top_indices
    except ImportError as e:
        raise ImportError(
                "torch package not found, install with 'pip install torch'"
            ) from e

class SharedEmbeddingCache:
    def __init__(self, vector_store):
        self._cache: Dict[str, np.ndarray] = {}
        self._lock = threading.Lock()
        self.vector_store = vector_store

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),retry=retry_if_exception_type(Exception))
    def _fetch_embeddings(self, statement_ids: List[str]) -> Dict[str, np.ndarray]:
        """Fetch embeddings with retry logic."""
        embeddings = self.vector_store.get_index('statement').get_embeddings(statement_ids)
        return {
            e['statement']['statementId']: np.array(e['embedding']) 
            for e in embeddings
        }

    def get_embeddings(self, statement_ids: List[str]) -> Dict[str, np.ndarray]:
        """Get embeddings from cache or fetch with retry."""
        missing_ids = []
        cached_embeddings = {}

        logger.debug(f'statement_ids: {statement_ids}')

        # Check cache first
        for sid in statement_ids:
            if sid in self._cache:
                cached_embeddings[sid] = self._cache[sid]
            else:
                missing_ids.append(sid)

        logger.debug(f'missing_ids: {missing_ids}')

        # Fetch missing embeddings with retry
        if missing_ids:
            try:
                new_embeddings = self._fetch_embeddings(missing_ids)
                with self._lock:
                    self._cache.update(new_embeddings)
                    cached_embeddings.update(new_embeddings)
            except Exception as e:
                logger.error(f"Failed to fetch embeddings after retries: {e}")
                # Return what we have from cache
                logger.warning(f"Returning {len(cached_embeddings)} cached embeddings out of {len(statement_ids)} requested")

        return cached_embeddings