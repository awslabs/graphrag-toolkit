# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List, Sequence, Any

from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod

from llama_index.core.schema import QueryBundle

DUMMY = 'dummy://'

logger = logging.getLogger(__name__)

class DummyVectorIndexFactory(VectorIndexFactoryMethod):
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
        if vector_index_info.startswith(DUMMY):
            logger.debug(f'Opening dummy vector indexes [index_names: {index_names}]')
            return [DummyVectorIndex(index_name=index_name) for index_name in index_names]
        else:
            return None


class DummyVectorIndex(VectorIndex):

    def add_embeddings(self, nodes):
        logger.debug(f'[{self.index_name}] add embeddings for nodes: {[n.id_ for n in nodes]}')
    
    def top_k(self, query_bundle:QueryBundle, top_k:int=5) -> Sequence[Any]:
        logger.debug(f'[{self.index_name}] top k query: {query_bundle.query_str}, top_k: {top_k}')
        return []

    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Any]:
        logger.debug(f'[{self.index_name}] get embeddings for ids: {ids}')
        return []
