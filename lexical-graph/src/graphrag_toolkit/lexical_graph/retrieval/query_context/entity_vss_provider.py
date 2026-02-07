# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List, Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.utils.entity_utils import rerank_entities
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider_base import EntityProviderBase
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider import EntityProvider
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_from_top_statement_provider import EntityFromTopStatementProvider
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from llama_index.core.schema import QueryBundle


logger = logging.getLogger(__name__)

class EntityVSSProvider(EntityProviderBase):
    
    def __init__(self, graph_store:GraphStore, vector_store:VectorStore, args:ProcessorArgs, filter_config:Optional[FilterConfig]=None):
        super().__init__(graph_store=graph_store, args=args, filter_config=filter_config)
        self.vector_store = vector_store
        self.index_name = 'topic' if not isinstance(vector_store.get_index('topic'), DummyVectorIndex) else 'chunk'
        
    def _get_node_ids(self, keywords:List[str]) -> List[str]:

        index_name = self.index_name
        id_name = f'{index_name}Id'

        query_bundle =  QueryBundle(query_str=', '.join(keywords))
        vss_results = self.vector_store.get_index(index_name).top_k(query_bundle, top_k=3, filter_config=self.filter_config)

        node_ids = [result[index_name][id_name] for result in vss_results]

        return node_ids

    def _get_entities_for_nodes(self, node_ids:List[str]) -> List[ScoredEntity]:

        if self.index_name == 'topic':
            cypher = f"""
            // get entities for topic ids
            MATCH (t:`__Topic__`)<-[:`__BELONGS_TO__`]-(:`__Statement__`)
            <-[:`__SUPPORTS__`]-()<-[:`__SUBJECT__`|`__OBJECT__`]-(entity)
            WHERE {self.graph_store.node_id("t.topicId")} in $nodeIds
            AND entity.class <> '__Local_Entity__'
            WITH DISTINCT entity
            MATCH (entity)-[r:`__SUBJECT__`|`__OBJECT__`]->()
            WITH entity, count(r) AS score ORDER BY score DESC LIMIT $limit
            RETURN {{
                {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                score: score
            }} AS result
            """
        else:
            cypher = f"""
            // get entities for chunk ids
            MATCH (c:`__Chunk__`)<-[:`__MENTIONED_IN__`]-(:`__Statement__`)
            <-[:`__SUPPORTS__`]-()<-[:`__SUBJECT__`|`__OBJECT__`]-(entity)
            WHERE {self.graph_store.node_id("c.chunkId")} in $nodeIds
            AND entity.class <> '__Local_Entity__'
            WITH DISTINCT entity
            MATCH (entity)-[r:`__SUBJECT__`|`__OBJECT__`]->()
            WITH entity, count(r) AS score ORDER BY score DESC LIMIT $limit
            RETURN {{
                {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                score: score
            }} AS result
            """

        parameters = {
            'nodeIds': node_ids,
            'limit': self.args.intermediate_limit
        }

        results = self.graph_store.execute_query(cypher, parameters)

        scored_entities = [
            ScoredEntity.model_validate(result['result'])
            for result in results
        ]

        return scored_entities
       
        
    def _get_entities_by_keyword_match(self, keywords:List[str], query_bundle:QueryBundle) -> List[ScoredEntity]:
        initial_entity_provider = EntityProvider(self.graph_store, self.args, self.filter_config)
        return initial_entity_provider.get_entities(keywords, query_bundle)
    
    def _get_entities_for_values(self, values:List[str]) -> List[ScoredEntity]:
        
        node_ids = self._get_node_ids(values)
        entities = self._get_entities_for_nodes(node_ids)

        if type(self).__name__ in self.args.debug_results:
            logger.debug(f'entities for values: [values: {values}, {self.index_name}_ids: {node_ids}, entities: {entities}]')

        return entities
    
    def _get_entities(self, keywords:List[str], query_bundle:QueryBundle) -> List[ScoredEntity]:

        all_entities_map = {}

        def add_to_entities_map(entities):
            all_entities_map.update({e.entity.entityId:e for e in entities})

        add_to_entities_map(self._get_entities_by_keyword_match(keywords, query_bundle))
        add_to_entities_map(self._get_entities_for_values([query_bundle.query_str]))
        add_to_entities_map(self._get_entities_for_values(keywords))
        
        return rerank_entities(list(all_entities_map.values()), query_bundle, keywords, self.args.reranker)
        

        