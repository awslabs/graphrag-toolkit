# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List, Optional, Dict

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.utils.reranker_utils import score_values_with_tfidf
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider_base import EntityProviderBase
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider import EntityProvider
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker

from llama_index.core.schema import QueryBundle, NodeWithScore, TextNode


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
        vss_results = self.vector_store.get_index(index_name).top_k(query_bundle, 3, filter_config=self.filter_config)

        node_ids = [result[index_name][id_name] for result in vss_results]

        return node_ids

    def _get_entities_for_nodes(self, node_ids:List[str]) -> List[ScoredEntity]:

        if self.index_name == 'topic':
            cypher = f"""
            // get entities for topic ids
            MATCH (t:`__Topic__`)<-[:`__BELONGS_TO__`]-(:`__Statement__`)
            <-[:`__SUPPORTS__`]-()<-[:`__SUBJECT__`|`__OBJECT__`]-(entity)
            WHERE {self.graph_store.node_id("t.topicId")} in $nodeIds
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
    
    def _update_reranked_entity_name_scores(self, reranked_entity_names:Dict[str, float], keywords:List[str]):

        num_keywords = len(keywords)

        for i, keyword in enumerate(keywords):
            multiplier = num_keywords - i
            entity_reranking_score = reranked_entity_names.get(keyword, None)
            if entity_reranking_score:
                reranked_entity_names[keyword] = entity_reranking_score * multiplier

        return reranked_entity_names
    
    def _get_reranked_entity_names_model(self, entities:List[ScoredEntity], keywords:List[str]) -> Dict[str, float]:

        reranker = SentenceReranker(model=GraphRAGConfig.reranking_model, top_n=3)
        rank_query = QueryBundle(query_str=' '.join(keywords))

        reranked_values = reranker.postprocess_nodes(
            [
                NodeWithScore(node=TextNode(text=entity.entity.value.lower()), score=0.0)
                for entity in entities
            ],
            rank_query
        )

        reranked_entity_names =  {
            reranked_value.text : reranked_value.score
            for reranked_value in reranked_values
        }

        logger.debug(f'reranking (model): [keywords: {keywords}, reranked_entity_names: {reranked_entity_names}]')

        return reranked_entity_names
    
    def _get_reranked_entity_names_tfidf(self, entities:List[ScoredEntity], keywords:List[str]) -> Dict[str, float]:
        
        entity_names = [entity.entity.value.lower() for entity in entities]
        reranked_entity_names = score_values_with_tfidf(entity_names, keywords)

        logger.debug(f'reranking (tfidf): [keywords: {keywords}, reranked_entity_names: {reranked_entity_names}]')

        return reranked_entity_names

    def _get_reranked_entity_names(self, entities:List[ScoredEntity], keywords:List[str]) -> Dict[str, float]:
 
        if self.args.reranker == 'model':
            results = self._get_reranked_entity_names_model(entities, keywords) 
        else:
            results = self._get_reranked_entity_names_tfidf(entities, keywords)

        return {
            k:round(v, 4) for k,v in results.items()
        }
        
    def _get_entities_by_keyword_match(self, keywords:List[str], query_bundle:QueryBundle) -> List[ScoredEntity]:
        initial_entity_provider = EntityProvider(self.graph_store, self.args, self.filter_config)
        return initial_entity_provider.get_entities(keywords, query_bundle)
    
    def _get_entities_for_values(self, values:List[str]) -> List[ScoredEntity]:
        
        node_ids = self._get_node_ids(values)
        entities = self._get_entities_for_nodes(node_ids)

        logger.debug(f'entities for values: [values: {values}, {self.index_name}_ids: {node_ids}, entities: {entities}]')

        reranked_entity_names = self._get_reranked_entity_names(entities, values)
        return self._get_reranked_entities(entities, reranked_entity_names)
                        
    def _get_reranked_entities(self, entities:List[ScoredEntity], reranked_entity_names:Dict[str, float]) -> List[ScoredEntity]:

        entity_id_map = {}

        for reranked_entity_name, reranking_score in reranked_entity_names.items():
            for entity in entities:
                if entity.entity.value.lower() == reranked_entity_name and entity.entity.entityId not in entity_id_map:
                    entity.reranking_score = reranking_score
                    entity_id_map[entity.entity.entityId] = None
                    

        entities.sort(key=lambda e: (-e.reranking_score, -e.score))

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'''reranked_entities: {[
                entity.model_dump_json(exclude_unset=True, exclude_none=True, warnings=False) 
                for entity in entities
            ]}''')

        return entities
    
    def _get_entities(self, keywords:List[str], query_bundle:QueryBundle) -> List[ScoredEntity]:

        all_entities_map = {}

        def add_to_entities_map(entities):
            all_entities_map.update({e.entity.entityId:e for e in entities})

        add_to_entities_map(self._get_entities_by_keyword_match(keywords, query_bundle))
        add_to_entities_map(self._get_entities_for_values([query_bundle.query_str])[:3])
        add_to_entities_map(self._get_entities_for_values(keywords)[:3])
        
        all_reranked_entity_names = self._get_reranked_entity_names(list(all_entities_map.values()), [query_bundle.query_str] + keywords)
        all_reranked_entities = self._get_reranked_entities(list(all_entities_map.values()), all_reranked_entity_names)
        
        return all_reranked_entities

        