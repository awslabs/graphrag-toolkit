# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import concurrent.futures
import logging
from typing import List, Iterator, cast, Optional

from graphrag_toolkit.lexical_graph.storage.graph import GraphOperation, GraphStore
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result, search_string_from, filter_config_to_opencypher_filters
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider_base import EntityProviderBase
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

class EntityProvider(EntityProviderBase):
    
    def __init__(self, graph_store:GraphStore, args:ProcessorArgs, filter_config:Optional[FilterConfig]=None):
        super().__init__(graph_store=graph_store, args=args, filter_config=filter_config)

        
    def _get_entities_for_keyword(self, keyword:str) -> List[ScoredEntity]:

        parts = keyword.split('|')

        if len(parts) > 1:

            cypher = f"""
            // get entities for keywords
            MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`|`__OBJECT__`]->()
            WHERE entity.search_str = $keyword and entity.class = $classification
            WITH entity, count(r) AS score ORDER BY score DESC
            RETURN {{
                {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                score: score
            }} AS result"""

            params = {
                'keyword': search_string_from(parts[0]),
                'classification': parts[1]
            }
        else:
            cypher = f"""
            // get entities for keywords
            MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`|`__OBJECT__`]->()
            WHERE entity.search_str = $keyword
            AND entity.class <> '__Local_Entity__'
            WITH entity, count(r) AS score ORDER BY score DESC
            RETURN {{
                {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                score: score
            }} AS result"""

            params = {
                'keyword': search_string_from(parts[0])
            }

        results = self.graph_store.execute_query(cypher, params, operation=GraphOperation.FIND_ENTITIES_BY_KEYWORD)

        entities = [
            ScoredEntity.model_validate(result['result'])
            for result in results
            if result['result']['score'] != 0
        ]

        if not entities:
            if len(parts) > 1:

                cypher = f"""
                // get entities for keywords
                MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`|`__OBJECT__`]->()
                WHERE entity.search_str STARTS WITH $keyword and entity.class STARTS WITH $classification
                WITH entity, count(r) AS score ORDER BY score DESC
                RETURN {{
                    {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                    score: score
                }} AS result"""

                params = {
                    'keyword': search_string_from(parts[0]),
                    'classification': parts[1],
                    '_starts_with': True,
                    '_classification_starts_with': True,
                }
            else:
                cypher = f"""
                // get entities for keywords
                MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`|`__OBJECT__`]->()
                WHERE entity.search_str STARTS WITH $keyword
                AND entity.class <> '__Local_Entity__'
                WITH entity, count(r) AS score ORDER BY score DESC
                RETURN {{
                    {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                    score: score
                }} AS result"""

                params = {
                    'keyword': search_string_from(parts[0]),
                    '_starts_with': True,
                }

            results = self.graph_store.execute_query(cypher, params, operation=GraphOperation.FIND_ENTITIES_BY_KEYWORD)

            entities = [
                ScoredEntity.model_validate(result['result'])
                for result in results
                if result['result']['score'] != 0
            ]

        return entities

                        
    def _get_entities(self, keywords:List[str], query_bundle:QueryBundle)  -> List[ScoredEntity]:

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.num_workers) as p:
            scored_entity_batches:Iterator[List[ScoredEntity]] = p.map(self._get_entities_for_keyword, keywords)
            scored_entities = sum(scored_entity_batches, start=cast(List[ScoredEntity], []))

        scored_entity_mappings = {}
        
        for scored_entity in scored_entities:
            entity_id = scored_entity.entity.entityId
            if entity_id not in scored_entity_mappings:
                scored_entity_mappings[entity_id] = scored_entity
            else:
                scored_entity_mappings[entity_id].score += scored_entity.score

        scored_entities = list(scored_entity_mappings.values())

        scored_entities.sort(key=lambda e:e.score, reverse=True)

        return scored_entities

        