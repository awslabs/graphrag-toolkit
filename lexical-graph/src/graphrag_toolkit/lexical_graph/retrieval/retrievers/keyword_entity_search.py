# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import concurrent.futures
import logging
import asyncio
from itertools import repeat
from typing import List, Iterator, cast

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result, search_string_from
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.prompts import SIMPLE_EXTRACT_KEYWORDS_PROMPT, EXTENDED_EXTRACT_KEYWORDS_PROMPT

from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)

class KeywordEntitySearch(BaseRetriever):
    def __init__(self,
                 graph_store:GraphStore,
                 llm:LLMCacheType=None, 
                 simple_extract_keywords_template=SIMPLE_EXTRACT_KEYWORDS_PROMPT,
                 extended_extract_keywords_template=EXTENDED_EXTRACT_KEYWORDS_PROMPT,
                 max_keywords=10,
                 expand_entities=False):
        
        self.graph_store = graph_store
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache
        )
        self.simple_extract_keywords_template=simple_extract_keywords_template
        self.extended_extract_keywords_template=extended_extract_keywords_template
        self.max_keywords = max_keywords
        self.expand_entities = expand_entities

    
    def _expand_entities(self, scored_entities:List[ScoredEntity]):
        
        if not scored_entities or len(scored_entities) >= self.max_keywords:
            return scored_entities
        
        upper_score_threshold = max(sc.score for sc in scored_entities) * 2
        
        original_entity_ids = [entity.entity.entityId for entity in scored_entities if entity.score > 0]  
        neighbour_entity_ids = set()
        
        start_entity_ids = set(original_entity_ids) 
        exclude_entity_ids = set(start_entity_ids)

        for limit in range (3, 1, -1):
        
            cypher = f"""
            // expand entities
            MATCH (entity:`__Entity__`)
            -[:`__SUBJECT__`|`__OBJECT__`]->(:`__Fact__`)<-[:`__SUBJECT__`|`__OBJECT__`]-
            (other:`__Entity__`)
            WHERE  {self.graph_store.node_id('entity.entityId')} IN $entityIds
            AND NOT {self.graph_store.node_id('other.entityId')} IN $excludeEntityIds
            WITH entity, other
            MATCH (other)-[r:`__SUBJECT__`|`__OBJECT__`]->()
            WITH entity, other, count(r) AS score ORDER BY score DESC
            RETURN {{
                {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                others: collect(DISTINCT {self.graph_store.node_id('other.entityId')})[0..$limit]
            }} AS result    
            """

            params = {
                'entityIds': list(start_entity_ids),
                'excludeEntityIds': list(exclude_entity_ids),
                'limit': limit
            }
        
            results = self.graph_store.execute_query(cypher, params)

            other_entity_ids = set([
                other_id
                for result in results
                for other_id in result['result']['others'] 
            ])
            
            neighbour_entity_ids.update(other_entity_ids)

            exclude_entity_ids.update(other_entity_ids)
            start_entity_ids = other_entity_ids
            
      
        cypher = f"""
        // expand entities: score entities by number of facts
        MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`]->(f:`__Fact__`)
        WHERE {self.graph_store.node_id('entity.entityId')} IN $entityIds
        WITH entity, count(r) AS score
        RETURN {{
            {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
            score: score
        }} AS result
        """

        params = {
            'entityIds': list(neighbour_entity_ids)
        }

        results = self.graph_store.execute_query(cypher, params)
        
        neighbour_entities = [
            ScoredEntity.model_validate(result['result'])
            for result in results 
            if result['result']['entity']['entityId'] not in original_entity_ids and result['result']['score'] <= upper_score_threshold and result['result']['score'] > 0.0
        ]
        
        neighbour_entities.sort(key=lambda e:e.score, reverse=True)

        num_addition_entities = self.max_keywords - len(scored_entities)

        scored_entities.extend(neighbour_entities[:num_addition_entities])        
        scored_entities.sort(key=lambda e:e.score, reverse=True)

        logger.debug('Expanded entities:\n' + '\n'.join(
            entity.model_dump_json(exclude_unset=True, exclude_defaults=True, exclude_none=True, warnings=False) 
            for entity in scored_entities)
        )

        return scored_entities
        
    def _get_entities_for_keyword(self, keyword:str) -> List[ScoredEntity]:
        parts = keyword.split('|')

        if len(parts) > 1:

            cypher = f"""
            // get entities for keywords
            MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`|`__OBJECT__`]->(:`__Fact__`)
            WHERE entity.search_str = $keyword and entity.class STARTS WITH $classification
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
            MATCH (entity:`__Entity__`)-[r:`__SUBJECT__`|`__OBJECT__`]->(:`__Fact__`)
            WHERE entity.search_str = $keyword
            WITH entity, count(r) AS score ORDER BY score DESC
            RETURN {{
                {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
                score: score
            }} AS result"""

            params = {
                'keyword': search_string_from(parts[0])
            }

        results = self.graph_store.execute_query(cypher, params)

        return [
            ScoredEntity.model_validate(result['result'])
            for result in results
            if result['result']['score'] != 0
        ]
                        
    def _get_entities_for_keywords(self, keywords:List[str])  -> List[ScoredEntity]:
        
        tasks = [
            self._get_entities_for_keyword(keyword)
            for keyword in keywords
            if keyword
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(keywords)) as p:
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

        logger.debug('Initial entities:\n' + '\n'.join(
            entity.model_dump_json(exclude_unset=True, exclude_defaults=True, exclude_none=True, warnings=False) 
            for entity in scored_entities)
        )

        return scored_entities

        
    def _extract_keywords(self, s:str, num_keywords:int, prompt_template:str):
        results = self.llm.predict(
            PromptTemplate(template=prompt_template),
            text=s,
            max_keywords=num_keywords
        )

        keywords = results.split('^')
        return keywords

    def _get_simple_keywords(self, query, num_keywords):
        simple_keywords = self._extract_keywords(query, num_keywords, self.simple_extract_keywords_template)
        logger.debug(f'Simple keywords: {simple_keywords}')
        return simple_keywords
    
    def _get_enriched_keywords(self, query, num_keywords):
        enriched_keywords = self._extract_keywords(query, num_keywords, self.extended_extract_keywords_template)
        logger.debug(f'Enriched keywords: {enriched_keywords}')
        return enriched_keywords

    def _get_keywords(self, query, max_keywords):

        num_keywords = max(int(max_keywords/2), 1)

        with concurrent.futures.ThreadPoolExecutor() as p:
            keyword_batches: Iterator[List[str]] = p.map(
                lambda f, *args: f(*args),
                (self._get_simple_keywords, self._get_enriched_keywords),
                repeat(query),
                repeat(num_keywords)
            )
            keywords = sum(keyword_batches, start=cast(List[str], []))
            unique_keywords = list(set(keywords))

        logger.debug(f'Keywords: {unique_keywords}')
        
        return unique_keywords

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        
        query = query_bundle.query_str
        
        keywords = self._get_keywords(query, self.max_keywords)
        scored_entities:List[ScoredEntity] = self._get_entities_for_keywords(keywords)

        if self.expand_entities:
            scored_entities = self._expand_entities(scored_entities)

        return [
            NodeWithScore(
                node=TextNode(text=scored_entity.entity.model_dump_json(exclude_none=True, exclude_defaults=True, indent=2)),
                score=scored_entity.score
            ) 
            for scored_entity in scored_entities
        ]