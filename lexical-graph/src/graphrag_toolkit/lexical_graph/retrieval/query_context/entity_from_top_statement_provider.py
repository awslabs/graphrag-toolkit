# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List, Optional

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.utils.reranker_utils import score_values_with_tfidf
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider_base import EntityProviderBase

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

class EntityFromTopStatementProvider(EntityProviderBase):
    
    def __init__(self, graph_store:GraphStore, vector_store:VectorStore, args:ProcessorArgs, filter_config:Optional[FilterConfig]=None):
        super().__init__(graph_store=graph_store, args=args, filter_config=filter_config)
        self.vector_store = vector_store
        self.index_name = 'topic' if not isinstance(vector_store.get_index('topic'), DummyVectorIndex) else 'chunk'

    def _get_top_statement_id(self, query_bundle:QueryBundle) -> str:
        
        logger.debug(f"Query: {query_bundle.query_str}")

        index_name = self.index_name
        id_name = f'{index_name}Id'
        
        top_k_results = self.vector_store.get_index(index_name).top_k(query_bundle, 3)
        node_ids = [r[index_name][id_name] for r in top_k_results]

        if self.index_name == 'topic':
        
            cypher = f'''
            // Get statements for top chunk
            MATCH (t:`__Topic__`)<-[:`__MENTIONED_IN__`]-(s:`__Statement__`)
            WHERE {self.graph_store.node_id("t.topicId")} in $nodeIds 
            RETURN {{
                statement: s.value,
                statementId: id(s)
            }} AS result
            '''

        else:
        
            cypher = f'''
            // Get statements for top chunk
            MATCH (c:`__Chunk__`)<-[:`__MENTIONED_IN__`]-(s:`__Statement__`)
            WHERE {self.graph_store.node_id("c.chunkId")} in $nodeIds 
            RETURN {{
                statement: s.value,
                statementId: id(s)
            }} AS result
            '''
        
        properties = {
            'nodeIds': node_ids
        }
    
    
        results = self.graph_store.execute_query(cypher, properties)
        
        statements = {
            r['result']['statement']:r['result']['statementId'] for r in results
        }

        logger.debug(f"Top statements: {statements}")
        
        scored_statements = score_values_with_tfidf(list(statements.keys()), [query_bundle.query_str], 1)
        
        top_statement = list(scored_statements.keys())[0]
        
        logger.debug(f"Top statement: {top_statement}")

        if scored_statements:
            return statements[top_statement]
        else:
            return None
    
    def _get_entities_for_statement(self, statement_id:str) -> List[str]:
        
        cypher = f'''
        // Get entities for statement
        MATCH (s)<-[:`__SUPPORTS__`]-(f)<-[:`__SUBJECT__`|`__OBJECT__`]-(entity)
        WHERE {self.graph_store.node_id("s.statementId")} in $statementIds
        AND entity.class <> '__Local_Entity__'
        WITH DISTINCT entity
        OPTIONAL MATCH (entity)-[r:`__SUBJECT__`|`__OBJECT__`]->()
        WITH entity, count(r) AS score ORDER BY score DESC
        RETURN {{
            {node_result('entity', self.graph_store.node_id('entity.entityId'), properties=['value', 'class'])},
            score: score
        }} AS result'''

        properties = {
            'statementIds': [statement_id]
        }
    
        results = self.graph_store.execute_query(cypher, properties)

        scored_entities = [
            ScoredEntity.model_validate(result['result'])
            for result in results
        ]

        return scored_entities

    def _get_entities(self, keywords:List[str], query_bundle:QueryBundle) -> List[ScoredEntity]:
        top_statement_id = self._get_top_statement_id(query_bundle)
        if not top_statement_id:
            return []
        return self._get_entities_for_statement(top_statement_id)


       