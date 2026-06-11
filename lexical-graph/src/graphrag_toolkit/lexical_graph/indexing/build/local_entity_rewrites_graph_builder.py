# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Fact
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, Query, QueryTree
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity
from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.core.compat import BaseNode


logger = logging.getLogger(__name__)

class LocalEntityRewritesGraphBuilder(GraphBuilder):
    
    @classmethod
    def index_key(cls) -> str:
        return 'fact'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        
        fact_metadata = node.metadata.get('fact', {})
        include_local_entities = kwargs['include_local_entities']

        if fact_metadata:

            fact = Fact.model_validate(fact_metadata)
            fact = string_complement_to_entity(fact)

            if fact.subject.classification and fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                if not include_local_entities:
                    logger.debug(f'Ignoring local entity rewrites for fact [fact_id: {fact.factId}]')
                    return
                
            copy_complement_relationships_to_subject = Query(
                query=f"""// copy complement relationships to subject
                UNWIND $params AS params
                MATCH (n),
                (s)-[r:`__RELATION__`]->(c)-[:`__OBJECT__`]->(f)
                WHERE {graph_client.node_id('n.entityId')} = params.n_id AND {graph_client.node_id('c.entityId')} = params.c_id
                MERGE (s)-[:`__RELATION__`{{value:r.value}}]->(n)
                MERGE (n)-[:`__OBJECT__`]->(f)
                """
            )

            delete_complement_relationships = Query(
                query=f"""// delete complement relationships
                UNWIND $params AS params
                MATCH (s)-[r1:`__RELATION__`]->(c)-[r2:`__OBJECT__`]->(f)
                WHERE {graph_client.node_id('c.entityId')} = params.c_id
                DELETE r1
                DELETE r2
                DETACH DELETE c
                """
            )
                
            if fact.subject:

                # Subject may map to a local entity (e.g. it may be an email address value that elsewhere acts as a comlement)

                get_subject_complement_ids = Query(
                    query=f"""// get complements matching subject (fact.subject)
                    UNWIND $params AS params
                    MATCH (n),
                    (c:`__Entity__`{{search_str: n.search_str, class: '{LOCAL_ENTITY_CLASSIFICATION}'}})
                    WHERE {graph_client.node_id('n.entityId')} = params.nId AND n.class <> '{LOCAL_ENTITY_CLASSIFICATION}'
                    RETURN {graph_client.node_id('n.entityId')} AS n_id, {graph_client.node_id('c.entityId')} AS c_id
                    """,
                    child_queries=[
                        copy_complement_relationships_to_subject, 
                        delete_complement_relationships
                    ]
                )

                params = {
                    'nId': fact.subject.entityId
                }

                query_tree = QueryTree('get-complements-for-subject', get_subject_complement_ids)

                graph_client.execute_query_with_retry(query_tree, self._to_params(params), max_attempts=5, max_wait=7)


            if fact.subject and fact.complement:

                # Complement may map to a real entity (e.g. it may be an email address value that elsewhere acts as a subject entity)

                get_complement_subject_ids = Query(
                    query=f"""// get subjects matching complement (fact.complement)
                    UNWIND $params AS params
                    MATCH (n), (c)
                    WHERE {graph_client.node_id('n.entityId')} = params.nId AND {graph_client.node_id('c.entityId')} = params.cId
                    RETURN {graph_client.node_id('n.entityId')} AS n_id, {graph_client.node_id('c.entityId')} AS c_id
                    """,
                    child_queries=[
                        copy_complement_relationships_to_subject, 
                        delete_complement_relationships
                    ]
                )

                params = {
                    'nId': fact.complement.altEntityId,
                    'cId': fact.complement.entityId
                }

                query_tree = QueryTree('get-real-subjects-for-complement', get_complement_subject_ids)

                graph_client.execute_query_with_retry(query_tree, self._to_params(params), max_attempts=5, max_wait=7)



        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')