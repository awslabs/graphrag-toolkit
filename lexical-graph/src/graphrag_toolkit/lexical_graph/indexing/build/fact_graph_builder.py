# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Optional

from graphrag_toolkit.lexical_graph.indexing.model import Fact
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, Query, QueryTree
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity

from llama_index.core.schema import BaseNode

logger = logging.getLogger(__name__)

class FactGraphBuilder(GraphBuilder):
    """
    Builds fact-related nodes and relationships in the graph database.

    This class is responsible for implementing the logic to construct and insert
    fact-related nodes and their relationships into the graph database. It utilizes
    the metadata of the provided node and executes cypher queries to persist the data.

    Attributes:
        None
    """
    @classmethod
    def index_key(cls) -> str:
        """
        Provides a method to retrieve the index key for the class.

        This method is a class-level utility that returns a predefined string
        representing the index key. It encapsulates the logic for fetching the
        key associated with the class data.

        Returns:
            str: The index key associated with the class, which is 'fact'.
        """
        return 'fact'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Builds and processes the relationships and properties for a fact node within a graph database.

        This function is responsible for validating metadata of a node, constructing query statements to
        add nodes and relationships to the graph database, and executing those queries. The function also
        handles scenarios where domain labels are required or when connections to previous or next facts need
        to be established.

        Args:
            node (BaseNode): The node containing metadata and text to be analyzed and inserted into the
                graph database as a fact.
            graph_client (GraphStore): The client used to interact with the graph database, providing methods
                for query execution and schema utility functions.
            **kwargs (Any): Additional parameters for customization, such as 'include_domain_labels' to
                determine whether domain labels for entities should be included in the queries.

        Raises:
            KeyError: If required keys such as 'include_domain_labels' are missing from the kwargs or
                metadata is incomplete.
            ValidationError: If the metadata provided in the node is invalid or does not match the expected
                structure for fact validation.
        """
        fact_metadata = node.metadata.get('fact', {})
        include_local_entities = kwargs['include_local_entities']
        
        if fact_metadata:

            fact = Fact.model_validate(fact_metadata)
            fact = string_complement_to_entity(fact)
        
            logger.debug(f'Inserting fact [fact_id: {fact.factId}]')

            statements = [
                '// insert facts',
                'UNWIND $params AS params',
                f'MERGE (statement:`__Statement__`{{{graph_client.node_id("statementId")}: params.statement_id}})',
                f'MERGE (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})',
                'ON CREATE SET fact.relation = params.p, fact.value = params.fact',
                'ON MATCH SET fact.relation = params.p, fact.value = params.fact',
                'MERGE (fact)-[:`__SUPPORTS__`]->(statement)'
            ]

            properties = {
                'statement_id': fact.statementId,
                'fact_id': fact.factId,
                'fact': node.text
            }

            query = '\n'.join(statements)
                
            graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

            def insert_entity_fact_relationship(relationship_type:str, entity_id:Optional[str]=None):

                statements_e2f = [
                    f'// insert entity-fact {relationship_type.lower()} relationship',
                    'UNWIND $params AS params',
                    f'MERGE (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})',
                    f'MERGE (entity:`__Entity__`{{{graph_client.node_id("entityId")}: params.entity_id}})',
                    f'MERGE (entity)-[:`__{relationship_type.upper()}__`]->(fact)'              
                ]

                properties_e2f = {}

                if entity_id:
                    properties_e2f['fact_id'] = fact.factId
                    properties_e2f['entity_id'] = entity_id
        
                query_e2f = '\n'.join(statements_e2f)
                
                graph_client.execute_query_with_retry(query_e2f, self._to_params(properties_e2f), max_attempts=5, max_wait=7)

            
            insert_entity_fact_relationship('subject')
            insert_entity_fact_relationship('object')
            
            if fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                if include_local_entities:
                    insert_entity_fact_relationship('subject', fact.subject.entityId)
                    if fact.object:
                        insert_entity_fact_relationship('object', fact.object.entityId)
                    if fact.complement:
                        insert_entity_fact_relationship('object', fact.complement.entityId)
            else:
                insert_entity_fact_relationship('subject', fact.subject.entityId)
                if fact.object:
                    insert_entity_fact_relationship('object', fact.object.entityId)          
                if fact.complement and include_local_entities:
                    insert_entity_fact_relationship('object', fact.complement.entityId)

            create_next_relationship = Query(
                query=f"""// insert connection to prev facts
                UNWIND $params AS params
                MATCH (start{{{graph_client.node_id("factId")}: params.startId}}), (end{{{graph_client.node_id("factId")}: params.endId}})
                MERGE (start)-[:`__NEXT__`]->(end)
                """
            )

            find_start_end_for_prev_facts = Query(
                query=f"""// get start and end facts for prev conection
                UNWIND $params AS params
                MATCH (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})<-[:`__SUBJECT__`]-(:`__Entity__`)-[:`__OBJECT__`]->(prevFact:`__Fact__`)
                WHERE fact <> prevFact
                RETURN {graph_client.node_id('prevFact.factId')} AS startId, {graph_client.node_id('fact.factId')} AS endId
                """,
                child_queries=[create_next_relationship]
            )

            params = {
                'fact_id': fact.factId
            }

            query_tree = QueryTree('insert-prev-facts', find_start_end_for_prev_facts)

            graph_client.execute_query_with_retry(query_tree, self._to_params(params), max_attempts=10, max_wait=10)

            if fact.object or fact.complement:

                find_start_end_for_next_facts = Query(
                    query=f"""// get start and end facts for next conection
                    UNWIND $params AS params
                    MATCH (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})<-[:`__OBJECT__`]-(:`__Entity__`)-[:`__SUBJECT__`]->(nextFact:`__Fact__`)
                    WHERE fact <> nextFact
                    RETURN {graph_client.node_id('fact.factId')} AS startId, {graph_client.node_id('nextFact.factId')} AS endId
                    """,
                    child_queries=[create_next_relationship]
                )

                params = {
                    'fact_id': fact.factId
                }

                query_tree = QueryTree('insert-next-facts', find_start_end_for_next_facts)

                graph_client.execute_query_with_retry(query_tree, self._to_params(params), max_attempts=10, max_wait=10)
           
        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')