# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Fact
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
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
                'UNWIND $params AS params'
            ]

            
            statements.extend([
                f'MERGE (statement:`__Statement__`{{{graph_client.node_id("statementId")}: params.statement_id}})',
                f'MERGE (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})',
                'ON CREATE SET fact.relation = params.p, fact.value = params.fact',
                'ON MATCH SET fact.relation = params.p, fact.value = params.fact',
                'MERGE (fact)-[:`__SUPPORTS__`]->(statement)',
            ])

            properties = {
                'statement_id': fact.statementId,
                'fact_id': fact.factId,
                'fact': node.text
            }

            allow_add_entities = include_local_entities if fact.subject.classification and fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION else True

            if allow_add_entities:
                
                statements.append(f'MERGE (subject:`__Entity__`{{{graph_client.node_id("entityId")}: params.s_id}})')
                statements.append(f'MERGE (subject)-[:`__SUBJECT__`]->(fact)')

                properties.update({
                    's_id': fact.subject.entityId,
                    'p': fact.predicate.value
                })

                if fact.object:

                    statements.append(f'MERGE (object:`__Entity__`{{{graph_client.node_id("entityId")}: params.o_id}})')
                    statements.append(f'MERGE (object)-[:`__OBJECT__`]->(fact)')

                    properties.update({                
                        'o_id': fact.object.entityId
                    })

                elif fact.complement and include_local_entities:

                    statements.append(f'MERGE (complement:`__Entity__`{{{graph_client.node_id("entityId")}: params.c_id}})')
                    statements.append(f'MERGE (complement)-[:`__OBJECT__`]->(fact)')

                    properties.update({                
                        'c_id': fact.complement.entityId
                    })
        
            query = '\n'.join(statements)
                
            graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

            statements = [
                '// insert connection to prev facts',
                'UNWIND $params AS params'
            ]

            statements.extend([
                f'MATCH (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})<-[:`__SUBJECT__`]-(:`__Entity__`)-[:`__OBJECT__`]->(prevFact:`__Fact__`)',
                'MERGE (fact)<-[:`__NEXT__`]-(prevFact)'
            ])

            properties = {
                'fact_id': fact.factId
            }

            query = '\n'.join(statements)
                
            graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

            if fact.object or fact.complement:

                statements = [
                    '// insert connection to next facts',
                    'UNWIND $params AS params'
                ]
            
                statements.extend([
                    f'MATCH (fact:`__Fact__`{{{graph_client.node_id("factId")}: params.fact_id}})<-[:`__OBJECT__`]-(:`__Entity__`)-[:`__SUBJECT__`]->(nextFact:`__Fact__`)',
                    'MERGE (fact)-[:`__NEXT__`]->(nextFact)'
                ])

                properties = {
                    'fact_id': fact.factId
                }

                query = '\n'.join(statements)
                    
                graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)
           

        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')