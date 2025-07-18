# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Fact
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import relationship_name_from, new_query_var
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity
from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION

from llama_index.core.schema import BaseNode

logger = logging.getLogger(__name__)

class EntityRelationGraphBuilder(GraphBuilder):
    """
    EntityRelationGraphBuilder is a specialized builder class responsible for creating
    entity relationship graphs based on fact data contained within nodes. It processes
    facts, generates corresponding graph relationships, and interacts with the graph store
    to persist these relationships. This class integrates domain-specific metadata and is
    configured to handle optional domain labels for nodes and relationships.

    Attributes:
        DEFAULT_CLASSIFICATION (str): The default classification label used when fact
            classification is not provided.
    """
    @classmethod
    def index_key(cls) -> str:
        """
        Returns the index key associated with this class. The index key is
        a unique identifier for objects of this type, allowing for effective
        lookup or indexing operations.

        Returns:
            str: The index key for the class instance.
        """
        return 'fact'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Builds and executes entity-relation creation logic based on the given node and fact metadata. It processes
        the metadata to create or update relationships between entities in a graph database. If `include_domain_labels`
        is specified, additional domain-specific labels will be added to the entities and relationships.

        Args:
            node (BaseNode): The node containing metadata for the fact data to process.
            graph_client (GraphStore): The graph client instance for executing database queries.
            **kwargs (Any): Additional keyword arguments. Must include:
                - include_domain_labels (bool): Indicator for whether domain labels should be included in the query.

        """
        fact_metadata = node.metadata.get('fact', {})
        include_domain_labels = kwargs['include_domain_labels']
        include_local_entities = kwargs['include_local_entities']

        if fact_metadata:

            fact = Fact.model_validate(fact_metadata)
            fact = string_complement_to_entity(fact)

            if fact.subject.classification and fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                if not include_local_entities:
                    logger.debug(f'Ignoring local entity relations for fact [fact_id: {fact.factId}]')
                    return

            if fact.subject and fact.object:
        
                logger.debug(f'Inserting entity SPO relations for fact [fact_id: {fact.factId}]')

                statements = [
                    '// insert entity SPO relations',
                    'UNWIND $params AS params'
                ]

                statements.append(f'MERGE (subject:`__Entity__`{{{graph_client.node_id("entityId")}: params.s_id}})')
                statements.append(f'MERGE (object:`__Entity__`{{{graph_client.node_id("entityId")}: params.o_id}})')

                statements.extend([
                    'MERGE (subject)-[r:`__RELATION__`{value: params.p}]->(object)',
                    'ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1'
                ])

                properties = {
                    's_id': fact.subject.entityId,
                    'o_id': fact.object.entityId,
                    'p': fact.predicate.value
                }
            
                query = '\n'.join(statements)
                    
                graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

                if include_domain_labels:

                    s_var = new_query_var()
                    o_var = new_query_var()
                    r_var = new_query_var()
                    s_id = fact.subject.entityId
                    o_id = fact.object.entityId
                    r_name = relationship_name_from(fact.predicate.value)
                    r_comment = f'// awsqid:{s_id}-{r_name}-{o_id}'

                    statements_r = [
                        f"MERGE ({s_var}:`__Entity__`{{{graph_client.node_id('entityId')}: '{s_id}'}})",
                        f"MERGE ({o_var}:`__Entity__`{{{graph_client.node_id('entityId')}: '{o_id}'}})",
                        f"MERGE ({s_var})-[{r_var}:`{r_name}`]->({o_var})",
                        f"ON CREATE SET {r_var}.count = 1 ON MATCH SET {r_var}.count = {r_var}.count + 1",
                        r_comment
                    ]

                    query_r = ' '.join(statements_r)

                    graph_client.execute_query_with_retry(query_r, {}, max_attempts=5, max_wait=7)
            
            elif include_local_entities and fact.subject and fact.complement:
        
                logger.debug(f'Inserting entity SPC relations for fact [fact_id: {fact.factId}]')

                statements = [
                    '// insert entity SPC relations',
                    'UNWIND $params AS params'
                ]

                statements.append(f'MERGE (subject:`__Entity__`{{{graph_client.node_id("entityId")}: params.s_id}})')
                statements.append(f'MERGE (complement:`__Entity__`{{{graph_client.node_id("entityId")}: params.c_id}})')

                statements.extend([
                    'MERGE (subject)-[r:`__RELATION__`{value: params.p}]->(complement)',
                    'ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1'
                ])

                properties = {
                    's_id': fact.subject.entityId,
                    'c_id': fact.complement.entityId,
                    'p': fact.predicate.value
                }
            
                query = '\n'.join(statements)
                    
                graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

                if include_domain_labels:

                    s_var = new_query_var()
                    c_var = new_query_var()
                    r_var = new_query_var()
                    s_id = fact.subject.entityId
                    c_id = fact.complement.entityId
                    r_name = relationship_name_from(fact.predicate.value)
                    r_comment = f'// awsqid:{s_id}-{r_name}-{c_id}'

                    statements_r = [
                        f"MERGE ({s_var}:`__Entity__`{{{graph_client.node_id('entityId')}: '{s_id}'}})",
                        f"MERGE ({c_var}:`__Entity__`{{{graph_client.node_id('entityId')}: '{c_id}'}})",
                        f"MERGE ({s_var})-[{r_var}:`{r_name}`]->({c_var})",
                        f"ON CREATE SET {r_var}.count = 1 ON MATCH SET {r_var}.count = {r_var}.count + 1",
                        r_comment
                    ]

                    query_r = ' '.join(statements_r)

                    graph_client.execute_query_with_retry(query_r, {}, max_attempts=5, max_wait=7)

            else:
                logger.debug(f'Neither an SPO nor SPC fact, so not creating relation [fact_id: {fact.factId}]')
           

        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')