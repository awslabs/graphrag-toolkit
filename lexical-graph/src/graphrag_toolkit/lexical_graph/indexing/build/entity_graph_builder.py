# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Fact, Entity
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import search_string_from, label_from, new_query_var
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_CLASSIFICATION, LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity

from llama_index.core.schema import BaseNode

logger = logging.getLogger(__name__)

class EntityGraphBuilder(GraphBuilder):
    """
    Handles the process of building and interacting with a graph database for entity and fact data
    representation. Supports operations to insert and manage entities and their relationships in the
    graph structure. Provides mechanisms for integrating metadata into the graph storage system.

    This class is designed to work with a specific graph storage client and encapsulates the logic
    necessary for mapping entities and facts from node data to a graph database, considering the domain
    classification when required. It assumes an ontology structure with subject and object entities
    linked by facts.

    Attributes:
        DEFAULT_CLASSIFICATION (str): The default classification for entities when not explicitly provided.
    """
    @classmethod
    def index_key(cls) -> str:
        """
        Provides a method to retrieve the index key associated with the class.

        This method is a class-level function that returns the index
        key string associated with the class. It can be used to
        uniquely identify or categorize instances of the class in
        various contexts.

        Returns:
            str: The index key associated with the class.
        """
        return 'fact'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Processes a given node and builds the corresponding entities in the graph database.

        This method extracts fact metadata from the provided node to construct nodes and
        relationships in a graph database using Cypher queries. It validates the fact
        metadata and leverages the graph_client to execute the queries. Properties for
        the subject and object are set or updated based on whether they already exist in
        the graph. Optionally, domain-specific labels can be included.

        The function handles missing fact metadata by logging a warning message. It also
        ensures reliability through query retries with controlled attempts and wait times.

        Args:
            node (BaseNode): The node from which fact metadata is to be extracted.
            graph_client (GraphStore): The graph database client to execute queries.
            **kwargs (Any): Additional options, such as `include_domain_labels`, which
                determines whether domain-specific labels are added to the entities.
        """
        fact_metadata = node.metadata.get('fact', {})
        include_domain_labels = kwargs['include_domain_labels']
        include_local_entities = kwargs['include_local_entities']

        if fact_metadata:

            fact = Fact.model_validate(fact_metadata)
            fact = string_complement_to_entity(fact)

            if fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                if not include_local_entities:
                    logger.debug(f'Ignoring local entities for fact [fact_id: {fact.factId}]')
                    return
        
            logger.debug(f'Inserting entities for fact [fact_id: {fact.factId}]')

            def insert_for_entity(entity:Entity):

                statements = [
                    '// insert entities',
                    'UNWIND $params AS params',
                    f'MERGE (entity:`__Entity__`{{{graph_client.node_id("entityId")}: params.e_id}})',
                    'ON CREATE SET entity.value = params.v, entity.search_str = params.e_search_str, entity.class = params.ec',
                    'ON MATCH SET entity.value = params.v, entity.search_str = params.e_search_str, entity.class = params.ec'
                ]

                properties = {
                    'e_id': entity.entityId,
                    'v': entity.value,
                    'e_search_str': search_string_from(entity.value),
                    'ec': entity.classification or DEFAULT_CLASSIFICATION
                }

                query = '\n'.join(statements)
                
                graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

            insert_for_entity(fact.subject)

            if fact.object and fact.object.entityId != fact.subject.entityId:
                insert_for_entity(fact.object)
            elif include_local_entities and fact.complement and fact.complement.entityId != fact.subject.entityId:
                insert_for_entity(fact.complement)

            if include_domain_labels:

                def insert_domain_entity(entity:Entity):

                    if entity.classification and entity.classification == LOCAL_ENTITY_CLASSIFICATION:
                        return

                    e_var = new_query_var()
                    e_id = entity.entityId
                    e_label = label_from(entity.classification or DEFAULT_CLASSIFICATION)
                    e_comment = f'// awsqid:{e_id}-{e_label}'
                    query_e = f"MERGE ({e_var}:`__Entity__`{{{graph_client.node_id('entityId')}: '{e_id}'}}) SET {e_var} :`{e_label}` {e_comment}"    
                    graph_client.execute_query_with_retry(query_e, {}, max_attempts=5, max_wait=7)

                insert_domain_entity(fact.subject)

                if fact.object and fact.object.entityId != fact.subject.entityId:
                    insert_domain_entity(fact.object)

                if include_local_entities and fact.complement and fact.complement.entityId != fact.subject.entityId:
                    insert_domain_entity(fact.complement)
                    
        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')