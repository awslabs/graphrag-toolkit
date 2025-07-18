# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Fact
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import label_from, relationship_name_from
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_CLASSIFICATION, LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity

from llama_index.core.schema import BaseNode

logger = logging.getLogger(__name__)

class GraphSummaryBuilder(GraphBuilder):
    """
    GraphSummaryBuilder is responsible for building and inserting graph summaries.

    This class extends the functionality of GraphBuilder to process nodes with
    fact metadata and update the graph data in a GraphStore. It validates fact
    metadata and generates graph representations to maintain consistent
    relationships and class counts between subject and object entities.

    Attributes:
        DEFAULT_CLASSIFICATION (str): Default classification value used when
            classification attributes are not provided.
    """
    @classmethod
    def index_key(cls) -> str:
        """
        A utility method to retrieve the key used for indexing within the class.

        This class method provides a standardized way to access the key that
        represents a specific purpose or data categorization within the class.
        Primarily, it is used as a constant reference across the application.

        Returns:
            str: The key used for indexing, which is 'fact' in this case.
        """
        return 'fact'
    
    def build(self, node:BaseNode, graph_client:GraphStore, **kwargs:Any):
        """
        Builds and executes a query to summarize graph data in the graph database based on
        the metadata associated with a given base node. The process involves constructing
        parameters and Cypher queries to manage relationships and classifications for nodes
        within a graph database. This method uses the `graph_client` to run the query with
        error retries and logs warnings for missing metadata.

        Args:
            node (BaseNode): The node containing metadata for generating the query. This
                metadata is used to derive the relationships and classifications required to
                configure the graph.
            graph_client (GraphStore): The client responsible for interacting with the
                backend graph database. It provides methods for query execution and ID
                formatting.
            **kwargs (Any): Additional arguments that may be passed for internal usage.
        """
        fact_metadata = node.metadata.get('fact', {})
        include_local_entities = kwargs['include_local_entities']
        
        if fact_metadata:

            fact = Fact.model_validate(fact_metadata)
            fact = string_complement_to_entity(fact)

            if fact.subject.classification and fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                if not include_local_entities:
                    logger.debug(f'Ignoring local entity relations for graph summary [fact_id: {fact.factId}]')
                    return

            if fact.subject and fact.object:

                tenant_id = graph_client.tenant_id
                
                sc_id = tenant_id.format_id('sys_class', fact.subject.classification or DEFAULT_CLASSIFICATION)
                oc_id = tenant_id.format_id('sys_class', fact.object.classification or DEFAULT_CLASSIFICATION)

                statements = []

                if sc_id == oc_id:

                    statements.extend( [
                        '// insert graph summary',
                        'UNWIND $params AS params',
                        f'MERGE (sc:`__SYS_Class__`{{{graph_client.node_id("sysClassId")}: params.sc_id}})',
                        'ON CREATE SET sc.value = params.sc, sc.count = 2 ON MATCH SET sc.count = sc.count + 2',                       
                        'MERGE (sc)-[r:`__SYS_RELATION__`{value: params.p}]->(sc)',
                        'ON CREATE SET r.count = 2 ON MATCH SET r.count = r.count + 2'                      
                    ])

                else:

                    statements.extend( [
                        '// insert graph summary',
                        'UNWIND $params AS params',
                        f'MERGE (sc:`__SYS_Class__`{{{graph_client.node_id("sysClassId")}: params.sc_id}})',
                        'ON CREATE SET sc.value = params.sc, sc.count = 1 ON MATCH SET sc.count = sc.count + 1',
                        f'MERGE (oc:`__SYS_Class__`{{{graph_client.node_id("sysClassId")}: params.oc_id}})',
                        'ON CREATE SET oc.value = params.oc, oc.count = 1 ON MATCH SET oc.count = oc.count + 1',
                        'MERGE (sc)-[r:`__SYS_RELATION__`{value: params.p}]->(oc)',
                        'ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1'
                        
                    ])

                properties = {
                    'sc_id': sc_id,
                    'oc_id': oc_id,
                    'sc': label_from(fact.subject.classification or DEFAULT_CLASSIFICATION),
                    'oc': label_from(fact.object.classification or DEFAULT_CLASSIFICATION),
                    'p': relationship_name_from(fact.predicate.value),
                }

                query = '\n'.join(statements)
                    
                graph_client.execute_query_with_retry(query, self._to_params(properties), max_attempts=5, max_wait=7)

        else:
            logger.warning(f'fact_id missing from fact node [node_id: {node.node_id}]')