# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO, VERSION_INDEPENDENT_ID_FIELDS
from graphrag_toolkit.lexical_graph.versioning import EXTRACT_TIMESTAMP, BUILD_TIMESTAMP, PREV_VERSIONS
from graphrag_toolkit.lexical_graph.metadata import format_metadata_list

from llama_index.core.schema import BaseNode

logger = logging.getLogger(__name__)

class SourceGraphBuilder(GraphBuilder):
    """
    Handles the logic for building and inserting source nodes into a graph database.

    This class specializes in dealing with source nodes in the graph and is
    responsible for generating the appropriate queries for their insertion or
    update. It works by leveraging metadata present in the `node` and converting
    this into structured properties that are stored in the graph database. This
    class should be used whenever source nodes need to be processed to ensure
    standardized handling and insertion logic.

    Attributes:
        index_key (str): The unique key used to index source nodes in the graph
            database.
    """
    @classmethod
    def index_key(cls) -> str:
        """
        Returns the key used for indexing the class's objects.

        This method provides a string key that represents the class's
        indexing mechanism. The key can be used to uniquely identify or
        categorize objects of the class during operations such as lookups
        and mappings.

        Returns:
            str: The string key representing the indexing mechanism.
        """
        return 'source'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Builds and executes a query to insert or update a source node in the graph database based
        on data from the provided node and metadata. Handles the creation of metadata properties
        and their assignment while ensuring the correct format for insertion or updates.

        Args:
            node (BaseNode): The node containing metadata and relevant details for the source.
                It is expected to have 'sourceId' in its metadata, which will be used as a unique
                identifier in the graph database.
            graph_client (GraphStore): The client responsible for interacting with the graph
                database. Provides utility methods for query construction and execution.
            **kwargs (Any): Additional keyword arguments that may be required by the function
                for execution. These are not explicitly used in the current implementation.

        Raises:
            No explicit exceptions are raised, but error handling and logging are done for cases
            where 'sourceId' is missing in the node metadata.
        """
        
        source_metadata = node.metadata.get('source', {})
        versioning_metadata = source_metadata.get('versioning', None)
        source_id = source_metadata.get('sourceId', None)

        if source_id:

            logger.debug(f'Inserting source [source_id: {source_id}]')
        
            statements = [
                '// insert source',
                'UNWIND $params AS params',
                f"MERGE (source:`__Source__`{{{graph_client.node_id('sourceId')}: '{source_id}'}})"
            ]

            metadata = source_metadata.get('metadata', {})
            
            clean_metadata = {}
            metadata_assignments_fns = {}

            def accept_k_v(key, value):
                clean_metadata[key] = value
                metadata_assignments_fns[key] = graph_client.property_assigment_fn(key, value)

            for k, v in metadata.items():
                key = k.strip().replace(' ', '_')
                value = v
                accept_k_v(key, value)

            if versioning_metadata:
                accept_k_v(EXTRACT_TIMESTAMP, versioning_metadata['extract_timestamp'])
                accept_k_v(BUILD_TIMESTAMP, versioning_metadata['build_timestamp'])
                accept_k_v(VALID_FROM, versioning_metadata['valid_from'])
                accept_k_v(VALID_TO, versioning_metadata['valid_to'])
                if 'id_fields' in versioning_metadata:
                    accept_k_v(VERSION_INDEPENDENT_ID_FIELDS, format_metadata_list(versioning_metadata['id_fields']))
                accept_k_v(PREV_VERSIONS, format_metadata_list(versioning_metadata.get('prev_versions', [])))

            def format_assigment(key):
                assigment = f'params.{key}'
                return metadata_assignments_fns[key](assigment)
        
            if clean_metadata:
                all_properties = ', '.join(f'source.{key} = {format_assigment(key)}' for key,_ in clean_metadata.items())
                statements.append(f'ON CREATE SET {all_properties} ON MATCH SET {all_properties}')
            
            query = '\n'.join(statements)
            
            graph_client.execute_query_with_retry(query, self._to_params(clean_metadata))

            # prev_source_ids = source_metadata.get('prev_versions', [])

            # if prev_source_ids:

            #     prev_versions_statements = [
            #         '// insert prev version relations',
            #         'UNWIND $params AS params',
            #         'MATCH (source:`__Source__`), (prev:`__Source__`)',
            #         f"WHERE {graph_client.node_id('source.sourceId')} = params.sourceId AND {graph_client.node_id('prev.sourceId')} IN params.prevSourceIds",
            #         'MERGE (source)-[:`__PREVIOUS_VERSION__`]->(prev)'
            #     ]

            #     prev_versions_properties = {
            #         'sourceId': source_id,
            #         'prevSourceIds': prev_source_ids
            #     }

            #     prev_versions_query = '\n'.join(prev_versions_statements)

            #    graph_client.execute_query_with_retry(prev_versions_query, self._to_params(prev_versions_properties))

        else:
            logger.warning(f'source_id missing from source node [node_id: {node.node_id}]')