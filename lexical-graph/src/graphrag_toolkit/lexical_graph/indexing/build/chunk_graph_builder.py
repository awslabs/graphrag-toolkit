# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder

from llama_index.core.schema import BaseNode
from llama_index.core.schema import NodeRelationship

logger = logging.getLogger(__name__)

class ChunkGraphBuilder(GraphBuilder):
    """Class responsible for building and managing a graph representation
    for chunks.

    The ChunkGraphBuilder class specializes in generating graph structures
    that represent "chunks" from input nodes. It retrieves metadata,
    relationships, and properties from the given node and translates
    them into database queries for storing this structure in a graph
    database. This class also handles linking chunks with their sources,
    parent nodes, child nodes, and their sequential order based on defined
    relationships.

    Attributes:
        _some_attribute (type): Description of an attribute (if any). Replace
        or expand this list with specific attributes used by the class.

    """
    @classmethod
    def index_key(cls) -> str:
        """
        Returns a string key that identifies the index used within a given context.

        Returns:
            str: A string representing the index key 'chunk'.
        """
        return 'chunk'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Builds and inserts a chunk node along with its relationships into a graph database. Handles the
        insertion of child, parent, previous, next, and source relationships. If a chunk ID or required
        relationship information is missing, the function logs warnings.

        Args:
            node: The node object containing chunk data and its relationships.
            graph_client: The graph client interface to interact with the graph database.
            **kwargs: Additional optional parameters for configuring the operation.
        """
        chunk_id = node.metadata.get('chunk', {}).get('chunkId', None)
        chunk_metadata = node.metadata.get('chunk', {}).get('metadata', {})
        
        if chunk_id:

            logger.debug(f'Inserting chunk [chunk_id: {chunk_id}]')

            statements_c = [
                '// insert chunks',
                'UNWIND $params AS params',
                f'MERGE (chunk:`__Chunk__`{{{graph_client.node_id("chunkId")}: params.chunk_id}})'
            ]

            chunk_property_setters = [
                'chunk.value = params.text'
            ]
            
            properties_c = {
                'chunk_id': chunk_id,
                'text': node.text
            }

            # Add external properties if present
            for key, value in chunk_metadata.items():
                if key != 'chunkId':  # Skip the ID field
                    chunk_property_setters.append(f'chunk.{key} = params.{key}')
                    properties_c[key] = value

            setter_statement = f"ON CREATE SET {', '.join(chunk_property_setters)} ON MATCH SET {', '.join(chunk_property_setters)}" 
            statements_c.append(setter_statement)

            query_c = '\n'.join(statements_c)

            graph_client.execute_query_with_retry(query_c, self._to_params(properties_c), max_attempts=5, max_wait=7)

            source_info = node.relationships.get(NodeRelationship.SOURCE, None)

            if source_info:
                
                source_id = source_info.node_id

                statements_s = [
                    '// insert chunk-source relationships',
                    'UNWIND $params AS params',
                    f'MERGE (chunk:`__Chunk__`{{{graph_client.node_id("chunkId")}: params.chunk_id}})',
                    f'MERGE (source:`__Source__`{{{graph_client.node_id("sourceId")}: params.source_id}})',
                    'MERGE (chunk)-[:`__EXTRACTED_FROM__`]->(source)'
                ]

                properties_s = {
                    'chunk_id': chunk_id,
                    'source_id': source_id
                }

                query_s = '\n'.join(statements_s)

                graph_client.execute_query_with_retry(query_s, self._to_params(properties_s), max_attempts=5, max_wait=7)

            else:
                logger.warning(f'source_id missing from chunk node [node_id: {chunk_id}]')

            
            def insert_chunk_to_chunk_relationship(node_id:str, relationship_type:str):

                statements_c2c = [
                    f'// insert chunk-chunk {relationship_type.lower()} relationships',
                    'UNWIND $params AS params',
                    f'MERGE (chunk:`__Chunk__`{{{graph_client.node_id("chunkId")}: params.chunk_id}})', 
                    f'MERGE (target:`__Chunk__`{{{graph_client.node_id("chunkId")}: params.target_id}})',
                    f'MERGE (chunk)-[:`__{relationship_type.upper()}__`]->(target)'
                ]

                properties_c2c = {
                    'chunk_id': chunk_id,
                    'target_id': node_id
                }

                query_c2c = '\n'.join(statements_c2c)

                graph_client.execute_query_with_retry(query_c2c, self._to_params(properties_c2c), max_attempts=5, max_wait=7)


            for node_relationship,relationship_info in node.relationships.items():

                node_id = relationship_info.node_id

                if node_relationship == NodeRelationship.PARENT:
                    insert_chunk_to_chunk_relationship(node_id, 'parent')   
                elif node_relationship == NodeRelationship.CHILD:
                    insert_chunk_to_chunk_relationship(node_id, 'child')     
                elif node_relationship == NodeRelationship.PREVIOUS:
                    insert_chunk_to_chunk_relationship(node_id, 'previous')    
                elif node_relationship == NodeRelationship.NEXT:
                    insert_chunk_to_chunk_relationship(node_id, 'next')

        else:
            logger.warning(f'chunk_id missing from chunk node [node_id: {node.node_id}]')
