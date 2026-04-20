# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from llama_index.core.schema import BaseNode, DEFAULT_TEXT_NODE_TMPL
from llama_index.core.schema import NodeRelationship

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

class ChunkNodeBuilder(NodeBuilder):
    """
    Builder class for creating chunk nodes from a list of base nodes.

    This class is responsible for constructing chunk nodes by copying the base
    nodes' structure, modifying their metadata, and setting specific attributes
    related to chunks. Chunk nodes are customized versions of base nodes that
    carry additional metadata about chunks and topics, which are used for further
    processing in the node relationship structure.

    Attributes:
        DEFAULT_TEXT_NODE_TMPL (str): A constant text template used in chunk nodes.
        TOPICS_KEY (str): Metadata key related to topics in the node.
        INDEX_KEY (str): Key used for indexing metadata.
    """
    @classmethod
    def name(cls) -> str:
        """
        Provides a class method to retrieve the name of the class.

        This method is designed to return a string representation of the class
        name. It does not take any external arguments, and its use is intended
        primarily for scenarios where the class name is needed dynamically.

        Returns:
            str: The name of the class, 'ChunkNodeBuilder'.
        """
        return 'ChunkNodeBuilder'
    
    @classmethod
    def metadata_keys(cls) -> List[str]:
        """
        Gets the list of metadata keys relevant for the class.

        This method provides a list of keys used to access specific metadata
        pertaining to the class. It ensures a consistent mechanism to reference
        metadata keys.

        Returns:
            List[str]: A list of metadata keys as strings.
        """
        return [TOPICS_KEY]
    
    def build_nodes(self, nodes:List[BaseNode], **kwargs):
        """
        Constructs and returns a list of processed chunk nodes by iterating through
        a list of input nodes, applying transformations, and filtering metadata.

        Args:
            nodes (List[BaseNode]): A list of input nodes that need to be processed
                and transformed into chunk nodes.

        Returns:
            List[BaseNode]: A list of processed chunk nodes with updated metadata,
            relationships, and excluded keys for embedding and LLM usage.
        """
        chunk_nodes = []

        build_timestamp = self._get_build_timestamp(**kwargs)

        for node in nodes:

            chunk_id = node.node_id
            chunk_node = node.model_copy()
            chunk_node.text_template = DEFAULT_TEXT_NODE_TMPL
            
            topics = [
                topic['value'] 
                for topic in node.metadata.get(TOPICS_KEY, {}).get('topics', []) 
                if not self.build_filters.ignore_topic(topic['value'])
            ]

            source_info = node.relationships[NodeRelationship.SOURCE]
            source_id = source_info.node_id

            metadata = {
                'source': {
                     'sourceId': source_id
                },
                'chunk': {
                    'chunkId': chunk_id,
                    'metadata': {}
                },
                'topics': topics 
            }  
                
            if source_info.metadata:

                metadata['source'].update(self._get_source_info_metadata(source_info.metadata))
                
                # Add external properties if configured
                external_props = GraphRAGConfig.chunk_external_properties
                if external_props and isinstance(external_props, dict):
                    valid_source_metadata = metadata['source']['metadata']
                    chunk_metadata = metadata['chunk']['metadata']
                    for prop_name, metadata_key in external_props.items():
                        if metadata_key in valid_source_metadata:
                            chunk_metadata[prop_name] = valid_source_metadata[metadata_key]


            metadata = self._update_metadata_with_versioning_info(metadata, node, build_timestamp)
            
            metadata[INDEX_KEY] = {
                'index': 'chunk',
                'key': self._clean_id(chunk_id)
            }

            chunk_node.metadata = metadata
            chunk_node.excluded_embed_metadata_keys = [INDEX_KEY, 'chunk', 'source']
            chunk_node.excluded_llm_metadata_keys = [INDEX_KEY, 'chunk', 'source']

            chunk_nodes.append(chunk_node)

        return chunk_nodes

    
