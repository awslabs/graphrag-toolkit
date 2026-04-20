# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Any, Optional
from graphrag_toolkit.lexical_graph.metadata import SourceMetadataFormatter, DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.source_node_builder import SourceNodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.chunk_node_builder import ChunkNodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.topic_node_builder import TopicNodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.statement_node_builder import StatementNodeBuilder

from llama_index.core.schema import BaseNode, NodeRelationship

logger = logging.getLogger(__name__)

class NodeBuilders():
    """
    Manages the creation, processing, and filtering of nodes using specified builders
    and configuration options.

    NodeBuilders is a class designed to build nodes derived from input metadata, using
    a series of configurable `NodeBuilder` instances and processing operations. The
    class also handles input node cleaning, ID rewriting for specific tenants, and
    the filtering of input metadata.

    Attributes:
        build_filters (BuildFilters): An instance of BuildFilters for filtering
            input metadata.
        id_generator (IdGenerator): An instance of IdGenerator for managing and
            rewriting node IDs.
        source_metadata_formatter: An instance of SourceMetadataFormatter responsible 
            for processing source metadata.
        builders (List[NodeBuilder]): A list of `NodeBuilder` instances used to
            generate derived nodes. Defaults to a set of default builders if not
            provided.
    """

    def __init__(
            self, 
            builders:Optional[List[NodeBuilder]]=None, 
            build_filters:Optional[BuildFilters]=None, 
            source_metadata_formatter:Optional[SourceMetadataFormatter]=None,
            id_generator:Optional[IdGenerator]=None
        ):
        
        """
        Initializes the class with the provided builders, build filters, source metadata
        formatter, and id generator. If not explicitly provided, default instances are
        created for build filters, source metadata formatter, and id generator. This
        ensures the necessary components are properly initialized and serves as the
        entry-point for managing builders and their dependencies.

        Args:
            builders (List[NodeBuilder], optional): A list of NodeBuilder instances
                responsible for constructing nodes. If not provided, default builders
                are initialized using the id generator, build filters, and source
                metadata formatter.
            build_filters (BuildFilters, optional): Instance of BuildFilters used to
                determine which nodes should be built. Defaults to a new instance of
                BuildFilters if not provided.
            source_metadata_formatter (Optional[SourceMetadataFormatter], optional):
                Formatter responsible for processing source metadata. Defaults to
                DefaultSourceMetadataFormatter if not provided.
            id_generator (IdGenerator, optional): Instance of IdGenerator used to
                generate unique identifiers. Defaults to a new IdGenerator instance if
                not provided.
        """

        id_generator = id_generator or IdGenerator()
        build_filters = build_filters or BuildFilters()
        source_metadata_formatter = source_metadata_formatter or DefaultSourceMetadataFormatter()

        self.build_filters = build_filters
        self.id_generator = id_generator
        self.builders = builders if builders is not None else self.default_builders(id_generator, build_filters, source_metadata_formatter)

        logger.debug(f'Node builders: {[type(b).__name__ for b in self.builders]}')
    
    def default_builders(self, id_generator:IdGenerator, build_filters:BuildFilters, source_metadata_formatter:SourceMetadataFormatter):
        """
        Builds and returns a list of default node builders using the provided IdGenerator.

        This method instantiates and provides a collection of node builders, where each
        builder is responsible for creating a specific type of node within the system.
        The `id_generator` is utilized to assign unique identifiers to the nodes created
        by these builders.

        Args:
            id_generator (IdGenerator): Instance of IdGenerator used to generate unique
                identifiers for nodes.

        Returns:
            list: A list of node builder instances including SourceNodeBuilder,
                ChunkNodeBuilder, TopicNodeBuilder, and StatementNodeBuilder. Each
                builder is initialized with the provided `id_generator`.
        """
        return [
            node_builder(
                id_generator=id_generator, 
                build_filters=build_filters, 
                source_metadata_formatter=source_metadata_formatter
            )
            for node_builder in [SourceNodeBuilder, ChunkNodeBuilder, TopicNodeBuilder, StatementNodeBuilder]
        ]
        
    @classmethod
    def class_name(cls) -> str:
        return 'NodeBuilders'
    
    def get_nodes_from_metadata(self, input_nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:
        """
        Processes input nodes by applying tenant id rewrites and cleaning text, then uses the builders to generate
        new nodes based on metadata and appends the original nodes to the results.

        Args:
            input_nodes (List[BaseNode]): A list of input nodes to be processed and used for generating new nodes.
            **kwargs (Any): Additional keyword arguments that may be required by the builders.

        Returns:
            List[BaseNode]: A list of processed nodes that includes both generated nodes and the original input nodes.

        Raises:
            Exception: If an error occurs during the node-building process by any builder.
        """

        if len(self.builders) == 0:
            return input_nodes
        
        def apply_tenant_id_rewrites(node):
            
            node.id_ =  self.id_generator.rewrite_id_for_tenant(node.id_)

            for _, node_info in node.relationships.items():
                if isinstance(node_info, list):
                    for n in node_info:
                        n.node_id = self.id_generator.rewrite_id_for_tenant(n.node_id)    
                else:
                    node_info.node_id = self.id_generator.rewrite_id_for_tenant(node_info.node_id)
           
            return node
        
        def clean_text(node):
            node.text = node.text.replace('\x00', '')
            return node
        
        def pre_process(node):
            node = clean_text(node)
            node = apply_tenant_id_rewrites(node)
            return node

        results = []

        filtered_nodes = [
            node 
            for node in input_nodes 
            if self.build_filters.filter_source_metadata_dictionary(node.relationships[NodeRelationship.SOURCE].metadata) 
        ]

        pre_processed_nodes = [
            pre_process(node) 
            for node in filtered_nodes
        ]

        for builder in self.builders:
            try:
                
                builder_specific_nodes = [
                    node
                    for node in pre_processed_nodes 
                    if any(key in builder.metadata_keys() for key in node.metadata)
                ]
                
                results.extend(builder.build_nodes(builder_specific_nodes, **kwargs))
            except Exception as e:
                    logger.exception('An error occurred while building nodes from chunks')
                    raise e
            
        results.extend(input_nodes) # Always add the original nodes after derived nodes    

        logger.debug(f'Accepted {len(input_nodes)} chunks, emitting {len(results)} nodes')

        return results
        
    def __call__(self, nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:    
        return self.get_nodes_from_metadata(nodes, **kwargs)
                    