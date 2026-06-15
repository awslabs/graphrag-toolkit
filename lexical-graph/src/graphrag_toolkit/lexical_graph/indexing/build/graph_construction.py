# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from tqdm import tqdm
from typing import Any, List, Union

from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import GraphBatchClient
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY 
from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.topic_graph_builder import TopicGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.statement_graph_builder import StatementGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.fact_graph_builder import FactGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import EntityGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.entity_relation_graph_builder import EntityRelationGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.graph_summary_builder import GraphSummaryBuilder
from graphrag_toolkit.lexical_graph.indexing.build.local_entity_rewrites_graph_builder import LocalEntityRewritesGraphBuilder

from pydantic import Field
from graphrag_toolkit.core.compat import BaseNode

logger = logging.getLogger(__name__)

def default_builders() -> List[GraphBuilder]:
    """
    Provides a list of default graph builders for constructing various types of graphs.

    This function initializes and returns a predefined collection of graph builder
    instances. Each builder is responsible for constructing a specific type of graph
    used in processing, analyzing, or summarizing graph data. The sequence and types
    of builders included in the returned list are predetermined based on the expected
    workflow of graph construction.

    Returns:
        List[GraphBuilder]: A list of instantiated graph builders in the specific
            order of SourceGraphBuilder, ChunkGraphBuilder, TopicGraphBuilder,
            StatementGraphBuilder, EntityGraphBuilder, EntityRelationGraphBuilder,
            FactGraphBuilder, GraphSummaryBuilder.
    """
    return [
        SourceGraphBuilder(),
        ChunkGraphBuilder(),
        TopicGraphBuilder(),
        StatementGraphBuilder(),
        EntityGraphBuilder(),
        EntityRelationGraphBuilder(),
        FactGraphBuilder(),
        LocalEntityRewritesGraphBuilder(),
        GraphSummaryBuilder()
    ]

GraphInfoType = Union[str, GraphStore]

class GraphConstruction(NodeHandler):
    """
    Provides functionality for constructing and building a graph using nodes and builders.

    The GraphConstruction class is a utility for managing graph construction using a set of
    builders and a graph client. It supports functionality to batch graph operations, process
    nodes, and utilize builders dynamically based on node metadata. This class facilitates
    graph creation and updating in a scalable and configurable manner.

    Attributes:
        graph_client (GraphStore): The graph client used to perform operations on the graph store.
        builders (List[GraphBuilder]): A collection of graph builders used to process nodes during
            graph construction.
    """
    @staticmethod
    def for_graph_store(graph_info:GraphInfoType=None, **kwargs):
        """
        Constructs a GraphConstruction instance either directly from a GraphStore instance
        or by creating a GraphStore instance using GraphStoreFactory if a non-GraphStore
        value is provided.

        Args:
            graph_info (GraphInfoType, optional): The initial graph information provided,
                which can either be a GraphStore instance or data that can be used to
                create a GraphStore instance.
            **kwargs: Additional parameters required for GraphConstruction or
                GraphStore creation.

        Returns:
            GraphConstruction: A new instance of the GraphConstruction class.
        """
        if isinstance(graph_info, GraphStore):
            return GraphConstruction(graph_client=graph_info, **kwargs)
        else:
            return GraphConstruction(graph_client=GraphStoreFactory.for_graph_store(graph_info, **kwargs), **kwargs)
    
    graph_client: GraphStore 
    builders:List[GraphBuilder] = Field(
        description='Graph builders',
        default_factory=default_builders
    )

    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        """
        Processes a list of nodes to construct a graph by utilizing builders with the
        specified configurations, such as batch writes. Nodes can be processed with or
        without progress visualization, and operations are applied in batches as per
        given configurations.

        Args:
            nodes: A list of BaseNode objects to be processed and incorporated into the
                graph. Each node is checked and processed via builders based on its
                metadata.
            **kwargs: Arbitrary keyword arguments for configuring the processing and
                graph construction. Supported keys include:
                - batch_writes_enabled (bool): Enables or disables batch write
                  operations.
                - batch_write_size (int): Configures the maximum size of each batch for
                  operations.

        Yields:
            BaseNode: Nodes that have been processed by the builders and subjected to
            batch operations, ready to be yielded back to the caller. Nodes that do not
            meet the criteria or are ignored are not yielded.

        Raises:
            Exception: Propagates any exception that occurs during the processing of a
                node with its assigned builders. Error details are logged before the
                exception is raised.
        """
        builders_dict = {}
        for b in self.builders:
            if b.index_key() not in builders_dict:
                builders_dict[b.index_key()] = []
            builders_dict[b.index_key()].append(b)

        batch_writes_enabled = kwargs.pop('batch_writes_enabled')
        batch_write_size = kwargs.pop('batch_write_size')
        
        logger.debug(f'Batch config: [batch_writes_enabled: {batch_writes_enabled}, batch_write_size: {batch_write_size}]')
        logger.debug(f'Graph construction kwargs: {kwargs}')

        with GraphBatchClient(self.graph_client, batch_writes_enabled=batch_writes_enabled, batch_write_size=batch_write_size) as batch_client:
        
            node_iterable = nodes if not self.show_progress else tqdm(nodes, desc=f'Building graph [batch_writes_enabled: {batch_writes_enabled}, batch_write_size: {batch_write_size}]')

            for node in node_iterable:

                node_id = node.node_id
                
                if [key for key in [INDEX_KEY] if key in node.metadata]:
                    
                    try:
                    
                        index = node.metadata[INDEX_KEY]['index']
                        builders = builders_dict.get(index, None)

                        if builders:
                            for builder in builders:
                                builder.build(node, batch_client, **kwargs)
                        else:
                            logger.debug(f'No builders for node [index: {index}]')

                    except Exception as e:
                        logger.exception('An error occurred while building the graph')
                        raise e
                        
                else:
                    logger.debug(f'Ignoring node [node_id: {node_id}]')
                    
                if batch_client.allow_yield(node):
                    yield node

            batch_nodes = batch_client.apply_batch_operations()
            for node in batch_nodes:
                yield node

        