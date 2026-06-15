# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any

from graphrag_toolkit.lexical_graph.indexing.model import Topic
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
from graphrag_toolkit.core.compat import BaseNode


logger = logging.getLogger(__name__)

class TopicGraphBuilder(GraphBuilder):
    """
    Handles the construction of a topic graph within a graph-based data store.

    This class is a specialized implementation of `GraphBuilder` that focuses on
    building and inserting topic-related information into a graph database. It
    validates the topic metadata, constructs the necessary graph relationships
    and nodes, and executes the insertion logic using the provided graph client.

    Attributes:
        node_id (Callable): A callable function from the `GraphStore` class used
            to generate a unique identifier for nodes in the graph.

        execute_query_with_retry (Callable): A callable function from the
            `GraphStore` class used to execute queries with retry mechanisms.
    """
    @classmethod
    def index_key(cls) -> str:
        """
        Returns the key used for indexing objects of this class.

        This method is a class-level method that provides a consistent indexing
        key for all objects belonging to this class. It ensures uniformity when
        storing or retrieving class instances using a common key identifier.

        Returns:
            str: A string value representing the index key for the class.
        """
        return 'topic'
    
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        Builds a topic node and its relationships in the graph database.

        This method takes a BaseNode object, processes its metadata to extract topic
        information, and creates or updates nodes and relationships in the graph
        database using the provided graph client. If the metadata contains topic data,
        it validates it, constructs the necessary query, and executes it in the graph
        store. If no topic data exists in the metadata, a warning is logged.

        Args:
            node: A BaseNode instance containing metadata about the topic.
            graph_client: A GraphStore instance used to execute queries against the
                graph database.
            **kwargs: Additional arguments for customization or further processing.
        """
        topic_metadata = node.metadata.get('topic', {})

        if topic_metadata:

            topic = Topic.model_validate(topic_metadata)
        
            logger.debug(f'Inserting topic [topic_id: {topic.topicId}]')

            statements = [
                '// insert topics',
                'UNWIND $params AS params',
                f'MERGE (topic:`__Topic__`{{{graph_client.node_id("topicId")}: params.topic_id}})',
                'ON CREATE SET topic.value=params.title',
                'ON MATCH SET topic.value=params.title',
                'WITH topic, params',
                'UNWIND params.chunk_ids as chunkIds',
                f'MERGE (chunk:`__Chunk__`{{{graph_client.node_id("chunkId")}: chunkIds.chunk_id}})',
                'MERGE (topic)-[:`__MENTIONED_IN__`]->(chunk)'
            ]

            chunk_ids =  [ {'chunk_id': chunkId} for chunkId in topic.chunkIds]

            properties = {
                'topic_id': topic.topicId,
                'title': topic.value,
                'chunk_ids': chunk_ids
            }

            query = '\n'.join(statements)

            graph_client.execute_query_with_retry(query, self._to_params(properties))

        else:
            logger.warning(f'topic_id missing from topic node [node_id: {node.node_id}]') 
