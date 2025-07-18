# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Dict

from llama_index.core.schema import TextNode, BaseNode
from llama_index.core.schema import NodeRelationship

from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection, Topic, Statement
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

class TopicNodeBuilder(NodeBuilder):
    """
    Builds Topic-related nodes by processing existing nodes and their metadata.

    This class inherits from NodeBuilder and provides functionality to create
    specialized topic-related nodes by extracting and validating topic metadata,
    aggregating associated statements, and managing relationships between source
    and topic nodes. It ensures clean and structured data for topic-based
    processing and indexing.

    Attributes:
        name (str): Class-level method that returns the name of the builder as
            'TopicNodeBuilder'.
        metadata_keys (List[str]): Class-level method that returns a list of metadata
            keys specific to this builder. Contains the key for identifying topics
            in metadata.
    """
    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the class.

        This method provides the name of the class as a string, which can
        be useful for identification or debugging purposes.

        Returns:
            str: The name of the class.
        """
        return 'TopicNodeBuilder'
    
    @classmethod
    def metadata_keys(cls) -> List[str]:
        """
        Returns a list of predefined metadata keys specific to the class.

        This class method provides a way to retrieve a static list of metadata
        keys that are applicable across all instances of the class.

        Returns:
            List[str]: A list containing metadata key names associated with the class.
        """
        return [TOPICS_KEY]
    
    def _add_chunk_id(self, node:TextNode, chunk_id:str):
        """
        Updates the given node's metadata to include a new chunk ID. This method retrieves
        the topic associated with the node, adds the given chunk ID to the list of
        chunk IDs (ensuring no duplicates), and updates the node's metadata with the
        modified topic information.

        Args:
            node: The TextNode instance whose metadata will be updated with the new
                chunk ID.
            chunk_id: The identifier for the chunk that needs to be added to the
                topic's chunk IDs.

        Returns:
            node: The updated TextNode instance with the modified metadata containing
                the new chunk ID.
        """
        topic = Topic.model_validate(node.metadata['topic'])

        existing_chunk_ids = dict.fromkeys(topic.chunkIds)
        existing_chunk_ids[chunk_id] = None

        topic.chunkIds = list(existing_chunk_ids.keys())

        node.metadata['topic'] = topic.model_dump(exclude_none=True)
        
        return node
    
    def _add_statements(self, node:TextNode, statements:List[Statement]):
        """
        Adds a list of statements to the metadata of a given TextNode. Existing statements
        in the node metadata are preserved, and new statements are added if they are not
        configured to be ignored by the build filters.

        Args:
            node (TextNode): The node whose metadata will be updated with the provided
                statements.
            statements (List[Statement]): A list of Statement objects to be added to the
                node's metadata.

        Returns:
            TextNode: The updated node with its metadata containing the new and existing
                statements.
        """
        existing_statements = dict.fromkeys(node.metadata['statements'])
                
        for statement in statements:
            if self.build_filters.ignore_statement(statement.value):
                continue
            existing_statements[statement.value] = None
            
        node.metadata['statements'] = list(existing_statements.keys())

        return node


    def build_nodes(self, nodes:List[BaseNode]):
        """
        Builds a list of topic nodes derived from the provided nodes. This function maps
        chunks and their contained relationships, topics, and statements into structured nodes.
        It ensures that unique topic identifiers are created for each source, and it manages
        the connections between source IDs and chunk IDs. The resulting nodes are prepared
        for further indexing operations.

        Args:
            nodes (List[BaseNode]): A list of `BaseNode` objects from which the topic
            nodes will be constructed. Each node is expected to have metadata and
            relationships that include topic and source information.

        Returns:
            List[TextNode]: A list of `TextNode` objects representing unique topics and their
            associated metadata, statements, and relationships.
        """
        topic_nodes:Dict[str, TextNode] = {}

        for node in nodes:

            chunk_id = node.node_id
            
            data = node.metadata.get(TOPICS_KEY, [])
            
            if not data:
                continue

            topics = TopicCollection.model_validate(data)

            source_info = node.relationships[NodeRelationship.SOURCE]
            source_id = source_info.node_id

            for topic in topics.topics:

                if self.build_filters.ignore_topic(topic.value):
                    continue
                
                topic_id =  self.id_generator.create_node_id('topic', source_id, topic.value) # topic identity defined by source, not chunk, so that we can connect same topic to multiple chunks in scope of single source

                if topic_id not in topic_nodes:
                    
                    metadata = {
                        'source': {
                            'sourceId': source_id
                        },
                        'topic': Topic(topicId=topic_id, value=topic.value).model_dump(exclude_none=True),
                        'statements': [],
                        INDEX_KEY: {
                            'index': 'topic',
                            'key': self._clean_id(topic_id)
                        }
                    }

                    if source_info.metadata:
                        metadata['source']['metadata'] = source_info.metadata

                    topic_node = TextNode(
                        id_ = topic_id,
                        text = topic.value,
                        metadata = metadata,
                        excluded_embed_metadata_keys = [INDEX_KEY, 'topic'],
                        excluded_llm_metadata_keys = [INDEX_KEY, 'topic']
                    )

                    topic_nodes[topic_id] = topic_node

                topic_node = topic_nodes[topic_id]
                
                topic_node = self._add_chunk_id(topic_node, chunk_id)
                topic_node = self._add_statements(topic_node, topic.statements)
            
                topic_nodes[topic_id] = topic_node

        return list(topic_nodes.values())
