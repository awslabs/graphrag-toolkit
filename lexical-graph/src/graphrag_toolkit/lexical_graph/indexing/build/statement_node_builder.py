# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from llama_index.core.schema import TextNode, BaseNode
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo

from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY, LOCAL_ENTITY_CLASSIFICATION
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import string_complement_to_entity

class StatementNodeBuilder(NodeBuilder):
    """
    Represents a specialized node builder for constructing statement and fact nodes
    from input nodes. This class is designed for processing structured data, extracting
    topics, statements, and facts, and creating associated metadata-enriched nodes.

    The primary use of this class is to generate nodes, with appropriate relationships
    and metadata, from a given list of base nodes. This is done by parsing topics and
    their associated statements and facts, applying filtering criteria, and using
    unique identifiers for linking nodes.

    The output nodes may be used in larger systems where structured topics, statements,
    and facts need to be indexed or related to other system components.

    Attributes:
        None
    """
    @classmethod
    def name(cls) -> str:
        """
        Provides a way to retrieve the name of the class. This method allows obtaining the
        specific identifier for the class when needed, especially in contexts requiring
        dynamic class identification.

        Returns:
            str: The name of the class, "StatementNodeBuilder".
        """
        return 'StatementNodeBuilder'
    
    @classmethod
    def metadata_keys(cls) -> List[str]:
        """
        Retrieves metadata keys specifically associated with the class. This method provides
        a list of standardized metadata keys that are utilized within the class context. The
        keys returned are constants predefined within the class, ensuring consistency and
        avoiding hardcoding.

        Returns:
            List[str]: A list of strings representing metadata keys associated
            with the class's operations or context.
        """
        return [TOPICS_KEY]
    
    def build_nodes(self, nodes:List[BaseNode]):
        """
        Builds and processes nodes from the provided list of BaseNode objects. This method
        constructs 'statement' and 'fact' nodes with associated metadata and relationships,
        based on the topics, statements, and facts provided in the nodes. The constructed
        nodes are further categorized, filtered, and structured to maintain logical
        connections between related elements such as topics, statements, and facts.

        Conditions:
        - Topics without data in metadata are skipped.
        - Statements or topics ignored by filters are excluded from node creation.
        - Facts are indexed and associated with their parent statements, ensuring
          deduplication when applicable.

        Args:
            nodes (List[BaseNode]): A list of BaseNode instances containing metadata,
                relationships, topics, statements, and facts to parse and process.

        Returns:
            List[TextNode]: A list of TextNode objects representing 'statement' and 'fact'
            nodes, properly structured, indexed, and enriched with metadata and relationships.
        """
        statement_nodes = {}
        fact_nodes = {}

        for node in nodes:

            chunk_id = node.node_id

            data = node.metadata.get(TOPICS_KEY, [])
            
            if not data:
                continue

            topics = TopicCollection.model_validate(data)

            source_info = node.relationships[NodeRelationship.SOURCE]
            source_id = source_info.node_id

            source_metadata = {
                'sourceId': source_id
            }

            if source_info.metadata:
                source_metadata['metadata'] = source_info.metadata

            for topic in topics.topics:

                if self.build_filters.ignore_topic(topic.value):
                    continue

                topic_id = self.id_generator.create_node_id('topic', source_id, topic.value) # topic identity defined by source, not chunk, so that we can connect same topic to multiple chunks in scope of single source

                prev_statement = None
                
                for statement in topic.statements:

                    if self.build_filters.ignore_statement(statement.value):
                        continue

                    statement_id = self.id_generator.create_node_id('statement', topic_id, statement.value)
     
                    if statement_id not in statement_nodes:

                        statement.statementId = statement_id
                        statement.topicId = topic_id    
                        statement.chunkId = chunk_id
                        
                        statement_metadata = {
                            'source': source_metadata,
                            'statement': statement.model_dump(),
                            INDEX_KEY: {
                                'index': 'statement',
                                'key': self._clean_id(statement_id)
                            }
                        }

                        statement_details = '\n'.join(statement.details)

                        statement_node = TextNode(
                            id_ = statement_id,
                            text = f'{statement.value}\n\n{statement_details}' if statement_details else statement.value,
                            metadata = statement_metadata,
                            excluded_embed_metadata_keys = [INDEX_KEY, 'statement'],
                            excluded_llm_metadata_keys = [INDEX_KEY, 'statement']
                        )

                        if prev_statement:
                            statement_node.relationships[NodeRelationship.PREVIOUS] = RelatedNodeInfo(
                                node_id=prev_statement.statementId,
                                metadata={
                                    'statement': prev_statement.model_dump()
                                }
                            ) 

                        prev_statement = statement

                        statement_nodes[statement_id] = statement_node
            
                    for fact in statement.facts:

                        fact = string_complement_to_entity(fact)

                        fact_value = self._format_fact(
                            fact.subject.value,
                            fact.subject.classification,
                            fact.predicate.value,
                            fact.object.value if fact.object else fact.complement.value,
                            fact.object.classification if fact.object else None
                        )
                        
                        fact_id = self.id_generator.create_node_id('fact', fact_value)

                        lookup_id = f'{statement_id}-{fact_id}'

                        if lookup_id not in fact_nodes:

                            fact.factId = fact_id
                            fact.statementId = statement_id

                            if fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION:
                                fact.subject.entityId = self.id_generator.create_node_id('local-entity', fact.subject.value, source_id)
                            else:
                                fact.subject.entityId = self.id_generator.create_node_id('entity', fact.subject.value, fact.subject.classification)
                            
                            if fact.object:
                                fact.object.entityId = self.id_generator.create_node_id('entity', fact.object.value, fact.object.classification)
                            
                            if fact.complement:
                                fact.complement.entityId = self.id_generator.create_node_id('local-entity', fact.complement.value, source_id)
                            
                            fact_metadata = {
                                'fact': fact.model_dump(),
                                INDEX_KEY: {
                                    'index': 'fact',
                                    'key': self._clean_id(fact_id)
                                }
                            }

                            fact_node = TextNode( # don't specify id here - each fact node should be indexable because facts can be associated with multiple statements
                                text = fact_value,
                                metadata = fact_metadata,
                                excluded_embed_metadata_keys = [INDEX_KEY, 'fact'],
                                excluded_llm_metadata_keys = [INDEX_KEY, 'fact']
                            )

                            fact_nodes[lookup_id] = fact_node

        results = []

        results.extend(statement_nodes.values())
        results.extend(fact_nodes.values())
        
        return results
