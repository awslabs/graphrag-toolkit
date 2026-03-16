# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.model import (
    SourceDocument,
    source_documents_from_source_types,
    Propositions,
    Entity,
    Relation,
    Fact,
    Statement,
    Topic,
    TopicCollection
)
from llama_index.core.schema import TextNode, Document, BaseNode, NodeRelationship, RelatedNodeInfo


class TestSourceDocument:
    """Tests for SourceDocument model."""
    
    def test_source_id_with_nodes(self):
        """Test source_id returns the source node ID from first node."""
        doc = Document(text="test", id_="doc1")
        node = TextNode(text="test", id_="node1")
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source123")
        
        source_doc = SourceDocument(nodes=[node])
        
        assert source_doc.source_id() == "source123"
    
    def test_source_id_with_empty_nodes(self):
        """Test source_id returns None when nodes list is empty."""
        source_doc = SourceDocument(nodes=[])
        
        assert source_doc.source_id() is None


class TestSourceDocumentsFromSourceTypes:
    """Tests for source_documents_from_source_types function."""
    
    def test_with_source_document_input(self):
        """Test that SourceDocument inputs are yielded as-is."""
        source_doc = SourceDocument(nodes=[])
        
        result = list(source_documents_from_source_types([source_doc]))
        
        assert len(result) == 1
        assert result[0] == source_doc
    
    def test_with_document_input(self):
        """Test that Document inputs are wrapped in SourceDocument."""
        doc = Document(text="test", id_="doc1")
        
        result = list(source_documents_from_source_types([doc]))
        
        assert len(result) == 1
        assert isinstance(result[0], SourceDocument)
        assert result[0].nodes == [doc]
    
    def test_with_text_node_single_source(self):
        """Test that TextNodes with same source are grouped together."""
        node1 = TextNode(text="test1", id_="node1")
        node1.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source1")
        
        node2 = TextNode(text="test2", id_="node2")
        node2.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source1")
        
        result = list(source_documents_from_source_types([node1, node2]))
        
        assert len(result) == 1
        assert len(result[0].nodes) == 2
        assert result[0].nodes[0] == node1
        assert result[0].nodes[1] == node2
    
    def test_with_text_node_multiple_sources(self):
        """Test that TextNodes with different sources create separate SourceDocuments."""
        node1 = TextNode(text="test1", id_="node1")
        node1.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source1")
        
        node2 = TextNode(text="test2", id_="node2")
        node2.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source2")
        
        result = list(source_documents_from_source_types([node1, node2]))
        
        assert len(result) == 2
        assert len(result[0].nodes) == 1
        assert len(result[1].nodes) == 1
        assert result[0].nodes[0] == node1
        assert result[1].nodes[0] == node2
    
    def test_with_mixed_input_types(self):
        """Test with mixed SourceDocument, Document, and TextNode inputs."""
        source_doc = SourceDocument(nodes=[])
        doc = Document(text="test", id_="doc1")
        
        node = TextNode(text="test", id_="node1")
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source1")
        
        result = list(source_documents_from_source_types([source_doc, doc, node]))
        
        assert len(result) == 3
        assert result[0] == source_doc
        assert result[1].nodes == [doc]
        assert result[2].nodes[0] == node
    
    def test_with_invalid_input_type_raises_error(self):
        """Test that invalid input type raises ValueError."""
        invalid_input = "not a valid type"
        
        with pytest.raises(ValueError) as exc_info:
            list(source_documents_from_source_types([invalid_input]))
        
        assert "Unexpected source type" in str(exc_info.value)


class TestDataModels:
    """Tests for data model instantiation."""
    
    def test_propositions_model(self):
        """Test Propositions model creation."""
        props = Propositions(propositions=["prop1", "prop2"])
        assert len(props.propositions) == 2
        assert props.propositions[0] == "prop1"
    
    def test_entity_model(self):
        """Test Entity model creation."""
        entity = Entity(value="test entity", classification="Person")
        assert entity.value == "test entity"
        assert entity.classification == "Person"
        assert entity.entityId is None
    
    def test_relation_model(self):
        """Test Relation model creation."""
        relation = Relation(value="works_at")
        assert relation.value == "works_at"
    
    def test_fact_model(self):
        """Test Fact model creation."""
        subject = Entity(value="John", classification="Person")
        predicate = Relation(value="works_at")
        obj = Entity(value="Company", classification="Organization")
        
        fact = Fact(subject=subject, predicate=predicate, object=obj)
        
        assert fact.subject.value == "John"
        assert fact.predicate.value == "works_at"
        assert fact.object.value == "Company"
    
    def test_statement_model(self):
        """Test Statement model creation."""
        statement = Statement(value="test statement")
        assert statement.value == "test statement"
        assert statement.details == []
        assert statement.facts == []
    
    def test_topic_model(self):
        """Test Topic model creation."""
        topic = Topic(value="test topic")
        assert topic.value == "test topic"
        assert topic.entities == []
        assert topic.statements == []
    
    def test_topic_collection_model(self):
        """Test TopicCollection model creation."""
        topic1 = Topic(value="topic1")
        topic2 = Topic(value="topic2")
        
        collection = TopicCollection(topics=[topic1, topic2])
        
        assert len(collection.topics) == 2
        assert collection.topics[0].value == "topic1"
