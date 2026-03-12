# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from llama_index.core.schema import Document, TextNode, NodeRelationship, RelatedNodeInfo
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.tenant_id import TenantId


class TestIdRewriterInitialization:
    """Tests for IdRewriter initialization."""
    
    def test_initialization_with_id_generator(self, default_id_gen):
        """Verify IdRewriter initializes with IdGenerator."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        
        assert rewriter.id_generator is default_id_gen
        assert rewriter.inner is None
    

class TestIdRewriterDocumentIdGeneration:
    """Tests for document ID generation."""
    
    def test_new_doc_id_generates_source_id(self, default_id_gen):
        """Verify _new_doc_id generates source ID for documents."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc = Document(text="Test document content", metadata={"key": "value"})
        
        new_id = rewriter._new_doc_id(doc)
        
        assert isinstance(new_id, str)
        assert len(new_id) > 0
    
    def test_new_doc_id_deterministic(self, default_id_gen):
        """Verify _new_doc_id is deterministic for same content."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc1 = Document(text="Same content", metadata={"key": "value"})
        doc2 = Document(text="Same content", metadata={"key": "value"})
        
        id1 = rewriter._new_doc_id(doc1)
        id2 = rewriter._new_doc_id(doc2)
        
        assert id1 == id2
    
    def test_new_doc_id_different_for_different_content(self, default_id_gen):
        """Verify _new_doc_id generates different IDs for different content."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc1 = Document(text="Content A", metadata={})
        doc2 = Document(text="Content B", metadata={})
        
        id1 = rewriter._new_doc_id(doc1)
        id2 = rewriter._new_doc_id(doc2)
        
        assert id1 != id2
    
    def test_new_doc_id_with_empty_metadata(self, default_id_gen):
        """Verify _new_doc_id handles empty metadata."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc = Document(text="Test content", metadata={})
        
        new_id = rewriter._new_doc_id(doc)
        
        assert isinstance(new_id, str)
        assert len(new_id) > 0


class TestIdRewriterNodeIdGeneration:
    """Tests for node ID generation."""
    
    def test_new_node_id_generates_chunk_id(self, default_id_gen):
        """Verify _new_node_id generates chunk ID for nodes."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        node = TextNode(text="Chunk content", metadata={})
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source123")
        
        new_id = rewriter._new_node_id(node)
        
        assert isinstance(new_id, str)
        assert len(new_id) > 0
    
    def test_new_node_id_without_source(self, default_id_gen):
        """Verify _new_node_id handles nodes without source relationship."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        node = TextNode(text="Chunk content", metadata={})
        
        new_id = rewriter._new_node_id(node)
        
        assert isinstance(new_id, str)
        assert len(new_id) > 0
        # Should generate a UUID-based source ID
        assert 'aws:' in new_id or len(new_id) > 0
    
    def test_new_node_id_deterministic_with_source(self, default_id_gen):
        """Verify _new_node_id is deterministic for same content and source."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        
        node1 = TextNode(text="Same chunk", metadata={})
        node1.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source123")
        
        node2 = TextNode(text="Same chunk", metadata={})
        node2.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="source123")
        
        id1 = rewriter._new_node_id(node1)
        id2 = rewriter._new_node_id(node2)
        
        assert id1 == id2


class TestIdRewriterNewId:
    """Tests for _new_id method."""
    
    def test_new_id_preserves_aws_prefix(self, default_id_gen):
        """Verify _new_id preserves IDs starting with 'aws:'."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        node = TextNode(text="Test", id_="aws:existing-id-123")
        
        new_id = rewriter._new_id(node)
        
        assert new_id == "aws:existing-id-123"
    

    def test_new_id_for_text_node(self, default_id_gen):
        """Verify _new_id generates node ID for TextNode instances."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        node = TextNode(text="Node content", metadata={})
        
        new_id = rewriter._new_id(node)
        
        assert isinstance(new_id, str)


class TestIdRewriterParseNodes:
    """Tests for _parse_nodes method."""
    
    def test_parse_nodes_rewrites_ids(self, default_id_gen):
        """Verify _parse_nodes rewrites node IDs."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        nodes = [
            TextNode(text="Node 1", id_="old-id-1"),
            TextNode(text="Node 2", id_="old-id-2")
        ]
        
        result = rewriter._parse_nodes(nodes)
        
        assert len(result) == 2
        assert result[0].id_ != "old-id-1"
        assert result[1].id_ != "old-id-2"
    
    def test_parse_nodes_without_inner_parser(self, default_id_gen):
        """Verify _parse_nodes works without inner parser."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        nodes = [TextNode(text="Test", id_="old-id")]
        
        result = rewriter._parse_nodes(nodes)
        
        assert len(result) == 1
        assert result[0].id_ != "old-id"
    


class TestIdRewriterHandleSourceDocs:
    """Tests for handle_source_docs method."""
    
    def test_handle_source_docs_rewrites_refnode_id(self, default_id_gen):
        """Verify handle_source_docs rewrites refNode ID."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc = SourceDocument(
            refNode=Document(text="Source doc", id_="old-ref-id")
        )
        
        result = rewriter.handle_source_docs([doc])
        
        assert len(result) == 1
        assert result[0].refNode.id_ != "old-ref-id"
    
    def test_handle_source_docs_rewrites_node_ids(self, default_id_gen):
        """Verify handle_source_docs rewrites node IDs."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc = SourceDocument(
            refNode=Document(text="Source"),
            nodes=[
                TextNode(text="Chunk 1", id_="old-node-1"),
                TextNode(text="Chunk 2", id_="old-node-2")
            ]
        )
        
        result = rewriter.handle_source_docs([doc])
        
        assert len(result) == 1
        assert len(result[0].nodes) == 2
        assert result[0].nodes[0].id_ != "old-node-1"
        assert result[0].nodes[1].id_ != "old-node-2"
    
    def test_handle_source_docs_without_refnode(self, default_id_gen):
        """Verify handle_source_docs handles documents without refNode."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        doc = SourceDocument(
            refNode=None,
            nodes=[TextNode(text="Chunk", id_="old-id")]
        )
        
        result = rewriter.handle_source_docs([doc])
        
        assert len(result) == 1
        assert result[0].nodes[0].id_ != "old-id"
    
    def test_handle_source_docs_multiple_documents(self, default_id_gen):
        """Verify handle_source_docs handles multiple documents."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        docs = [
            SourceDocument(
                refNode=Document(text=f"Doc {i}", id_=f"old-{i}"),
                nodes=[TextNode(text=f"Chunk {i}", id_=f"old-chunk-{i}")]
            )
            for i in range(3)
        ]
        
        result = rewriter.handle_source_docs(docs)
        
        assert len(result) == 3
        for i, doc in enumerate(result):
            assert doc.refNode.id_ != f"old-{i}"
            assert doc.nodes[0].id_ != f"old-chunk-{i}"


class TestIdRewriterIntegration:
    """Integration tests for IdRewriter."""
    

    def test_id_rewriter_preserves_content(self, default_id_gen):
        """Verify IdRewriter preserves node content while rewriting IDs."""
        rewriter = IdRewriter(id_generator=default_id_gen)
        
        original_text = "Important content that must be preserved"
        original_metadata = {"key": "value", "number": 42}
        
        node = TextNode(
            text=original_text,
            metadata=original_metadata.copy(),
            id_="old-id"
        )
        
        result = rewriter._parse_nodes([node])
        
        assert len(result) == 1
        assert result[0].text == original_text
        assert result[0].metadata["key"] == "value"
        assert result[0].metadata["number"] == 42
        assert result[0].id_ != "old-id"
