# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from llama_index.core.schema import Document, TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline import (
    PassThroughDecorator,
    ExtractionPipeline
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


class TestPassThroughDecoratorInitialization:
    """Tests for PassThroughDecorator initialization."""
    
    def test_initialization(self):
        """Verify PassThroughDecorator initializes correctly."""
        decorator = PassThroughDecorator()
        assert decorator is not None


class TestPassThroughDecoratorHandleInputDocs:
    """Tests for handle_input_docs method."""
    
    def test_handle_input_docs_returns_iterable(self):
        """Verify handle_input_docs returns iterable."""
        decorator = PassThroughDecorator()
        docs = [SourceDocument(refNode=Document(text="test"))]
        
        result = decorator.handle_input_docs(docs)
        
        assert hasattr(result, '__iter__')
    
    def test_handle_input_docs_preserves_documents(self):
        """Verify handle_input_docs preserves documents."""
        decorator = PassThroughDecorator()
        docs = [
            SourceDocument(refNode=Document(text="doc1")),
            SourceDocument(refNode=Document(text="doc2"))
        ]
        
        result = list(decorator.handle_input_docs(docs))
        
        assert len(result) == 2
        assert result[0].refNode.text == "doc1"
        assert result[1].refNode.text == "doc2"
    
    def test_handle_input_docs_with_empty_list(self):
        """Verify handle_input_docs handles empty list."""
        decorator = PassThroughDecorator()
        
        result = list(decorator.handle_input_docs([]))
        
        assert result == []


class TestPassThroughDecoratorHandleOutputDoc:
    """Tests for handle_output_doc method."""
    
    def test_handle_output_doc_returns_document(self):
        """Verify handle_output_doc returns document."""
        decorator = PassThroughDecorator()
        doc = SourceDocument(refNode=Document(text="test"))
        
        result = decorator.handle_output_doc(doc)
        
        assert isinstance(result, SourceDocument)
    
    def test_handle_output_doc_preserves_document(self):
        """Verify handle_output_doc preserves document."""
        decorator = PassThroughDecorator()
        doc = SourceDocument(
            refNode=Document(text="original"),
            nodes=[TextNode(text="chunk")]
        )
        
        result = decorator.handle_output_doc(doc)
        
        assert result.refNode.text == "original"
        assert len(result.nodes) == 1
        assert result.nodes[0].text == "chunk"


class TestExtractionPipelineInitialization:
    """Tests for ExtractionPipeline initialization."""
    
    def test_initialization_with_empty_components(self):
        """Verify ExtractionPipeline initializes with empty components."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        assert pipeline is not None
    
    def test_initialization_with_decorator(self):
        """Verify ExtractionPipeline initializes with decorator."""
        decorator = PassThroughDecorator()
        pipeline = ExtractionPipeline(
            components=[],
            decorator=decorator
        )
        
        assert pipeline is not None


class TestExtractionPipelineCreate:
    """Tests for ExtractionPipeline.create factory method."""
    
    def test_create_with_empty_components(self):
        """Verify create works with empty components."""
        pipeline = ExtractionPipeline.create(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        assert pipeline is not None
    

class TestExtractionPipelineExtract:
    """Tests for extract method."""
    
    def test_extract_with_documents(self):
        """Verify extract processes documents."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        docs = [Document(text="test document")]
        result = list(pipeline.extract(docs))
        
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_extract_with_empty_input(self):
        """Verify extract handles empty input."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        result = list(pipeline.extract([]))
        
        assert isinstance(result, list)
        assert len(result) == 0
    

class TestExtractionPipelineSourceDocumentsConversion:
    """Tests for _source_documents_from_base_nodes method."""
    


class TestExtractionPipelineIntegration:
    """Integration tests for ExtractionPipeline."""
    
    def test_full_pipeline_flow(self):
        """Verify complete pipeline flow."""
        decorator = PassThroughDecorator()
        pipeline = ExtractionPipeline(
            components=[],
            decorator=decorator
        )
        
        docs = [Document(text="Test document for pipeline")]
        result = list(pipeline.extract(docs))
        
        assert len(result) > 0
        assert all(isinstance(sd, SourceDocument) for sd in result)
    
    def test_pipeline_with_multiple_documents(self):
        """Verify pipeline handles multiple documents."""
        pipeline = ExtractionPipeline(
            components=[],
            decorator=PassThroughDecorator()
        )
        
        docs = [
            Document(text=f"Document {i}")
            for i in range(5)
        ]
        
        result = list(pipeline.extract(docs))
        
        assert len(result) >= 5
