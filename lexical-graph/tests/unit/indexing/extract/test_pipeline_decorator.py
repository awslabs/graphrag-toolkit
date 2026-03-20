# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from typing import Iterable
from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_decorator import PipelineDecorator
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from llama_index.core.schema import Document, TextNode


class ConcretePipelineDecorator(PipelineDecorator):
    """Concrete implementation of PipelineDecorator for testing."""
    
    def __init__(self, transform_input=False, transform_output=False):
        self.transform_input = transform_input
        self.transform_output = transform_output
        self.input_docs_called = False
        self.output_doc_called = False
    
    def handle_input_docs(self, docs: Iterable[SourceDocument]) -> Iterable[SourceDocument]:
        """Handle input documents, optionally transforming them."""
        self.input_docs_called = True
        docs_list = list(docs)
        
        if self.transform_input:
            # Add a marker to metadata to show transformation occurred
            for doc in docs_list:
                if doc.refNode and hasattr(doc.refNode, 'metadata'):
                    if doc.refNode.metadata is None:
                        doc.refNode.metadata = {}
                    doc.refNode.metadata['input_transformed'] = True
        
        return docs_list
    
    def handle_output_doc(self, doc: SourceDocument) -> SourceDocument:
        """Handle output document, optionally transforming it."""
        self.output_doc_called = True
        
        if self.transform_output:
            # Add a marker to metadata to show transformation occurred
            if doc.refNode and hasattr(doc.refNode, 'metadata'):
                if doc.refNode.metadata is None:
                    doc.refNode.metadata = {}
                doc.refNode.metadata['output_transformed'] = True
        
        return doc


class TestPipelineDecoratorAbstract:
    """Tests for PipelineDecorator abstract base class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Verify PipelineDecorator cannot be instantiated directly."""
        # This should raise TypeError because abstract methods are not implemented
        with pytest.raises(TypeError):
            PipelineDecorator()
    
    def test_concrete_implementation_can_be_instantiated(self):
        """Verify concrete implementation can be instantiated."""
        decorator = ConcretePipelineDecorator()
        assert decorator is not None
        assert isinstance(decorator, PipelineDecorator)


class TestPipelineDecoratorHandleInputDocs:
    """Tests for handle_input_docs method."""
    
    def test_handle_input_docs_called(self):
        """Verify handle_input_docs is called."""
        decorator = ConcretePipelineDecorator()
        doc = SourceDocument(refNode=Document(text="test"))
        
        result = decorator.handle_input_docs([doc])
        
        assert decorator.input_docs_called is True
    
    def test_handle_input_docs_returns_iterable(self):
        """Verify handle_input_docs returns iterable of SourceDocuments."""
        decorator = ConcretePipelineDecorator()
        docs = [
            SourceDocument(refNode=Document(text="doc1")),
            SourceDocument(refNode=Document(text="doc2"))
        ]
        
        result = decorator.handle_input_docs(docs)
        
        assert hasattr(result, '__iter__')
        result_list = list(result)
        assert len(result_list) == 2
        assert all(isinstance(d, SourceDocument) for d in result_list)
    
    def test_handle_input_docs_with_transformation(self):
        """Verify handle_input_docs can transform documents."""
        decorator = ConcretePipelineDecorator(transform_input=True)
        doc = SourceDocument(refNode=Document(text="test", metadata={}))
        
        result = list(decorator.handle_input_docs([doc]))
        
        assert result[0].refNode.metadata.get('input_transformed') is True
    
    def test_handle_input_docs_with_empty_list(self):
        """Verify handle_input_docs handles empty list."""
        decorator = ConcretePipelineDecorator()
        
        result = list(decorator.handle_input_docs([]))
        
        assert result == []
        assert decorator.input_docs_called is True
    
    def test_handle_input_docs_preserves_document_count(self):
        """Verify handle_input_docs preserves document count."""
        decorator = ConcretePipelineDecorator()
        docs = [
            SourceDocument(refNode=Document(text=f"doc{i}"))
            for i in range(5)
        ]
        
        result = list(decorator.handle_input_docs(docs))
        
        assert len(result) == 5


class TestPipelineDecoratorHandleOutputDoc:
    """Tests for handle_output_doc method."""
    
    def test_handle_output_doc_called(self):
        """Verify handle_output_doc is called."""
        decorator = ConcretePipelineDecorator()
        doc = SourceDocument(refNode=Document(text="test"))
        
        result = decorator.handle_output_doc(doc)
        
        assert decorator.output_doc_called is True
    
    def test_handle_output_doc_returns_source_document(self):
        """Verify handle_output_doc returns SourceDocument."""
        decorator = ConcretePipelineDecorator()
        doc = SourceDocument(refNode=Document(text="test"))
        
        result = decorator.handle_output_doc(doc)
        
        assert isinstance(result, SourceDocument)
    
    def test_handle_output_doc_with_transformation(self):
        """Verify handle_output_doc can transform document."""
        decorator = ConcretePipelineDecorator(transform_output=True)
        doc = SourceDocument(refNode=Document(text="test", metadata={}))
        
        result = decorator.handle_output_doc(doc)
        
        assert result.refNode.metadata.get('output_transformed') is True
    
    def test_handle_output_doc_with_nodes(self):
        """Verify handle_output_doc handles document with nodes."""
        decorator = ConcretePipelineDecorator()
        doc = SourceDocument(
            refNode=Document(text="test"),
            nodes=[
                TextNode(text="chunk1"),
                TextNode(text="chunk2")
            ]
        )
        
        result = decorator.handle_output_doc(doc)
        
        assert isinstance(result, SourceDocument)
        assert len(result.nodes) == 2


class TestPipelineDecoratorIntegration:
    """Integration tests for PipelineDecorator."""
    
    def test_input_and_output_handling_sequence(self):
        """Verify input and output handling can be used in sequence."""
        decorator = ConcretePipelineDecorator(
            transform_input=True,
            transform_output=True
        )
        
        # Handle input
        input_doc = SourceDocument(refNode=Document(text="test", metadata={}))
        processed_docs = list(decorator.handle_input_docs([input_doc]))
        
        # Handle output
        output_doc = decorator.handle_output_doc(processed_docs[0])
        
        assert decorator.input_docs_called is True
        assert decorator.output_doc_called is True
        assert output_doc.refNode.metadata.get('input_transformed') is True
        assert output_doc.refNode.metadata.get('output_transformed') is True
    
    def test_multiple_decorators_can_be_chained(self):
        """Verify multiple decorators can process documents sequentially."""
        decorator1 = ConcretePipelineDecorator(transform_input=True)
        decorator2 = ConcretePipelineDecorator(transform_output=True)
        
        doc = SourceDocument(refNode=Document(text="test", metadata={}))
        
        # First decorator processes input
        intermediate = list(decorator1.handle_input_docs([doc]))
        
        # Second decorator processes output
        result = decorator2.handle_output_doc(intermediate[0])
        
        assert result.refNode.metadata.get('input_transformed') is True
        assert result.refNode.metadata.get('output_transformed') is True
