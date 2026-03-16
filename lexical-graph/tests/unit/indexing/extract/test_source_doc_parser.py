# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.extract.source_doc_parser import SourceDocParser
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument


# Concrete implementation for testing
class ConcreteSourceDocParser(SourceDocParser):
    """Concrete implementation of SourceDocParser for testing."""
    
    def _parse_source_docs(self, source_documents):
        """Simple pass-through implementation for testing."""
        return source_documents


class TestSourceDocParserInitialization:
    """Tests for SourceDocParser initialization."""
    
    def test_initialization_default(self):
        """Verify SourceDocParser concrete implementation initializes."""
        parser = ConcreteSourceDocParser()
        assert parser is not None


class TestSourceDocParserParsing:
    """Tests for document parsing functionality."""
    
    def test_parse_valid_document(self):
        """Verify parsing of valid document."""
        parser = ConcreteSourceDocParser()
        from llama_index.core.schema import TextNode
        node = TextNode(text="Sample document text")
        doc = SourceDocument(nodes=[node])
        
        result = list(parser.parse_source_docs([doc]))
        
        assert len(result) == 1
        assert result[0].nodes[0].text == "Sample document text"
    
    def test_parse_document_with_metadata(self):
        """Verify metadata is preserved during parsing."""
        parser = ConcreteSourceDocParser()
        from llama_index.core.schema import TextNode
        node = TextNode(
            text="Document content",
            metadata={
                "source": "test_source",
                "date": "2024-01-01",
                "author": "Test Author"
            }
        )
        doc = SourceDocument(nodes=[node])
        
        result = list(parser.parse_source_docs([doc]))
        
        assert len(result) == 1
        assert result[0].nodes[0].metadata["source"] == "test_source"
