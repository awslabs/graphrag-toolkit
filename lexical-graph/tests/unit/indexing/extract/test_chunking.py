# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from hypothesis import given, strategies as st, settings
from graphrag_toolkit.core.text_splitter import SentenceSplitter
from graphrag_toolkit.core.types import Document


class TestChunkingProperties:
    """Property-based tests for document chunking."""
    
    pass


class TestChunkingBasicBehavior:
    """Unit tests for basic chunking behavior."""
    
    def test_chunking_simple_text(self):
        """Verify chunking works with simple text."""
        text = "This is a simple test. It has multiple sentences. Each sentence is short."
        doc = Document(text=text)
        
        splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)
        chunks = splitter.get_nodes_from_documents([doc])
        
        assert len(chunks) > 0
        assert all(chunk.text for chunk in chunks)
    
    def test_chunking_preserves_content_no_overlap(self):
        """Verify chunking without overlap preserves exact content."""
        text = "A" * 100 + "B" * 100 + "C" * 100
        doc = Document(text=text)
        
        splitter = SentenceSplitter(chunk_size=100, chunk_overlap=0)
        chunks = splitter.get_nodes_from_documents([doc])
        
        concatenated = ''.join(chunk.text for chunk in chunks)
        assert concatenated == text
    
    def test_chunking_with_overlap_contains_original(self):
        """Verify chunking with overlap contains all original content."""
        text = "The quick brown fox jumps over the lazy dog. " * 10
        doc = Document(text=text)
        
        splitter = SentenceSplitter(chunk_size=100, chunk_overlap=20)
        chunks = splitter.get_nodes_from_documents([doc])
        
        # All original sentences should appear in at least one chunk
        assert len(chunks) > 0
        all_text = ' '.join(chunk.text for chunk in chunks)
        for sentence in text.strip().split('. '):
            if sentence:
                assert sentence in all_text
    
    def test_chunking_empty_document(self):
        """Verify handling of empty document."""
        doc = Document(text="")
        
        splitter = SentenceSplitter(chunk_size=100, chunk_overlap=10)
        chunks = splitter.get_nodes_from_documents([doc])
        
        # Empty document should produce no chunks or one empty chunk
        assert len(chunks) <= 1
    
    def test_chunking_single_character(self):
        """Verify handling of single character document."""
        doc = Document(text="A")
        
        splitter = SentenceSplitter(chunk_size=100, chunk_overlap=10)
        chunks = splitter.get_nodes_from_documents([doc])
        
        assert len(chunks) == 1
        assert chunks[0].text == "A"
