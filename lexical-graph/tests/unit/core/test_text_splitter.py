# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for custom text splitters."""

import pytest

from graphrag_toolkit.core.text_splitter import SentenceSplitter, TokenTextSplitter
from graphrag_toolkit.core.types import Document, Node, NodeRef


class TestSentenceSplitter256:
    """Test SentenceSplitter with chunk_size=256, chunk_overlap=25."""

    def setup_method(self):
        self.splitter = SentenceSplitter(chunk_size=256, chunk_overlap=25)

    def test_short_text_single_chunk(self):
        doc = Document(text="Hello world. This is a test.")
        nodes = self.splitter.get_nodes_from_documents([doc])
        assert len(nodes) == 1
        assert nodes[0].text == "Hello world. This is a test."

    def test_long_text_multiple_chunks(self):
        text = "Amazon Neptune is a fast graph database. It supports Gremlin and SPARQL. " * 30
        doc = Document(text=text)
        nodes = self.splitter.get_nodes_from_documents([doc])
        assert len(nodes) > 1

    def test_metadata_copied(self):
        doc = Document(text="Test sentence one. Test sentence two. " * 50, metadata={"source": "test.txt"})
        nodes = self.splitter.get_nodes_from_documents([doc])
        for node in nodes:
            assert node.metadata == {"source": "test.txt"}


class TestSentenceSplitter50:
    """Test SentenceSplitter with chunk_size=50, chunk_overlap=10."""

    def setup_method(self):
        self.splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)

    def test_produces_multiple_chunks(self):
        text = "Amazon Neptune is a fast graph database. It supports Gremlin and SPARQL. " * 20
        doc = Document(text=text)
        nodes = self.splitter.get_nodes_from_documents([doc])
        assert len(nodes) >= 3

    def test_chunk_token_sizes(self):
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        text = "Amazon Neptune is a fast graph database. It supports Gremlin and SPARQL. " * 20
        doc = Document(text=text)
        nodes = self.splitter.get_nodes_from_documents([doc])
        for node in nodes:
            token_count = len(enc.encode(node.text))
            # Allow slight overshoot for sentence boundaries
            assert token_count <= 60, f"Chunk has {token_count} tokens, expected <= ~50"


class TestTokenTextSplitter:
    """Test TokenTextSplitter with chunk_size=25, chunk_overlap=5."""

    def setup_method(self):
        self.splitter = TokenTextSplitter(chunk_size=25, chunk_overlap=5)

    def test_short_text_no_split(self):
        result = self.splitter.split_text("Hello world")
        assert result == ["Hello world"]

    def test_split_with_overlap(self):
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        text = "The quick brown fox jumps over the lazy dog. " * 20
        chunks = self.splitter.split_text(text)
        assert len(chunks) > 1
        for chunk in chunks:
            assert len(enc.encode(chunk)) <= 25

    def test_overlap_present(self):
        text = "word " * 100  # 100 tokens
        chunks = self.splitter.split_text(text)
        # With chunk_size=25, overlap=5, step=20
        # Expect ~5 chunks for 100 tokens
        assert len(chunks) >= 4


class TestEdgeCases:
    """Test edge cases."""

    def test_empty_text(self):
        splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)
        doc = Document(text="")
        nodes = splitter.get_nodes_from_documents([doc])
        assert len(nodes) == 1
        assert nodes[0].text == ""

    def test_single_sentence(self):
        splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)
        doc = Document(text="Just one sentence here.")
        nodes = splitter.get_nodes_from_documents([doc])
        assert len(nodes) == 1
        assert nodes[0].text == "Just one sentence here."

    def test_sentence_longer_than_chunk_size(self):
        splitter = SentenceSplitter(chunk_size=10, chunk_overlap=2)
        # A single long sentence that exceeds chunk_size
        long_sentence = "This is a very long sentence with many words that exceeds the token limit significantly."
        doc = Document(text=long_sentence)
        nodes = splitter.get_nodes_from_documents([doc])
        # Should still produce output (split sub-sentence)
        assert len(nodes) >= 1
        # Reconstructed text should cover the original
        combined = " ".join(n.text for n in nodes)
        assert "This is a very long sentence" in combined or nodes[0].text.startswith("This")


class TestNodeOutput:
    """Test that output Node objects have correct relationships and char indices."""

    def test_source_relationship(self):
        splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)
        doc = Document(text="First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence. " * 5)
        nodes = splitter.get_nodes_from_documents([doc])
        for node in nodes:
            assert "source" in node.relationships
            ref = node.relationships["source"]
            assert isinstance(ref, NodeRef)
            assert ref.node_id == doc.node_id

    def test_prev_next_relationships(self):
        splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)
        doc = Document(text="First sentence. Second sentence. Third sentence. " * 10)
        nodes = splitter.get_nodes_from_documents([doc])
        if len(nodes) > 1:
            assert "previous" not in nodes[0].relationships
            assert "next" in nodes[0].relationships
            assert "previous" in nodes[-1].relationships
            assert "next" not in nodes[-1].relationships

    def test_char_indices(self):
        splitter = SentenceSplitter(chunk_size=50, chunk_overlap=0)
        text = "Hello world. This is a test. Another sentence here. One more sentence. Final."
        doc = Document(text=text)
        nodes = splitter.get_nodes_from_documents([doc])
        for node in nodes:
            assert node.start_char_idx is not None
            assert node.end_char_idx is not None
            assert text[node.start_char_idx:node.end_char_idx] == node.text


class TestLlamaIndexComparison:
    """Compare output with LlamaIndex SentenceSplitter if available."""

    @pytest.fixture(autouse=True)
    def check_llamaindex(self):
        try:
            from llama_index.core.node_parser import SentenceSplitter as LISplitter
            from llama_index.core.schema import Document as LIDoc
            self.li_available = True
            self.LISplitter = LISplitter
            self.LIDoc = LIDoc
        except ImportError:
            self.li_available = False

    @pytest.mark.skipif(
        not pytest.importorskip("llama_index.core", reason="llama-index-core not installed"),
        reason="llama-index-core not installed"
    )
    def test_matches_llamaindex_output(self):
        if not self.li_available:
            pytest.skip("llama-index-core not installed")

        text = "Amazon Neptune is a fast graph database. It supports Gremlin and SPARQL. " * 20

        our_splitter = SentenceSplitter(chunk_size=50, chunk_overlap=10)
        li_splitter = self.LISplitter(chunk_size=50, chunk_overlap=10)

        our_nodes = our_splitter.get_nodes_from_documents([Document(text=text)])
        li_nodes = li_splitter.get_nodes_from_documents([self.LIDoc(text=text)])

        assert len(our_nodes) == len(li_nodes), (
            f"Chunk count mismatch: ours={len(our_nodes)}, LI={len(li_nodes)}"
        )
        for i, (ours, theirs) in enumerate(zip(our_nodes, li_nodes)):
            assert ours.text == theirs.text, (
                f"Chunk {i} text mismatch:\n  ours={ours.text[:80]!r}\n  LI  ={theirs.text[:80]!r}"
            )

    @pytest.mark.skipif(
        not pytest.importorskip("llama_index.core", reason="llama-index-core not installed"),
        reason="llama-index-core not installed"
    )
    def test_matches_llamaindex_256_25(self):
        if not self.li_available:
            pytest.skip("llama-index-core not installed")

        text = "Amazon Neptune is a fast graph database. It supports Gremlin and SPARQL. " * 50

        our_splitter = SentenceSplitter(chunk_size=256, chunk_overlap=25)
        li_splitter = self.LISplitter(chunk_size=256, chunk_overlap=25)

        our_nodes = our_splitter.get_nodes_from_documents([Document(text=text)])
        li_nodes = li_splitter.get_nodes_from_documents([self.LIDoc(text=text)])

        assert len(our_nodes) == len(li_nodes)
        for i, (ours, theirs) in enumerate(zip(our_nodes, li_nodes)):
            assert ours.text == theirs.text, f"Chunk {i} mismatch"
