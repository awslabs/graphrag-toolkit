# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Integration tests verifying core module components work together."""

from __future__ import annotations

import asyncio
import json
from typing import Iterator
from unittest.mock import MagicMock, patch

from graphrag_toolkit.core import (
    BedrockEmbeddingProvider,
    BedrockLLMProvider,
    CallbackRegistry,
    Document,
    Node,
    NodeRef,
    NodeRelationship,
    NodeWithScore,
    Pipeline,
    PostProcessor,
    PromptTemplate,
    QueryBundle,
    QueryEngine,
    Retriever,
    TextNode,
    Transform,
)
from graphrag_toolkit.core.callbacks import TRANSFORM_END, TRANSFORM_START
from graphrag_toolkit.core.extractor import Extractor


# --- Helpers ---

def _mock_embedding_body(embedding: list[float]) -> MagicMock:
    body = MagicMock()
    body.read.return_value = json.dumps({"embedding": embedding}).encode()
    return body


def _mock_converse_response(text: str) -> dict:
    return {"output": {"message": {"content": [{"text": text}]}}}


# --- Test: Indexing Pipeline ---

class _SentenceSplitter(Transform):
    """Split document text into sentence-level nodes."""

    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        result = []
        for node in nodes:
            for sentence in node.text.split(". "):
                child = Node(
                    text=sentence.strip().rstrip("."),
                    metadata={"source_id": node.node_id},
                    relationships={"source": NodeRef(node_id=node.node_id)},
                )
                result.append(child)
        return result


class TestIndexingPipeline:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_documents_to_embedded_nodes(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": _mock_embedding_body([0.1, 0.2, 0.3])
        }

        # Create source documents
        docs = [
            Document(text="GraphRAG combines graphs and retrieval. It improves accuracy."),
            Document(text="Knowledge graphs store entities. Entities have relationships."),
        ]

        # Run transform pipeline
        pipeline = Pipeline(transforms=[_SentenceSplitter()])
        nodes = pipeline.run(docs)

        # Verify splits
        assert len(nodes) == 4
        assert all(n.relationships.get("source") for n in nodes)

        # Embed all nodes
        embedder = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0")
        for node in nodes:
            node.embedding = embedder.embed_text(node.text)

        # Verify embeddings assigned
        assert all(n.embedding == [0.1, 0.2, 0.3] for n in nodes)
        assert mock_client.invoke_model.call_count == 4


# --- Test: Retrieval Pipeline ---

class _MockRetriever(Retriever):
    def __init__(self, nodes: list[NodeWithScore]):
        self._nodes = nodes

    def retrieve(self, query: QueryBundle) -> list[NodeWithScore]:
        return self._nodes


class _ScoreFilter(PostProcessor):
    def __init__(self, threshold: float):
        self._threshold = threshold

    def process(self, nodes: list[NodeWithScore], query: QueryBundle) -> list[NodeWithScore]:
        return [n for n in nodes if n.score >= self._threshold]


class _SimpleQAEngine(QueryEngine):
    def __init__(self, retriever: Retriever, postprocessor: PostProcessor, llm: BedrockLLMProvider):
        self._retriever = retriever
        self._pp = postprocessor
        self._llm = llm

    def query(self, query_str: str) -> str:
        qb = QueryBundle(query_str=query_str)
        nodes = self._retriever.retrieve(qb)
        nodes = self._pp.process(nodes, qb)
        context = "\n".join(n.node.text for n in nodes)
        prompt = f"Context: {context}\n\nQuestion: {query_str}"
        return self._llm.predict(prompt)

    def stream(self, query_str: str) -> Iterator[str]:
        yield self.query(query_str)


class TestRetrievalPipeline:
    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_query_end_to_end(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client
        mock_client.converse.return_value = _mock_converse_response("GraphRAG improves accuracy by using graphs.")

        # Set up retrieval nodes
        retrieved = [
            NodeWithScore(node=Node(text="GraphRAG uses knowledge graphs"), score=0.9),
            NodeWithScore(node=Node(text="Vector search is fast"), score=0.3),
            NodeWithScore(node=Node(text="Graphs improve multi-hop reasoning"), score=0.8),
        ]

        retriever = _MockRetriever(retrieved)
        postprocessor = _ScoreFilter(threshold=0.5)
        llm = BedrockLLMProvider(model_id="anthropic.claude-3-sonnet")
        engine = _SimpleQAEngine(retriever, postprocessor, llm)

        answer = engine.query("How does GraphRAG work?")

        assert answer == "GraphRAG improves accuracy by using graphs."
        # Verify low-score node was filtered
        call_args = mock_client.converse.call_args[1]
        prompt_text = call_args["messages"][0]["content"][0]["text"]
        assert "Vector search is fast" not in prompt_text
        assert "GraphRAG uses knowledge graphs" in prompt_text


# --- Test: PromptTemplate → LLMProvider ---

class TestPromptLLMIntegration:
    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_formatted_prompt_reaches_llm(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client
        mock_client.converse.return_value = _mock_converse_response("42")

        template = PromptTemplate("Given context: {context}\nAnswer: {question}")
        formatted = template.format(context="The sky is blue", question="What color?")

        llm = BedrockLLMProvider(model_id="anthropic.claude-3-haiku")
        result = llm.predict(formatted)

        assert result == "42"
        sent_text = mock_client.converse.call_args[1]["messages"][0]["content"][0]["text"]
        assert "The sky is blue" in sent_text
        assert "What color?" in sent_text


# --- Test: EmbeddingProvider → Node ---

class TestEmbeddingNodeIntegration:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_embed_and_assign_to_nodes(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client
        embedding_1024 = [0.01 * i for i in range(1024)]
        mock_client.invoke_model.return_value = {
            "body": _mock_embedding_body(embedding_1024)
        }

        embedder = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0")
        nodes = [Node(text="first"), Node(text="second"), Node(text="third")]

        embeddings = embedder.embed_texts([n.text for n in nodes])
        for node, emb in zip(nodes, embeddings):
            node.embedding = emb

        assert all(len(n.embedding) == 1024 for n in nodes)
        assert embedder.dimensions == 1024


# --- Test: CallbackRegistry ---

class _EventEmittingTransform(Transform):
    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        CallbackRegistry.emit(TRANSFORM_START, {"transform": "upper"})
        result = [Node(text=n.text.upper(), node_id=n.node_id) for n in nodes]
        CallbackRegistry.emit(TRANSFORM_END, {"transform": "upper", "count": len(result)})
        return result


class TestCallbackIntegration:
    def setup_method(self):
        CallbackRegistry.clear()

    def teardown_method(self):
        CallbackRegistry.clear()

    def test_pipeline_emits_events(self):
        events = []
        CallbackRegistry.register(lambda et, p: events.append((et, p)))

        pipeline = Pipeline(transforms=[_EventEmittingTransform()])
        nodes = [Node(text="hello", node_id="1")]
        result = pipeline.run(nodes)

        assert result[0].text == "HELLO"
        assert len(events) == 2
        assert events[0] == (TRANSFORM_START, {"transform": "upper"})
        assert events[1] == (TRANSFORM_END, {"transform": "upper", "count": 1})


# --- Test: Compatibility Layer ---

class TestCompatibilityIntegration:
    def test_textnode_in_pipeline(self):
        """TextNode alias works seamlessly in pipeline flows."""
        nodes = [TextNode(text="compat test", node_id="tn-1")]
        assert isinstance(nodes[0], Node)

        # Use in retriever result
        nws = NodeWithScore(node=nodes[0], score=0.7)
        assert nws.node.text == "compat test"

    def test_node_relationship_constants(self):
        """NodeRelationship constants work with Node.relationships dict."""
        parent = Node(text="parent", node_id="p1")
        child = Node(
            text="child",
            relationships={NodeRelationship.PARENT: NodeRef(node_id=parent.node_id)},
        )
        assert child.relationships["parent"].node_id == "p1"

    def test_textnode_and_node_interop(self):
        """TextNode and Node are the same class — interop is seamless."""
        tn = TextNode(text="hello", node_id="x")
        n = Node(text="hello", node_id="x")
        assert tn == n
        assert type(tn) is type(n)
