# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.types."""

from uuid import UUID

from graphrag_toolkit.core.types import (
    Document,
    Node,
    NodeRef,
    NodeWithScore,
    QueryBundle,
)


class TestNodeRef:
    def test_creation_with_defaults(self):
        ref = NodeRef(node_id="abc")
        assert ref.node_id == "abc"
        assert ref.metadata == {}

    def test_creation_with_metadata(self):
        ref = NodeRef(node_id="abc", metadata={"key": "value"})
        assert ref.metadata == {"key": "value"}

    def test_equality(self):
        assert NodeRef(node_id="a") == NodeRef(node_id="a")
        assert NodeRef(node_id="a") != NodeRef(node_id="b")

    def test_repr(self):
        ref = NodeRef(node_id="x")
        assert "x" in repr(ref)


class TestNode:
    def test_creation_with_defaults(self):
        node = Node(text="hello")
        assert node.text == "hello"
        assert node.metadata == {}
        assert node.embedding is None
        assert node.relationships == {}
        # node_id should be a valid UUID
        UUID(node.node_id)

    def test_creation_with_all_fields(self):
        ref = NodeRef(node_id="parent")
        node = Node(
            text="hello",
            node_id="custom-id",
            metadata={"src": "test"},
            embedding=[0.1, 0.2, 0.3],
            relationships={"parent": ref},
        )
        assert node.node_id == "custom-id"
        assert node.metadata == {"src": "test"}
        assert node.embedding == [0.1, 0.2, 0.3]
        assert node.relationships == {"parent": ref}

    def test_auto_generated_ids_are_unique(self):
        n1 = Node(text="a")
        n2 = Node(text="b")
        assert n1.node_id != n2.node_id

    def test_empty_embedding(self):
        node = Node(text="t", embedding=[])
        assert node.embedding == []

    def test_empty_metadata(self):
        node = Node(text="t", metadata={})
        assert node.metadata == {}

    def test_relationships_with_node_ref(self):
        ref = NodeRef(node_id="ref-1", metadata={"rel": "child"})
        node = Node(text="parent", relationships={"child": ref})
        assert node.relationships["child"].node_id == "ref-1"
        assert node.relationships["child"].metadata == {"rel": "child"}

    def test_equality(self):
        node1 = Node(text="a", node_id="id1")
        node2 = Node(text="a", node_id="id1")
        node3 = Node(text="a", node_id="id2")
        assert node1 == node2
        assert node1 != node3

    def test_repr(self):
        node = Node(text="hello", node_id="nid")
        r = repr(node)
        assert "hello" in r
        assert "nid" in r


class TestDocument:
    def test_inherits_from_node(self):
        doc = Document(text="doc content")
        assert isinstance(doc, Node)

    def test_creation_with_defaults(self):
        doc = Document(text="doc")
        assert doc.text == "doc"
        assert doc.metadata == {}
        assert doc.embedding is None
        assert doc.relationships == {}
        UUID(doc.node_id)

    def test_creation_with_all_fields(self):
        doc = Document(
            text="doc",
            node_id="doc-1",
            metadata={"author": "test"},
            embedding=[1.0],
            relationships={"src": NodeRef(node_id="s")},
        )
        assert doc.node_id == "doc-1"
        assert doc.metadata == {"author": "test"}
        assert doc.embedding == [1.0]


class TestNodeWithScore:
    def test_creation_with_default_score(self):
        node = Node(text="t")
        nws = NodeWithScore(node=node)
        assert nws.node is node
        assert nws.score == 0.0

    def test_creation_with_score(self):
        node = Node(text="t")
        nws = NodeWithScore(node=node, score=0.95)
        assert nws.score == 0.95

    def test_equality(self):
        n = Node(text="t", node_id="x")
        assert NodeWithScore(node=n, score=1.0) == NodeWithScore(node=n, score=1.0)
        assert NodeWithScore(node=n, score=1.0) != NodeWithScore(node=n, score=0.5)

    def test_repr(self):
        n = Node(text="t", node_id="x")
        nws = NodeWithScore(node=n, score=0.5)
        assert "0.5" in repr(nws)


class TestQueryBundle:
    def test_creation_with_defaults(self):
        qb = QueryBundle(query_str="what is X?")
        assert qb.query_str == "what is X?"
        assert qb.embedding is None

    def test_creation_with_embedding(self):
        qb = QueryBundle(query_str="q", embedding=[0.1, 0.2])
        assert qb.embedding == [0.1, 0.2]

    def test_equality(self):
        assert QueryBundle(query_str="a") == QueryBundle(query_str="a")
        assert QueryBundle(query_str="a") != QueryBundle(query_str="b")

    def test_repr(self):
        qb = QueryBundle(query_str="hello")
        assert "hello" in repr(qb)
