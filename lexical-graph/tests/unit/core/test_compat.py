# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.compat."""

from graphrag_toolkit.core import Node, NodeRef, TextNode
from graphrag_toolkit.core.compat import (
    BaseComponent,
    BaseNode,
    NodeRelationship,
    RelatedNodeInfo,
)


class TestAliases:
    def test_textnode_is_node(self):
        assert TextNode is Node

    def test_basenode_is_node(self):
        assert BaseNode is Node

    def test_related_node_info_is_noderef(self):
        assert RelatedNodeInfo is NodeRef

    def test_textnode_creation_identical_to_node(self):
        tn = TextNode(text="hello", node_id="x")
        n = Node(text="hello", node_id="x")
        assert tn == n


class TestNodeRelationship:
    def test_source(self):
        assert NodeRelationship.SOURCE == "source"

    def test_previous(self):
        assert NodeRelationship.PREVIOUS == "previous"

    def test_next(self):
        assert NodeRelationship.NEXT == "next"

    def test_parent(self):
        assert NodeRelationship.PARENT == "parent"

    def test_child(self):
        assert NodeRelationship.CHILD == "child"


class TestBaseComponent:
    def test_instantiation(self):
        bc = BaseComponent()
        assert isinstance(bc, BaseComponent)


class TestPackageImports:
    def test_import_textnode_from_core(self):
        from graphrag_toolkit.core import TextNode as TN

        assert TN is Node

    def test_import_node_from_core(self):
        from graphrag_toolkit.core import Node as N

        assert N is Node
