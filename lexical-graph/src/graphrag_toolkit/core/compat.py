# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility aliases mapping LlamaIndex type names to core types."""

from __future__ import annotations

from graphrag_toolkit.core.types import Node, NodeRef

# LlamaIndex compatibility aliases
TextNode = Node
BaseNode = Node
RelatedNodeInfo = NodeRef


class NodeRelationship:
    """String constants for node relationship types."""

    SOURCE = "source"
    PREVIOUS = "previous"
    NEXT = "next"
    PARENT = "parent"
    CHILD = "child"


class BaseComponent:
    """Minimal base class for type compatibility with LlamaIndex components."""
