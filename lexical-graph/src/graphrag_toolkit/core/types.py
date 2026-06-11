# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Core data model types replacing llama_index.core.schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from uuid import uuid4


@dataclass
class NodeRef:
    """Reference to another node by ID, with optional metadata."""

    node_id: str
    metadata: dict = field(default_factory=dict)


@dataclass
class Node:
    """Base node representing a chunk of text with optional embedding and relationships."""

    text: str
    node_id: str = field(default_factory=lambda: str(uuid4()))
    metadata: dict = field(default_factory=dict)
    embedding: Optional[list[float]] = None
    relationships: dict[str, NodeRef] = field(default_factory=dict)


@dataclass
class Document(Node):
    """A source document. Inherits all fields from Node."""


@dataclass
class NodeWithScore:
    """A node paired with a relevance score."""

    node: Node
    score: float = 0.0


@dataclass
class QueryBundle:
    """A user query with optional pre-computed embedding."""

    query_str: str
    embedding: Optional[list[float]] = None
