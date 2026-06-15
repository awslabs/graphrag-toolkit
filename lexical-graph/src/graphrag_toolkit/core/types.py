# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Core data model types for graph nodes, documents, and queries."""

from __future__ import annotations

import json as _json
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


class NodeRef(BaseModel):
    """Reference to another node by ID, with optional metadata."""

    node_id: str
    node_type: Optional[str] = None
    metadata: dict = Field(default_factory=dict)
    hash: Optional[str] = None


class Node(BaseModel):
    """Base node representing a chunk of text with optional embedding and relationships."""

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    id_: str = Field(default_factory=lambda: str(uuid4()), alias="id_")
    text: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)
    embedding: Optional[List[float]] = None
    relationships: Dict[str, Any] = Field(default_factory=dict)
    excluded_embed_metadata_keys: List[str] = Field(default_factory=list)
    excluded_llm_metadata_keys: List[str] = Field(default_factory=list)
    text_template: str = "{metadata_str}\n\n{content}"
    metadata_template: str = "{key}: {value}"
    metadata_separator: str = "\n"
    start_char_idx: Optional[int] = None
    end_char_idx: Optional[int] = None
    mimetype: str = "text/plain"

    @model_validator(mode="before")
    @classmethod
    def _handle_node_id(cls, data: Any) -> Any:
        """Accept node_id as input and map to id_."""
        if isinstance(data, dict) and "node_id" in data and "id_" not in data:
            data["id_"] = data.pop("node_id")
        return data

    @property
    def node_id(self) -> str:
        """Backward-compatible alias for id_."""
        return self.id_

    @node_id.setter
    def node_id(self, value: str) -> None:
        self.id_ = value

    def get_content(self, metadata_mode: str = "none") -> str:
        """Get node content, optionally formatted with metadata."""
        if metadata_mode == "none" or not self.metadata:
            return self.text
        metadata_keys = [k for k in self.metadata if k not in self.excluded_embed_metadata_keys]
        metadata_str = self.metadata_separator.join(
            self.metadata_template.format(key=k, value=self.metadata[k])
            for k in metadata_keys
        )
        return self.text_template.format(metadata_str=metadata_str, content=self.text)

    def set_content(self, value: str) -> None:
        """Set node text content."""
        self.text = value

    def as_related_node_info(self) -> "NodeRef":
        """Return a NodeRef pointing to this node."""
        return NodeRef(node_id=self.id_, metadata=self.metadata)

    @classmethod
    def from_json(cls, json_str: str) -> "Node":
        """Deserialize a Node from JSON."""
        data = _json.loads(json_str)
        # Normalize node_id field name
        node_id = data.pop("node_id", None) or data.pop("id_", None) or str(uuid4())
        data["id_"] = node_id
        # Parse relationships from serialized format
        raw_rels = data.get("relationships", {})
        relationships = {}
        for key, val in raw_rels.items():
            if isinstance(val, dict):
                relationships[key] = NodeRef(
                    node_id=val.get("node_id", ""),
                    metadata=val.get("metadata", {}),
                )
            elif isinstance(val, str):
                relationships[key] = NodeRef(node_id=val)
            else:
                relationships[key] = val
        data["relationships"] = relationships
        return cls.model_validate(data)

    def to_json(self) -> str:
        """Serialize node to JSON string."""
        return self.model_dump_json()

    def to_dict(self) -> dict:
        """Serialize node to dictionary."""
        return self.model_dump()


class Document(Node):
    """A source document. Inherits all fields from Node."""

    @property
    def doc_id(self) -> str:
        """Alias for id_ field."""
        return self.id_


class NodeWithScore(BaseModel):
    """A node paired with a relevance score."""

    node: Node
    score: Optional[float] = 0.0

    @property
    def metadata(self) -> dict:
        """Delegate to node.metadata."""
        return self.node.metadata

    @property
    def text(self) -> str:
        """Delegate to node.text."""
        return self.node.text


class QueryBundle(BaseModel):
    """A user query with optional pre-computed embedding."""

    query_str: str
    embedding: Optional[List[float]] = None

    def __init__(self, query_str: str = "", **kwargs: Any) -> None:
        """Allow positional initialization: QueryBundle('text')."""
        super().__init__(query_str=query_str, **kwargs)
