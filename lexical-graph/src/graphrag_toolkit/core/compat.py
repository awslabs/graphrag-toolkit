# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Type aliases and relationship key utilities."""

from __future__ import annotations

from graphrag_toolkit.core.types import Node, NodeRef

# Type aliases
TextNode = Node
BaseNode = Node
RelatedNodeInfo = NodeRef

# Module-level lazy cache for LlamaIndex enum map
_LI_ENUM_MAP = None


def _get_li_enum_map():
    global _LI_ENUM_MAP
    if _LI_ENUM_MAP is None:
        try:
            from llama_index.core.schema import NodeRelationship as LI_NR
            _LI_ENUM_MAP = {"source": LI_NR.SOURCE, "previous": LI_NR.PREVIOUS,
                           "next": LI_NR.NEXT, "parent": LI_NR.PARENT, "child": LI_NR.CHILD}
        except ImportError:
            _LI_ENUM_MAP = {}
    return _LI_ENUM_MAP


class NodeRelationship:
    """String constants for node relationship types."""

    SOURCE = "source"
    PREVIOUS = "previous"
    NEXT = "next"
    PARENT = "parent"
    CHILD = "child"

    # Enum value mapping for nodes created by optional chunking dependencies
    _LLAMA_INDEX_KEYS = {
        "source": "1",
        "previous": "2",
        "next": "3",
        "parent": "4",
        "child": "5",
    }

    @classmethod
    def get_relationship(cls, relationships: dict, rel_type: str, default=None):
        """Get a relationship from a node's relationships dict, handling
        string keys, enum value strings, and enum object keys."""
        # Try our string key first (e.g. "source")
        result = relationships.get(rel_type)
        if result is not None:
            return result
        # Try enum value string (e.g. "1")
        enum_val = cls._LLAMA_INDEX_KEYS.get(rel_type)
        if enum_val:
            result = relationships.get(enum_val)
            if result is not None:
                return result
        # Try enum object as key (SentenceSplitter uses actual enum instances)
        enum_map = _get_li_enum_map()
        if enum_map:
            enum_key = enum_map.get(rel_type)
            if enum_key:
                result = relationships.get(enum_key)
                if result is not None:
                    return result
        return default


from pydantic import BaseModel

from graphrag_toolkit.core.transform import Transform


class BaseComponent(BaseModel):
    """Base class for pipeline components."""


class TransformComponent(BaseComponent, Transform):
    """Base class for callable transform pipeline components."""

    def __call__(self, nodes: list, **kwargs) -> list:
        """Default no-op implementation."""
        return nodes
