# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Anti-corruption layer for LlamaIndex node types.

Converts LlamaIndex nodes to internal Node types at the pipeline boundary.
This is the ONLY module that needs to understand LlamaIndex's relationship
key format. All downstream code works with plain string keys.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Module-level lazy cache for LlamaIndex enum mapping
_RELATIONSHIP_MAP: Optional[Dict] = None


def _build_relationship_map() -> Dict:
    """Build mapping from LlamaIndex enum objects to string keys.

    Uses runtime introspection — maps enum objects by identity, not by
    hardcoded string values. If LlamaIndex changes enum values, this
    still works correctly. If they remove enum members, we get ImportError.
    """
    global _RELATIONSHIP_MAP
    try:
        from llama_index.core.schema import NodeRelationship as LI_NR
        _RELATIONSHIP_MAP = {
            LI_NR.SOURCE: "source",
            LI_NR.PREVIOUS: "previous",
            LI_NR.NEXT: "next",
            LI_NR.PARENT: "parent",
            LI_NR.CHILD: "child",
        }
    except ImportError:
        _RELATIONSHIP_MAP = {}
    return _RELATIONSHIP_MAP


def _get_relationship_map() -> Dict:
    global _RELATIONSHIP_MAP
    if _RELATIONSHIP_MAP is None:
        _build_relationship_map()
    return _RELATIONSHIP_MAP


def normalize_relationship_keys(relationships: Dict) -> Dict[str, Any]:
    """Normalize relationship dict keys to plain strings.

    Handles:
    - String keys ('source') -> pass through
    - LlamaIndex enum objects (<NodeRelationship.SOURCE: '1'>) -> map to string
    - Enum value strings ('1', '2', ...) -> map to string

    This function is called ONCE at the boundary where LlamaIndex nodes
    enter our pipeline.
    """
    _REV_VALUE_MAP = {"1": "source", "2": "previous", "3": "next", "4": "parent", "5": "child"}
    rel_map = _get_relationship_map()
    result = {}

    for key, value in relationships.items():
        if isinstance(key, str):
            # Could be our string key ('source') or LlamaIndex enum value ('1')
            normalized = _REV_VALUE_MAP.get(key, key)
            result[normalized] = value
        elif key in rel_map:
            # LlamaIndex enum object
            result[rel_map[key]] = value
        else:
            # Unknown key type — try .value attribute (future enum variants)
            val = getattr(key, "value", None)
            if val and val in _REV_VALUE_MAP:
                result[_REV_VALUE_MAP[val]] = value
            else:
                logger.warning(f"Unknown relationship key: {type(key)}={key}")
                result[str(key)] = value

    return result


def convert_llama_node(li_node) -> "Node":
    """Convert a LlamaIndex TextNode/BaseNode to internal Node type.

    Called at the pipeline boundary (in IdRewriter) immediately after
    LlamaIndex's SentenceSplitter produces nodes.
    """
    from graphrag_toolkit.core.types import Node, NodeRef

    # Normalize relationships: convert enum keys to strings, LI RelatedNodeInfo to NodeRef
    normalized_rels = {}
    for key, value in normalize_relationship_keys(li_node.relationships).items():
        if hasattr(value, "node_id"):
            # LlamaIndex RelatedNodeInfo or our NodeRef
            normalized_rels[key] = NodeRef(
                node_id=value.node_id,
                metadata=getattr(value, "metadata", {}),
                hash=getattr(value, "hash", None),
            )
        elif isinstance(value, dict):
            normalized_rels[key] = NodeRef(
                node_id=value.get("node_id", ""),
                metadata=value.get("metadata", {}),
            )
        else:
            normalized_rels[key] = value

    return Node(
        node_id=li_node.node_id,
        text=li_node.text,
        metadata=dict(li_node.metadata) if li_node.metadata else {},
        embedding=li_node.embedding,
        relationships=normalized_rels,
        excluded_embed_metadata_keys=list(getattr(li_node, "excluded_embed_metadata_keys", [])),
        excluded_llm_metadata_keys=list(getattr(li_node, "excluded_llm_metadata_keys", [])),
        start_char_idx=getattr(li_node, "start_char_idx", None),
        end_char_idx=getattr(li_node, "end_char_idx", None),
    )


def is_llama_index_node(node) -> bool:
    """Check if a node is a LlamaIndex type (not our internal Node)."""
    try:
        from llama_index.core.schema import BaseNode as LI_BaseNode
        return isinstance(node, LI_BaseNode)
    except ImportError:
        return False
