# Copyright (c) Evan Erwee. All rights reserved.

"""Document Graph Data Models.

This module defines Pydantic models for graph nodes and edges.
"""

import logging
from pydantic import BaseModel
from typing import Dict

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

class NodeModel(BaseModel):
    """Pydantic model for graph nodes.
    
    Attributes:
        id: Unique identifier for the node
        type: Node type/label
        properties: Dictionary of node properties
        
    Examples:
        >>> node = NodeModel(
        ...     id="doc-123",
        ...     type="Document",
        ...     properties={"title": "Example Document", "created_date": "2025-07-25"}
        ... )
        >>> node.id
        'doc-123'
        >>> node.type
        'Document'
        >>> node.properties["title"]
        'Example Document'
    """
    id: str
    type: str
    properties: Dict[str, object]

class EdgeModel(BaseModel):
    """Pydantic model for graph edges.
    
    Attributes:
        source: Source node identifier
        target: Target node identifier
        type: Edge type/relationship
        properties: Dictionary of edge properties
        
    Examples:
        >>> edge = EdgeModel(
        ...     source="doc-123",
        ...     target="doc-456",
        ...     type="REFERENCES",
        ...     properties={"weight": 0.8, "created_date": "2025-07-25"}
        ... )
        >>> edge.source
        'doc-123'
        >>> edge.target
        'doc-456'
        >>> edge.type
        'REFERENCES'
    """
    source: str
    target: str
    type: str
    properties: Dict[str, object]
