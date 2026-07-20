# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""[Module Name] Module for Document Graph Plugin System.

This module [provides/defines/implements] [brief description of module purpose].

The module [includes/contains/offers] [key components or features]:
- [Component/Feature 1]: [Brief description]
- [Component/Feature 2]: [Brief description]
- [Component/Feature 3]: [Brief description]

[Additional context about design principles, architecture, or implementation details]

Usage:
    # [Example usage code]
    [explanation of example]
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


"""Graph Elements Module for Document Graph Storage.

This module defines the fundamental data structures used to represent graph elements
(nodes and edges) in the document graph storage system. These classes serve as the
core data transfer objects between different components of the storage layer.

The module provides two main dataclasses:
- Node: Represents a vertex in the graph with an ID, labels, and properties
- Edge: Represents a relationship between two nodes with source and target IDs,
  a label, and properties

These classes are designed to be simple, serializable data containers that can be
easily converted to and from various graph database formats.

Usage:
    
    # Create a node
    document_node = Node(
        id="doc123",
        labels=["Document"],
        properties={"title": "Example Document", "created_at": "2023-01-01"}
    )
    
    # Create an edge
    contains_edge = Edge(
        id="edge456",
        source_id="doc123",
        target_id="section789",
        label="CONTAINS",
        properties={"order": 1}
    )
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class Node:
    """
    Represents a node (vertex) in the document graph.
    
    A node is a fundamental element in a graph that represents an entity or concept.
    In the document graph context, nodes typically represent documents, sections,
    paragraphs, or other document elements.
    
    This class is implemented as a dataclass for simplicity and serialization support.
    It provides default initialization for labels and properties if they are not provided.
    
    Attributes:
        id (str): Unique identifier for the node.
        labels (list): List of labels/types associated with this node.
            Examples: ["Document", "Section", "Paragraph"].
            Defaults to an empty list if not provided.
        properties (Dict[str, Any]): Dictionary of key-value properties for the node.
            Examples: {"title": "Introduction", "created_at": "2023-01-01"}.
            Defaults to an empty dictionary if not provided.
    
    Example:
        >>> node = Node(id="doc123", labels=["Document"], properties={"title": "Example"})
        >>> print(node.id, node.labels)
        doc123 ['Document']
    """
    
    id: str
    labels: list = None
    properties: Dict[str, Any] = None
    
    def __post_init__(self):
        """
        Initialize default values for properties and labels after instance creation.
        
        This method is automatically called by the dataclass after the object is created.
        It ensures that properties and labels are never None by setting them to empty
        collections when they are not provided during initialization.
        
        - Sets properties to an empty dict if None
        - Sets labels to an empty list if None
        """
        if self.properties is None:
            self.properties = {}
        if self.labels is None:
            self.labels = []


@dataclass
class Edge:
    """
    Represents an edge (relationship) in the document graph.
    
    An edge is a connection between two nodes in a graph that represents a relationship
    or association. In the document graph context, edges typically represent relationships
    like "CONTAINS", "REFERENCES", "SIMILAR_TO", etc. between document elements.
    
    This class is implemented as a dataclass for simplicity and serialization support.
    It provides default initialization for properties if they are not provided.
    
    Attributes:
        id (str): Unique identifier for the edge.
        source_id (str): Identifier of the source/from node.
        target_id (str): Identifier of the target/to node.
        label (str): Type of relationship this edge represents.
            Examples: "CONTAINS", "REFERENCES", "SIMILAR_TO".
        properties (Dict[str, Any]): Dictionary of key-value properties for the edge.
            Examples: {"weight": 0.8, "created_at": "2023-01-01"}.
            Defaults to an empty dictionary if not provided.
    
    Example:
        >>> edge = Edge(
        ...     id="edge123",
        ...     source_id="doc1",
        ...     target_id="section2",
        ...     label="CONTAINS",
        ...     properties={"order": 1}
        ... )
        >>> print(edge.source_id, edge.label, edge.target_id)
        doc1 CONTAINS section2
    """
    
    id: str
    source_id: str
    target_id: str
    label: str
    properties: Dict[str, Any] = None
    
    def __post_init__(self):
        """
        Initialize default values for properties after instance creation.
        
        This method is automatically called by the dataclass after the object is created.
        It ensures that properties is never None by setting it to an empty
        dictionary when it is not provided during initialization.
        
        - Sets properties to an empty dict if None
        """
        if self.properties is None:
            self.properties = {}