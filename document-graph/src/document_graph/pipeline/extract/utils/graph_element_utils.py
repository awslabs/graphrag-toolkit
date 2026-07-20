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
# logging via graphrag-toolkit  # noqa: F401


# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Graph element utilities for creating nodes, links, and edges.

This module provides functions for creating graph elements such as text nodes and links/edges
between nodes. It delegates to the shared implementation in the graph module.
"""

# Import from the authoritative shared location
from document_graph.shared.graph.graph_element_utils import (
    create_text_node,
    create_link
)

# Alias for backward compatibility
def create_edge(source: str, target: str, edge_type: str):
    """Create an edge between two nodes in the graph.
    
    This is an alias for create_link, maintained for backward compatibility.
    
    Args:
        source: The ID of the source node
        target: The ID of the target node
        edge_type: The type of edge to create
        
    Returns:
        A dictionary representing the edge/link between the nodes
    """
    return create_link(source, target, edge_type)
