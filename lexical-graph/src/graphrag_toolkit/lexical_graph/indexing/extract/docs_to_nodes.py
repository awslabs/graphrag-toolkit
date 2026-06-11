# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Any

from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import DoNotCheckpoint

from graphrag_toolkit.core.compat import BaseNode, BaseComponent, NodeRelationship
from graphrag_toolkit.core.transform import Transform
from graphrag_toolkit.core.types import Node, NodeRef

logger = logging.getLogger(__name__)

class DocsToNodes(BaseComponent, Transform, DoNotCheckpoint):
    """Parses documents into nodes.

    This class is responsible for parsing a collection of documents or nodes into
    a corresponding list of nodes. It extends functionality from `Transform` and
    `DoNotCheckpoint` to ensure compatibility with inheritable features and avoid
    saving checkpoints during operations.

    Attributes:
        None
    """
    def __call__(
        self,
        nodes: List[BaseNode],
        **kwargs: Any,
    ) -> List[BaseNode]:
        """
        Parses a sequence of nodes into a list of `BaseNode` objects. If a node is of type
        `Document`, it converts the node into `BaseNode` by creating a text node with
        a SOURCE relationship. For other node types, it retains the original node.

        Args:
            nodes (List[BaseNode]): A list of nodes to be parsed.
            **kwargs (Any): Additional keyword arguments for any future extensibility.

        Returns:
            List[BaseNode]: A list of parsed `BaseNode` objects.
        """
        def to_node(node):
            if hasattr(node, 'doc_id'):
                # Build a text node from the document with a SOURCE relationship
                text_node = Node(
                    text=node.text,
                    metadata=dict(node.metadata),
                    relationships={
                        NodeRelationship.SOURCE: NodeRef(
                            node_id=node.node_id,
                            metadata=dict(node.metadata),
                        )
                    },
                )
                return text_node
            else:
                return node
    
        return [to_node(n) for n in nodes]
