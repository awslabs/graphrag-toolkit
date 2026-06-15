# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Any

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.core.compat import BaseNode


logger = logging.getLogger(__name__)

class NullBuilder(NodeHandler):
    """
    Handles the acceptance of nodes without performing any transformations, primarily
    used as a pass-through handler.

    The class is designed to process and yield nodes without altering their state. This
    can be helpful in scenarios where nodes need to be logged or monitored without any
    modification. The class inherits from `NodeHandler`.

    Attributes:
        None
    """
    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        """
        Accepts a list of nodes and processes them, yielding each node while logging its acceptance. This function is designed
        to produce a generator for the given nodes after logging their node IDs.

        Args:
            nodes (List[BaseNode]): A list of nodes to be processed. Each node is expected to have a `node_id` attribute
                which will be used for logging.
            **kwargs (Any): Additional arguments that might be used for extended functionality or context, but are not
                required for this function's core behavior.

        Yields:
            BaseNode: Each node from the input list is yielded after being processed (specifically logged in this case).
        """
        for node in nodes:
            try:
                logger.debug(f'Accepted node [node_id: {node.node_id}]')         
                yield node
            except Exception as e:
                logger.error(f'Error while accepting node: {str(e)}')