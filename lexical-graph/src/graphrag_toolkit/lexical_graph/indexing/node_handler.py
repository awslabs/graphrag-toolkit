# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc
from typing import List, Any, Generator
from pydantic import Field
from graphrag_toolkit.core.compat import BaseNode, BaseComponent
from graphrag_toolkit.core.transform import Transform

class NodeHandler(BaseComponent, Transform):
    """
    Handles the processing and transformation of node data.

    This class is designed to process a collection of nodes with optional
    parameters. It serves as a base class for customizable node handling
    operations, requiring the implementation of the `accept` method to
    define specific processing logic. The `__call__` method is provided
    for use as a callable, enabling straightforward invocation of the
    processing logic.

    Attributes:
        show_progress (bool): Whether to show progress during processing.
    """
    show_progress: bool = Field(default=True, description='Whether to show progress.')

    def __call__(self, nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:
        """
        Processes and filters a list of nodes by applying the accept method to each node.

        The method takes a list of BaseNode objects, applies the accept method, and
        returns a new list containing the results.

        Args:
            nodes: A list of BaseNode objects that need to be processed.
            **kwargs: Additional keyword arguments that can be passed to the accept
                method.

        Returns:
            A list of BaseNode objects that have been processed by the accept method.
        """
        return [n for n in self.accept(nodes, **kwargs)]
    
    @abc.abstractmethod
    def accept(self, nodes: List[BaseNode], **kwargs: Any) -> Generator[BaseNode, None, None]:
        """
        Abstract base class for implementing a visitor pattern that can process
        a collection of nodes. This requires subclasses to implement the `accept`
        method to define their processing logic.

        Args:
            nodes: A list of nodes derived from the BaseNode class that are to
                be processed by the visitor pattern.
            **kwargs: Additional keyword arguments that can be passed during the
                processing of the nodes.

        Yields:
            BaseNode: Processed node instances derived from BaseNode, one at
                a time as the generator progresses.
        """
        raise NotImplementedError()