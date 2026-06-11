# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import abc
from typing import Dict, Any

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.core.compat import BaseComponent, BaseNode


class GraphBuilder(BaseComponent):
    """
    Handles the construction and management of graph structures.

    GraphBuilder serves as a base class for implementing components that facilitate the
    creation, indexing, and management of graphs within a specific application context. It
    provides a structure for defining essential methods that subclasses must override to
    handle graph building and indexing functionalities.

    Attributes:
        index_key (str): Represents the unique key or identifier used to distinguish
            the indexing strategy of the graph structure.
    """
    def _to_params(self, p:Dict):
        """
        Converts a given dictionary into a specific parameters structure expected by
        the application or system.

        The function processes the input dictionary and wraps it inside another
        dictionary under the key `'params'`. It ensures consistency in the data
        format for further use or processing.

        Args:
            p (Dict): A dictionary containing the parameters to be converted.

        Returns:
            Dict: A dictionary wrapping the input as a value under the key `'params'`.
        """
        return { 'params': [p] if p else [] }

    @classmethod
    @abc.abstractmethod
    def index_key(cls) -> str:
        """
        Defines an abstract class method to retrieve the index key associated with
        the implementing class. This method must be implemented by all subclasses to
        provide a unique identifier or key for indexing purposes.

        Returns:
            str: A string representing the index key for the class.
        """
        pass

    @abc.abstractmethod
    def build(self, node:BaseNode, graph_client: GraphStore, **kwargs:Any):
        """
        An abstract method designated for building a specific process related to a
        BaseNode within a GraphStore using additional parameters.

        Args:
            node: The node instance of type BaseNode on which the build operation
                is performed.
            graph_client: The graph storage client of type GraphStore responsible
                for managing graph operations.
            **kwargs: Arbitrary additional arguments that may be required for the
                build operation specific to the implementation.
        """
        pass