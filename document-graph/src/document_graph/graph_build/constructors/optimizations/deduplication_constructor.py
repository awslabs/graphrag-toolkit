# Copyright (c) Evan Erwee. All rights reserved.
"""Deduplication constructor — ensures unique graph objects during construction."""

from document_graph.graph_build.constructors.constructors_provider_base import ConstructorProvider
from typing import List, Tuple
import pandas as pd
from document_graph.model_elements import Node, Edge

class DeduplicationConstructor(ConstructorProvider):
    """
    DeduplicationConstructor is responsible for constructing unique graph objects.

    This class provides implementations for the construction of deduplicated nodes
    and edges from the given input data. It ensures that no duplicate graph elements
    are created, facilitating the creation of a cleaner graph structure.

    Attributes
    ----------
    None
    """
    
    def construct(self, data: pd.DataFrame) -> Tuple[List[Node], List[Edge]]:
        """
        Construct a graph representation from the given data.

        This method processes a DataFrame to generate a list of unique nodes and
        edges, suitable for representing a graph structure. Deduplication logic
        for nodes and edges is currently to be implemented.

        Args:
            data (pd.DataFrame): The input data containing information required to
                construct nodes and edges.

        Returns:
            Tuple[List[Node], List[Edge]]: A tuple containing a list of unique nodes
            and a list of unique edges derived from the input data.
        """
        # TODO: Implement deduplication logic for nodes and edges
        nodes = []
        edges = []
        return nodes, edges