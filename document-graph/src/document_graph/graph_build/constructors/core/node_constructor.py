# Copyright (c) Evan Erwee. All rights reserved.
"""Node constructor — converts DataFrame rows into graph nodes."""

from document_graph.graph_build.constructors.constructors_provider_base import ConstructorProvider
from typing import List, Tuple
import pandas as pd
from document_graph.model_elements import Node, Edge

class NodeConstructor(ConstructorProvider):
    """
    Represents a node constructor that converts data into nodes and edges.

    This class is used to construct nodes and edges from the input data provided
    as a DataFrame. It inherits from the `ConstructorProvider` base class and
    implements the `construct` method, which processes the data to generate a
    list of `Node` objects and a list of `Edge` objects.

    Methods
    -------
    construct(data: pd.DataFrame) -> Tuple[List[Node], List[Edge]]
        Constructs and returns the nodes and edges based on the provided DataFrame.
    """
    
    def construct(self, data: pd.DataFrame) -> Tuple[List[Node], List[Edge]]:
        """
        Converts a DataFrame into nodes and edges.

        This method takes a pandas DataFrame, processes its content, and produces a list
        of nodes and edges based on the data. Each row in the DataFrame may represent
        relationships or entities that are used to construct nodes and edges.

        Parameters:
        data : pd.DataFrame
            A pandas DataFrame containing raw data from which nodes and edges are to be
            constructed.

        Returns:
        Tuple[List[Node], List[Edge]]
            A tuple containing two lists:
            - nodes: A list of Node objects created from the DataFrame.
            - edges: A list of Edge objects defining relationships between the nodes.
        """
        # TODO: Implement DataFrame to nodes conversion
        nodes = []
        edges = []
        return nodes, edges