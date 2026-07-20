# Copyright (c) Evan Erwee. All rights reserved.
"""Property constructor — constructs node and edge properties from data."""

from document_graph.graph_build.constructors.constructors_provider_base import ConstructorProvider
from typing import List, Tuple
import pandas as pd
from document_graph.model_elements import Node, Edge

class PropertyConstructor(ConstructorProvider):
    """
    Handles the construction of properties from given data.

    This class is specifically designed to generate properties, represented as nodes
    and edges, from the provided data encapsulated in a DataFrame. It serves as a
    provider that processes this data and triage_constructs meaningful structures that can
    be used in further processing or analysis.
    """
    
    def construct(self, data: pd.DataFrame) -> Tuple[List[Node], List[Edge]]:
        """
        Construct nodes and edges from a given DataFrame.

        This function is intended to map the properties of a DataFrame to a list of
        nodes and edges. The details of the mapping process need to be implemented.
        It will create and return a list of nodes and edges based on the provided
        data structure.

        Args:
            data (pd.DataFrame): The input DataFrame that contains the necessary data
                for constructing nodes and edges.

        Returns:
            Tuple[List[Node], List[Edge]]: A tuple containing a list of nodes and a
                list of edges.
        """
        # TODO: Implement DataFrame columns to properties mapping
        nodes = []
        edges = []
        return nodes, edges