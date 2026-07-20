# Copyright (c) Evan Erwee. All rights reserved.
"""Constructors provider base — abstract base class for constructor providers."""

import logging
from abc import ABC, abstractmethod
from typing import List, Tuple

import pandas as pd

from document_graph.model_elements import Node, Edge

# Use document-graph logging
logger = logging.getLogger(__name__)

class ConstructorProvider(ABC):
    """
    Provides an interface for constructing graph nodes and edges from data.

    This is an abstract base class designed to standardize the construction
    of graph components (nodes and edges) from a given dataset. By defining
    a common interface, it ensures a consistent approach and validation for
    turning raw data into graph representations. Extend this class and provide
    implementations for the abstract methods to use it effectively.
    """
    
    def __init__(self, config):
        """Initialize constructor with configuration."""
        self.config = config
        self.args = config.args
        logger.debug(f"Initialized {self.__class__.__name__} with config: {config.type}")
    
    @abstractmethod
    def construct(self, data: pd.DataFrame) -> Tuple[List[Node], List[Edge]]:
        """
        Defines an abstract method for constructing graph nodes and edges from a given
        data source. This method is intended to be implemented by subclasses to provide
        specific logic for parsing data and generating corresponding graph structures.

        Parameters:
            data (pd.DataFrame): A pandas DataFrame that serves as the input data
            for constructing the graph structure.

        Returns:
            Tuple[List[Node], List[Edge]]: A tuple containing a list of Node objects
            and a list of Edge objects. These represent the constructed graph's nodes
            and edges respectively.

        Raises:
            None
        """
        pass
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """
        Validates the input DataFrame to ensure it is not empty.

        This method checks whether the provided DataFrame is valid by verifying that
        it is neither None nor empty. It returns a boolean indicating the validity
        of the DataFrame. If the DataFrame is invalid, a warning will be logged.

        Parameters:
        data (pd.DataFrame): The DataFrame to be validated.

        Returns:
        bool: True if the DataFrame is valid (not None and not empty), otherwise False.
        """
        if data is None or data.empty:
            logger.warning(f"{self.__class__.__name__}: Input DataFrame is empty")
            return False
        return True