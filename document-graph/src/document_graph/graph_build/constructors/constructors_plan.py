# Copyright (c) Evan Erwee. All rights reserved.
"""Constructors plan — orchestrates execution of constructor providers on data."""

import logging
from typing import List, Tuple
import pandas as pd
from document_graph.graph_build.constructors.constructors_provider_config import ConstructorProviderConfig
from document_graph.graph_build.constructors.constructors_provider_factory import ConstructorProviderFactory
from document_graph.model_elements import Node, Edge

# Use document-graph logging
logger = logging.getLogger(__name__)

class ConstructorPlan:
    """
    Manages a construction plan for executing multiple constructor providers.

    The `ConstructorPlan` class is responsible for handling a list of constructor
    provider configurations. It executes all constructors in sequence, processes
    an input DataFrame, and combines the resulting nodes and edges. Appropriate
    logging and error handling are applied throughout the execution of the
    constructor pipeline.
    """
    
    def __init__(self, configs: List[ConstructorProviderConfig]):
        """
        Initializes an instance of the ConstructorPlan with a given list of constructor
        configurations. Logs the number of configurations provided during
        initialization.

        Attributes:
            configs: List of ConstructorProviderConfig
                A list of configuration objects provided for the ConstructorPlan
                instance.

        Parameters:
            configs: List[ConstructorProviderConfig]
                List of constructor configuration objects passed during the
                initialization of the class.
        """
        self.configs = configs
        logger.info(f"ConstructorPlan initialized with {len(configs)} constructors")
    
    def execute(self, data: pd.DataFrame) -> Tuple[List[Node], List[Edge]]:
        """
        Executes a pipeline of constructors to generate nodes and edges from a given data.

        This method processes an input DataFrame and iteratively applies a series of constructors
        defined in the instance configuration. Each constructor generates a set of nodes and edges
        based on its specific implementation logic and the provided data. The pipeline continues
        even if individual constructors fail, logging warnings or errors at each step. At the end
        of the pipeline, the method aggregates all the nodes and edges created by the constructors
        and returns them as a tuple.

        Raises:
            Logger warnings and errors for an empty DataFrame or for failures during the execution
            of individual constructors.

        Parameters:
            data (pd.DataFrame): The input data to be processed.

        Returns:
            Tuple[List[Node], List[Edge]]: A tuple containing two lists:
                - A list of all nodes created by the constructors in the pipeline.
                - A list of all edges created by the constructors in the pipeline.
        """
        if data.empty:
            logger.warning("Input DataFrame is empty")
            return [], []
        
        all_nodes = []
        all_edges = []
        
        logger.info(f"Starting constructor pipeline: {len(data)} rows")
        
        for i, config in enumerate(self.configs):
            try:
                logger.info(f"Executing constructor {i+1}/{len(self.configs)}: {config.name} ({config.type})")
                
                constructor = ConstructorProviderFactory.create(config)
                nodes, edges = constructor.construct(data)
                
                all_nodes.extend(nodes)
                all_edges.extend(edges)
                
                logger.info(f"  Completed: +{len(nodes)} nodes, +{len(edges)} edges")
                
            except Exception as e:
                logger.error(f"Constructor {config.name} failed: {e}")
                # Continue with other constructors
        
        logger.info(f"Constructor pipeline completed: {len(all_nodes)} total nodes, {len(all_edges)} total edges")
        return all_nodes, all_edges