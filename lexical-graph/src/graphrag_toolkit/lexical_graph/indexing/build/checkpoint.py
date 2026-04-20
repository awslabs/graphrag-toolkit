# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from os.path import join
from typing import Any, List

from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

from llama_index.core.schema import TransformComponent, BaseNode

SAVEPOINT_ROOT_DIR = 'save_points'

logger = logging.getLogger(__name__)

class DoNotCheckpoint:
    """Represents a placeholder class with no specific implementation.

    This class serves as a construction placeholder or a basic template
    for defining future classes. It currently does not contain any data
    members or methods, and its primary purpose is to act as a structural
    starter or to suppress checkpointing mechanisms in certain frameworks.
    """
    pass

class CheckpointFilter(TransformComponent, DoNotCheckpoint):
    """
    Manages filtering of nodes based on the absence of a checkpoint.

    This class filters nodes to ensure that only those without existing checkpoints
    in a specified directory are processed by an inner TransformComponent. It combines
    functionality from the TransformComponent and DoNotCheckpoint classes, while allowing
    chaining of transformations and checkpoint-based filtering.

    Attributes:
        checkpoint_name (str): The name of the checkpoint used for filtering.
        checkpoint_dir (str): Path to the directory where checkpoints are stored.
        inner (TransformComponent): The wrapped TransformComponent for processing nodes.
    """
    checkpoint_name:str
    checkpoint_dir:str
    inner:TransformComponent
    tenant_id:TenantId
        
    def checkpoint_does_not_exist(self, node_id):
        """
        Checks whether a checkpoint exists for the given node and determines if the node
        should be included or ignored based on this status.

        This method evaluates the existence of a checkpoint for the specified node
        using the node's identifier. If the checkpoint already exists, the node will
        be ignored; otherwise, it will be included.

        Args:
            node_id: Identifier of the node for which to check the existence of a
                checkpoint.

        Returns:
            bool: Returns False if the checkpoint exists, indicating the node should
                be ignored. Returns True if the checkpoint does not exist,
                indicating the node should be included.
        """
        tenant_node_id = self.tenant_id.rewrite_id(node_id)
        node_checkpoint_path = join(self.checkpoint_dir, tenant_node_id)
        if os.path.exists(node_checkpoint_path):
            logger.debug(f'Ignoring node because checkpoint already exists [node_id: {tenant_node_id}, checkpoint: {self.checkpoint_name}, component: {type(self.inner).__name__}]')
            return False
        else:
            logger.debug(f'Including node [node_id: {tenant_node_id}, checkpoint: {self.checkpoint_name}, component: {type(self.inner).__name__}]')
            return True
        
    def __call__(self, nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:
        """
        Filters nodes based on specific criteria and forwards the filtered list to an inner callable.

        This method processes a list of nodes, removing nodes that satisfy a particular condition
        (checkpoint existence). The filtered list is then passed to another callable for further
        processing.

        Args:
            nodes: A list of BaseNode objects to be filtered.
            **kwargs: Additional keyword arguments to be passed to the inner callable.

        Returns:
            A list of BaseNode objects that have been filtered and processed by the inner callable.
        """
        discarded_count = 0
        filtered_nodes = []
        
        for node in nodes:
            if self.checkpoint_does_not_exist(node.id_):
                filtered_nodes.append(node)
            else:
                discarded_count += 1
        
        if discarded_count > 0:
            logger.info(f'[{type(self.inner).__name__}] Discarded {discarded_count} out of {discarded_count + len(filtered_nodes)} nodes because they have already been checkpointed')

        return self.inner.__call__(filtered_nodes, **kwargs)

    
class CheckpointWriter(NodeHandler):

    checkpoint_name:str
    checkpoint_dir:str
    inner:NodeHandler

    def touch(self, path):
        """
        Creates an empty file or updates the modification and access times
        of the specified file. If the file does not exist, it is created.

        Args:
            path (str): The path of the file to be created or modified.
        """
        with open(path, 'a'):
            os.utime(path, None)
    
    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        """
        Processes and categorizes nodes as checkpointable or non-checkpointable based on metadata,
        performing checkpoint-related operations for applicable nodes.

        Args:
            nodes (List[BaseNode]): A list of nodes to be processed. Each node contains a unique
                identifier and associated metadata that determines whether it is checkpointable.
            **kwargs (Any): Additional keyword arguments to be passed to the inner accept method.

        Yields:
            BaseNode: Nodes that have been processed and classified. Each node is yielded
                after logging and performing checkpoint-related operations if applicable.
        """
        for node in self.inner.accept(nodes, **kwargs):
            node_id = node.node_id
            if [key for key in [INDEX_KEY] if key in node.metadata]:
                logger.debug(f'Non-checkpointable node [checkpoint: {self.checkpoint_name}, node_id: {node_id}, component: {type(self.inner).__name__}]') 
            else:
                logger.debug(f'Checkpointable node [checkpoint: {self.checkpoint_name}, node_id: {node_id}, component: {type(self.inner).__name__}]') 
                node_checkpoint_path = join(self.checkpoint_dir, node_id)
                self.touch(node_checkpoint_path)
            yield node

class Checkpoint():
    """
    Creates and manages checkpoints for data processing components.

    This class is used to wrap certain components with checkpointing functionality.
    Checkpoints allow intermediate states or results of data processing components to
    be saved and restored. It can optionally enable or disable checkpointing, and
    ensures the necessary directory structure is prepared for storing checkpoint files.

    Attributes:
        checkpoint_name (str): The name of the checkpoint.
        checkpoint_dir (str): The directory where checkpoint files are stored.
        enabled (bool): Indicates whether the checkpointing functionality is enabled.
    """
    def __init__(self, checkpoint_name, output_dir=None, enabled=True):
        """
        Initializes an instance with the specified checkpoint name, output directory, and enabled status.

        Args:
            checkpoint_name: Name of the checkpoint to be used.
            output_dir: Output directory where the results are saved. Defaults to GraphRAGConfig.local_output_dir.
            enabled: A boolean flag indicating if the instance is enabled. Defaults to True.
        """
        from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        resolved_output_dir = output_dir if output_dir is not None else GraphRAGConfig.local_output_dir
        self.checkpoint_name = checkpoint_name
        self.checkpoint_dir = self.prepare_output_directories(checkpoint_name, resolved_output_dir)
        self.enabled = enabled

    def add_filter(self, o, tenant_id:TenantId):
        """
        Adds a checkpoint filter to a transform component if conditions are met.

        This method wraps the provided transform component (`o`) with a checkpoint
        filter if the component satisfies the specified conditions. Specifically, the
        checkpoint filter is applied if the instance is enabled, the provided object
        is of type `TransformComponent`, and is not of type `DoNotCheckpoint`.
        Otherwise, the method returns the component unchanged.

        Args:
            o: The object to potentially wrap with a checkpoint filter.

        Returns:
            The original object or a `CheckpointFilter` wrapping the input object
            depending on the specified conditions.
        """
        if self.enabled and isinstance(o, TransformComponent) and not isinstance(o, DoNotCheckpoint):
            logger.debug(f'Wrapping with checkpoint filter [checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]')
            return CheckpointFilter(inner=o, checkpoint_dir=self.checkpoint_dir, checkpoint_name=self.checkpoint_name, tenant_id=tenant_id)
        else:
            logger.debug(f'Not wrapping with checkpoint filter [checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]')
            return o
        
    def add_writer(self, o):
        """
        Adds a checkpoint writer wrapper to the provided object if enabled and if the object is an instance of NodeHandler.
        This function allows integration of checkpoint writing capability into a component when applicable.
        If the wrapping is not performed, the provided object is returned unmodified.

        Args:
            o: The object to potentially wrap with a checkpoint writer. Typically expected to be an instance
                of NodeHandler.

        Returns:
            The wrapped object with checkpoint writing capability, or the original object if wrapping is not
            applicable based on the specified conditions.
        """
        if self.enabled and isinstance(o, NodeHandler):
            logger.debug(f'Wrapping with checkpoint writer [checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]')
            return CheckpointWriter(inner=o, checkpoint_dir=self.checkpoint_dir, checkpoint_name=self.checkpoint_name)
        else:
            logger.debug(f'Not wrapping with checkpoint writer [checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]')
            return o

    def prepare_output_directories(self, checkpoint_name, output_dir):
        """
        Prepares the output directories for saving checkpoints.

        This function creates a specific directory structure based on the provided
        checkpoint name and output directory. The directories are necessary for
        saving and organizing checkpoint files during the program's execution. If the
        designated checkpoint directory does not already exist, it is created.

        Args:
            checkpoint_name: Specifies the name of the checkpoint. This name is used
                to identify and organize directories.
            output_dir: Specifies the root output directory where the checkpoint
                subdirectory will be created.

        Returns:
            The complete path of the prepared checkpoint directory.
        """
        checkpoint_dir = join(output_dir, SAVEPOINT_ROOT_DIR, checkpoint_name)
        
        logger.debug(f'Preparing checkpoint directory [checkpoint: {checkpoint_name}, checkpoint_dir: {checkpoint_dir}]')

        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
   
        return checkpoint_dir

    
