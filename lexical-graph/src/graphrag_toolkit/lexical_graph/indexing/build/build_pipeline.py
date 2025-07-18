# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import multiprocessing
import math
from typing import Any, List, Optional, Iterable
from pipe import Pipe

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.metadata import SourceMetadataFormatter, DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.indexing import NodeHandler, IdGenerator
from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import run_pipeline
from graphrag_toolkit.lexical_graph.indexing.model import SourceType, SourceDocument, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import Checkpoint, CheckpointWriter
from graphrag_toolkit.lexical_graph.indexing.build.node_builders import NodeBuilders
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters

from llama_index.core.utils import iter_batch
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import TransformComponent, BaseNode

logger = logging.getLogger(__name__)

class NodeFilter(TransformComponent):
      
    def __call__(self, nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:
        return nodes

class BuildPipeline():
    """
    Represents a configurable build pipeline designed for efficiently processing and transforming
    data into nodes through various components, while supporting batching, parallelization,
    and customizations.

    The BuildPipeline class provides a mechanism to process collections of input data using a
    series of transformation components. It supports batch processing, parallel execution with
    a configurable number of workers, and optional input/output customization through builders
    and filters. It is tailored for scalable processing needs, such as in data ingestion frameworks,
    where transformations and filtering of input data are required. The pipeline manages pipeline
    components, applies node filtering and building logic, and executes transformations in parallel
    to produce output nodes.

    Attributes:
        inner_pipeline (IngestionPipeline): The internal ingestion pipeline used for processing
        the transformation components.
        num_workers (int): Number of parallel workers for processing the data. Defaults to the
        system's CPU count if not provided.
        batch_size (int): Size of batches for processing data. Defaults to a configured batch size.
        batch_writes_enabled (bool): Flag indicating whether batch writes are enabled.
        batch_write_size (int): Size of batches for processing writes. Defaults to a configured size.
        include_domain_labels (bool): Flag indicating whether domain labels should be included.
        node_builders (NodeBuilders): Object that encapsulates the logic for building nodes,
        applying filters, and formatting metadata.
        node_filter (NodeFilter): Filter used for excluding or including nodes based on certain conditions.
        pipeline_kwargs (dict): Additional keyword arguments passed to the pipeline.
    """
    @staticmethod
    def create(components: List[TransformComponent], 
               num_workers:Optional[int]=None, 
               batch_size:Optional[int]=None, 
               batch_writes_enabled:Optional[bool]=None, 
               batch_write_size:Optional[int]=None, 
               builders:Optional[List[NodeBuilder]]=[], 
               show_progress=False, 
               checkpoint:Optional[Checkpoint]=None,
               build_filters:Optional[BuildFilters]=None,
               source_metadata_formatter:Optional[SourceMetadataFormatter]=None,
               include_domain_labels:Optional[bool]=None,
               include_local_entities:Optional[bool]=None,
               tenant_id:Optional[TenantId]=None,
               **kwargs:Any
            ):
        """
        Creates and initializes a `Pipe` object configured with the provided parameters and
        components through the `BuildPipeline`. This method facilitates seamless aggregation
        of processing components into a pipeline, enabling execution with specific settings
        such as concurrency, batching, filtering, and more.

        Args:
            components (List[TransformComponent]): List of transformation components to be
            included in the pipeline.
            num_workers (Optional[int]): Number of worker threads or processes for parallel
            execution of the pipeline. Defaults to None.
            batch_size (Optional[int]): Size of data batches to be processed. Defaults to None.
            batch_writes_enabled (Optional[bool]): Toggles batching for writes during the
            processing. Defaults to None.
            batch_write_size (Optional[int]): Size of data batches for write operations,
            applicable if batching is enabled. Defaults to None.
            builders (Optional[List[NodeBuilder]]): Optional list of node builders if specific
            building structures are required. Defaults to an empty list.
            show_progress (bool): Flag indicating whether to show progress during processing.
            Defaults to False.
            checkpoint (Optional[Checkpoint]): Optional checkpoint configuration to enable
            resumption from a specific state. Defaults to None.
            build_filters (Optional[BuildFilters]): Filters applied during the build process
            to refine data processing. Defaults to None.
            source_metadata_formatter (Optional[SourceMetadataFormatter]): Formatter for source
            metadata to customize metadata configuration. Defaults to None.
            include_domain_labels (Optional[bool]): Specifies whether domain labels should be
            incorporated in the output. Defaults to None.
            include_local_entities (Optional[bool]): Specifies whether local
            entities are included in the graph. Defaults to None.
            tenant_id (Optional[TenantId]): Identifier for tenant-specific operations or
            segregations. Defaults to None.
            **kwargs (Any): Additional keyword arguments to customize further configuration
                of the pipeline.

        Returns:
            Pipe: A configured `Pipe` instance encapsulating the constructed pipeline for
            execution.
        """
        return Pipe(
            BuildPipeline(
                components=components,
                num_workers=num_workers,
                batch_size=batch_size,
                batch_writes_enabled=batch_writes_enabled,
                batch_write_size=batch_write_size,
                builders=builders,
                show_progress=show_progress,
                checkpoint=checkpoint,
                build_filters=build_filters,
                source_metadata_formatter=source_metadata_formatter,
                include_domain_labels=include_domain_labels,
                include_local_entities=include_local_entities,
                tenant_id=tenant_id,
                **kwargs
            ).build
        )
    
    def __init__(self, 
                 components: List[TransformComponent], 
                 num_workers:Optional[int]=None, 
                 batch_size:Optional[int]=None, 
                 batch_writes_enabled:Optional[bool]=None, 
                 batch_write_size:Optional[int]=None, 
                 builders:Optional[List[NodeBuilder]]=[], 
                 show_progress=False, 
                 checkpoint:Optional[Checkpoint]=None,
                 build_filters:Optional[BuildFilters]=None,
                 source_metadata_formatter:Optional[SourceMetadataFormatter]=None,
                 include_domain_labels:Optional[bool]=None,
                 include_local_entities:Optional[bool]=None,
                 tenant_id:Optional[TenantId]=None,
                 **kwargs:Any
            ):
        """
        Initializes an instance of a class responsible for configuring and managing a data
        processing pipeline. This pipeline processes input through a customizable sequence
        of transformation components, while optionally supporting checkpointing, multiprocessing,
        and domain-specific metadata.

        Args:
            components (List[TransformComponent]): A list of transformation components that
            collectively define the pipeline workflow. Defaults to an empty list if None.
            num_workers (Optional[int]): The number of worker processes for parallel execution.
            Defaults to the system's processor count or a preconfigured value.
            batch_size (Optional[int]): The size of data batches to process in each step of the
            pipeline. Defaults to a preconfigured value.
            batch_writes_enabled (Optional[bool]): Indicates whether batch writes are enabled
            during processing. Defaults to a preconfigured value.
            batch_write_size (Optional[int]): Specifies the number of items to include in each
            batch write operation. Defaults to a preconfigured value.
            builders (Optional[List[NodeBuilder]]): A list of node builders used for node creation
            in the pipeline. Defaults to an empty list.
            show_progress (bool): Whether to display progress updates during processing.
            Defaults to False.
            checkpoint (Optional[Checkpoint]): An object for managing checkpoint operations,
            enabling the pipeline to resume from a specific state. Defaults to None.
            build_filters (Optional[BuildFilters]): Filters applied during node building,
            restricting or modifying node creation. Defaults to None.
            source_metadata_formatter (Optional[SourceMetadataFormatter]): A formatter for metadata
            associated with nodes, enabling customization or domain-specific adjustments.
            Defaults to a `DefaultSourceMetadataFormatter` instance.
            include_domain_labels (Optional[bool]): Indicates whether domain labels should be
            included in the output during processing. Defaults to a preconfigured value.
            include_local_entities (Optional[bool]): Specifies whether local
            entities are included in the graph. Defaults to a preconfigured value.
            tenant_id (Optional[TenantId]): An identifier for the tenant, used for scoping data.
            Defaults to None.
            **kwargs (Any): Additional keyword arguments to configure the pipeline behavior.
        """
        components = components or []
        num_workers = num_workers or GraphRAGConfig.build_num_workers
        batch_size = batch_size or GraphRAGConfig.build_batch_size
        batch_writes_enabled = batch_writes_enabled or GraphRAGConfig.batch_writes_enabled
        batch_write_size = batch_write_size or GraphRAGConfig.build_batch_write_size
        include_domain_labels = include_domain_labels or GraphRAGConfig.include_domain_labels
        include_local_entities = include_local_entities or GraphRAGConfig.include_local_entities
        source_metadata_formatter = source_metadata_formatter or DefaultSourceMetadataFormatter()
        
        for c in components:
            if isinstance(c, NodeHandler):
                c.show_progress = show_progress

        if num_workers > multiprocessing.cpu_count():
            num_workers = multiprocessing.cpu_count()
            logger.debug(f'Setting num_workers to CPU count [num_workers: {num_workers}]')

        if checkpoint and components:
            
            l = len(components)
            for i, c in enumerate(reversed(components)):
                updated_component = checkpoint.add_writer(c)
                if isinstance(updated_component, CheckpointWriter):
                    components[l-i-1] = updated_component
                    break

        logger.debug(f'Build pipeline components: {[type(c).__name__ for c in components]}')

        self.inner_pipeline=IngestionPipeline(transformations=components, disable_cache=True)
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.batch_writes_enabled = batch_writes_enabled
        self.batch_write_size = batch_write_size
        self.include_domain_labels = include_domain_labels
        self.include_local_entities = include_local_entities
        self.node_builders = NodeBuilders(
            builders=builders, 
            build_filters=build_filters, 
            source_metadata_formatter=source_metadata_formatter, 
            id_generator=IdGenerator(tenant_id=tenant_id)
        )
        self.node_filter = NodeFilter() if not checkpoint else checkpoint.add_filter(NodeFilter())
        self.pipeline_kwargs = kwargs
    
    def _to_node_batches(self, source_doc_batches:Iterable[Iterable[SourceDocument]]) -> List[List[BaseNode]]:
        """
        Converts batches of source documents into batches of nodes based on filtering and
        builder processes. Each batch of source documents is processed individually to form
        a batch of nodes.

        Args:
            source_doc_batches: A collection of collections, where each inner collection
            contains `SourceDocument` instances to be processed.

        Returns:
            A list of lists, where each inner list contains `BaseNode` objects that
            represent the processed and filtered nodes derived from the input source
            documents.
        """
        results = []
    
        for source_documents in source_doc_batches:
        
            chunk_node_batches = [
                self.node_filter(source_document.nodes)
                for source_document in source_documents
            ]

            node_batches = [
                self.node_builders(chunk_nodes) 
                for chunk_nodes in chunk_node_batches if chunk_nodes
            ]

            nodes = [
                node
                for nodes in node_batches
                for node in nodes
            ]   
        
            results.append(nodes)

        return results

    def build(self, inputs: Iterable[SourceType]):
        """
        Processes a set of input source types to generate and yield output nodes through a
        configurable build pipeline. The method organizes source documents into batches
        and processes them through a pipeline with customizable settings for parallel
        execution, batching, and additional pipeline-specific options.

        Args:
            inputs: An iterable of SourceType objects representing source data to be
            processed into nodes.

        Yields:
            BaseNode: Yields nodes as output from the processing pipeline, after being
            transformed and processed according to the pipeline steps.
        """
        input_source_documents = source_documents_from_source_types(inputs)

        for source_documents in iter_batch(input_source_documents, self.batch_size):

            num_source_docs_per_batch = math.ceil(len(source_documents)/self.num_workers)
            source_doc_batches = iter_batch(source_documents, num_source_docs_per_batch)
            
            node_batches:List[List[BaseNode]] = self._to_node_batches(source_doc_batches)

            logger.info(f'Running build pipeline [batch_size: {self.batch_size}, num_workers: {self.num_workers}, job_sizes: {[len(b) for b in node_batches]}, batch_writes_enabled: {self.batch_writes_enabled}, batch_write_size: {self.batch_write_size}]')

            output_nodes = run_pipeline(
                self.inner_pipeline,
                node_batches,
                num_workers=self.num_workers,
                batch_writes_enabled=self.batch_writes_enabled,
                batch_size=self.batch_size,
                batch_write_size=self.batch_write_size,
                include_domain_labels=self.include_domain_labels,
                include_local_entities=self.include_local_entities,
                **self.pipeline_kwargs
            )

            for node in output_nodes:
                yield node       

