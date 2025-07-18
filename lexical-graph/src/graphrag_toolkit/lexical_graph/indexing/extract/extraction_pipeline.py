# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from pipe import Pipe
from typing import List, Optional, Sequence, Dict, Iterable, Any

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import run_pipeline, node_batcher
from graphrag_toolkit.lexical_graph.indexing.model import SourceType, SourceDocument, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_decorator import PipelineDecorator
from graphrag_toolkit.lexical_graph.indexing.extract.source_doc_parser import SourceDocParser
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import Checkpoint
from graphrag_toolkit.lexical_graph.indexing.extract.docs_to_nodes import DocsToNodes
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter

from llama_index.core.node_parser import TextSplitter
from llama_index.core.utils import iter_batch
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.schema import TransformComponent
from llama_index.core.schema import BaseNode, Document
from llama_index.core.schema import NodeRelationship

logger = logging.getLogger(__name__)
    
class PassThroughDecorator(PipelineDecorator):
    """
    A decorator class that passes through input and output documents unchanged.

    This class is intended to be used as a no-op (no operation) decorator within
    a pipeline. It receives documents, performs no modifications, and forwards
    them as-is to the next component in the pipeline. The purpose of this class
    is to act as a placeholder or default decorator which enables seamless
    integration and testing of pipelines without altering the data flow.

    Attributes:
        None
    """
    def __init__(self):
        pass
    
    def handle_input_docs(self, nodes:Iterable[SourceDocument]):
        """
        Handles and processes the given input documents.

        This method takes an iterable of SourceDocument instances and processes them
        as required. The processed documents are then returned.

        Args:
            nodes (Iterable[SourceDocument]): An iterable containing SourceDocument
                instances to be handled.

        Returns:
            Iterable[SourceDocument]: The processed iterable of SourceDocument
                instances.
        """
        return nodes
    
    def handle_output_doc(self, node: SourceDocument) -> SourceDocument:
        """
        Handles the processing of a SourceDocument node and returns it after output handling.

        The method takes a single SourceDocument object as input, processes it, and returns
        the same SourceDocument object. It can be utilized to apply specific output-related
        handling or modifications to the input document.

        Args:
            node (SourceDocument): The document to be handled and returned after processing.

        Returns:
            SourceDocument: The processed document after output handling.
        """
        return node


class ExtractionPipeline():
    """Represents a data extraction pipeline with customizable components.

    This class defines a pipeline for processing and extracting data using a series
    of configurable components. The pipeline allows for the use of pre-processors,
    decorators, and post-processing logic to handle the data extraction workflow
    in a modular and scalable way. Additionally, it supports multi-worker execution,
    batching, and integration with filters and checkpoints for state management.

    Attributes:
        ingestion_pipeline (IngestionPipeline): The pipeline of components to transform
            input data.
        pre_processors (List[SourceDocParser]): Pre-processors used to parse input source
            documents before starting the extraction process.
        extraction_decorator (PipelineDecorator): A decorator for handling additional
            input and output transformations in the extraction pipeline.
        num_workers (int): The number of workers used for parallel processing in the
            pipeline.
        batch_size (int): The size of data batches for processing in the pipeline.
        show_progress (bool): Determines whether progress should be logged or displayed
            during pipeline execution.
        id_rewriter (IdRewriter): A component responsible for rewriting node identifiers
            within the extraction pipeline.
        extraction_filters (FilterConfig): Filters applied to input data nodes to
            determine which nodes are processed by the pipeline.
        pipeline_kwargs (dict): Additional runtime parameters and configurations for
            the pipeline components.
    """
    @staticmethod
    def create(components: List[TransformComponent], 
               pre_processors:Optional[List[SourceDocParser]]=None,
               extraction_decorator:PipelineDecorator=None, 
               num_workers=None, 
               batch_size=None, 
               show_progress=False, 
               checkpoint:Optional[Checkpoint]=None,
               tenant_id:Optional[TenantId]=None,
               extraction_filters:Optional[FilterConfig]=None,
               **kwargs:Any):
        """
        Creates an instance of the extraction pipeline, configured with specified components,
        optional pre-processors, decorators, filters, and other settings. This method returns
        a pipeline configured to extract data from source documents using the specified
        settings and options.

        This static method streamlines the process of constructing a pipeline by enabling
        customization through its arguments, including batching, progress visibility, tenant
        filtering, checkpointing, and additional behaviors via keyword arguments.

        Args:
            components (List[TransformComponent]): A list of components for the
                transformation pipeline.
            pre_processors (Optional[List[SourceDocParser]]): Optional list of pre-processors
                to apply on the source documents.
            extraction_decorator (PipelineDecorator): An optional decorator for customizing
                the extraction process.
            num_workers (Optional[int]): The number of workers to use for parallel processing.
            batch_size (Optional[int]): The number of items to process in each batch during
                execution.
            show_progress (bool): Specifies whether to display progress during the pipeline
                execution.
            checkpoint (Optional[Checkpoint]): An optional checkpoint configuration for managing
                pipeline state and recovery.
            tenant_id (Optional[TenantId]): An optional identifier to constrain data processing
                to a specific tenant.
            extraction_filters (Optional[FilterConfig]): An optional set of filters to apply for
                extraction rules.
            **kwargs (Any): Additional settings or configurations for extending the pipeline's
                behavior.

        Returns:
            Pipe: A configured pipeline object wrapping the extraction pipeline's extraction
            logic.
        """
        return Pipe(
            ExtractionPipeline(
                components=components, 
                pre_processors=pre_processors,
                extraction_decorator=extraction_decorator,
                num_workers=num_workers,
                batch_size=batch_size,
                show_progress=show_progress,
                checkpoint=checkpoint,
                tenant_id=tenant_id,
                extraction_filters=extraction_filters,
                **kwargs
            ).extract
        )
    
    def __init__(self, 
                 components: List[TransformComponent], 
                 pre_processors:Optional[List[SourceDocParser]]=None,
                 extraction_decorator:PipelineDecorator=None, 
                 num_workers=None, 
                 batch_size=None, 
                 show_progress=False, 
                 checkpoint:Optional[Checkpoint]=None,
                 tenant_id:Optional[TenantId]=None,
                 extraction_filters:Optional[FilterConfig]=None,
                 **kwargs:Any):
        """
        Initializes the extraction pipeline with provided components, configurations, and optional
        pre-processing and filtering capabilities to handle document processing tasks. This class
        configures the pipeline with a series of components, manages their interactions, and sets up
        any necessary decorators for additional functionality.

        Args:
            components (List[TransformComponent]): A list of transformation components that constitute
                the extraction pipeline, which will process and transform the data.
            pre_processors (Optional[List[SourceDocParser]]): Optional list of pre-processors to parse
                source documents before they are ingested into the pipeline. Defaults to None.
            extraction_decorator (PipelineDecorator): Optional pipeline decorator for modifying or
                enhancing the extraction process. Defaults to PassThroughDecorator if not provided.
            num_workers (Optional[int]): Number of workers to parallelize the pipeline's processing.
                Defaults to the predefined configuration value.
            batch_size (Optional[int]): Batch size to determine how many items are processed concurrently.
                Defaults to the predefined configuration value.
            show_progress (bool): Flag to enable or disable progress visualization during pipeline execution.
                Defaults to False.
            checkpoint (Optional[Checkpoint]): Optional checkpoint to integrate filtering or additional
                processing into the pipeline. Defaults to None.
            tenant_id (Optional[TenantId]): Identifies the tenant for generating unique IDs within the
                pipeline's transformations. Defaults to None.
            extraction_filters (Optional[FilterConfig]): Configuration for additional filtering criteria
                to apply during the extraction process. Defaults to an empty FilterConfig instance.
            **kwargs (Any): Additional keyword arguments to be passed to the pipeline configurations.

        Attributes:
            ingestion_pipeline (IngestionPipeline): The constructed pipeline consisting of all configured
                transformation components, responsible for processing the ingested documents.
            pre_processors (List[SourceDocParser]): A list of pre-processors, if provided, for initial
                parsing of source documents.
            extraction_decorator (PipelineDecorator): The decorator used to modify or extend the extraction
                logic as part of the pipeline execution.
            num_workers (int): The number of workers allocated for parallel processing in the pipeline.
            batch_size (int): The size of processing batches for the pipeline's operations.
            show_progress (bool): Indicates whether progress visualization is enabled during pipeline
                execution.
            id_rewriter (IdRewriter): A rewriter for generating unique IDs to ensure document traceability
                within the pipeline.
            extraction_filters (FilterConfig): Holds the configuration for additional filters to refine
                the data extraction process.
            pipeline_kwargs (dict): Captures any additional pipeline configuration settings provided
                via keyword arguments.
        """
        components = components or []
        num_workers = num_workers or GraphRAGConfig.extraction_num_workers
        batch_size = batch_size or GraphRAGConfig.extraction_batch_size

        for c in components:
            if isinstance(c, BaseExtractor):
                c.show_progress = show_progress

        def add_id_rewriter(c):
            """
            Pipeline that processes input data through multiple transformation components and optional
            pre-processing steps. It supports concurrency and batching to improve efficiency. An optional
            decorator can be applied to the extraction process, and advanced configurations such as filters
            or checkpoints can be provided.

            Args:
                components (List[TransformComponent]): A list of transformation components that process
                    the data in a sequence.
                pre_processors (Optional[List[SourceDocParser]]): A list of pre-processing components to
                    parse and prepare the input data before it enters the transformation pipeline.
                extraction_decorator (PipelineDecorator): An optional decorator applied to the extraction
                    process for extra functionality.
                num_workers (Optional[int]): The number of worker threads used for processing. If not
                    provided, processing is done synchronously.
                batch_size (Optional[int]): The size of the batches processed at a time. Determines how
                    input data is split and processed.
                show_progress (bool): If True, displays a progress indicator during processing. Defaults to
                    False.
                checkpoint (Optional[Checkpoint]): Optional checkpoint configuration to manage or resume
                    processing from a specific point.
                tenant_id (Optional[TenantId]): An optional identifier for associating the pipeline
                    components with a specific tenant context.
                extraction_filters (Optional[FilterConfig]): Optional configuration specifying filters to
                    apply during the extraction process.
                **kwargs (Any): Additional parameters for further customization of the pipeline or its
                    methods.
            """
            if isinstance(c, TextSplitter):
                logger.debug(f'Wrapping {type(c).__name__} with IdRewriter')
                return IdRewriter(inner=c, id_generator=IdGenerator(tenant_id=tenant_id))
            else:
                return c
            
        components = [add_id_rewriter(c) for c in components]
        
        if not any([isinstance(c, IdRewriter) for c in components]):
            logger.debug(f'Adding DocToNodes to components')
            components.insert(0, IdRewriter(inner=DocsToNodes(), id_generator=IdGenerator(tenant_id=tenant_id)))
            
        if checkpoint:
            components = [checkpoint.add_filter(c) for c in components]

        logger.debug(f'Extract pipeline components: {[type(c).__name__ for c in components]}')

        self.ingestion_pipeline = IngestionPipeline(transformations=components)
        self.pre_processors = pre_processors or []
        self.extraction_decorator = extraction_decorator or PassThroughDecorator()
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.show_progress = show_progress
        self.id_rewriter = IdRewriter(id_generator=IdGenerator(tenant_id=tenant_id))
        self.extraction_filters = extraction_filters or FilterConfig()
        self.pipeline_kwargs = kwargs
    
    def _source_documents_from_base_nodes(self, nodes:Sequence[BaseNode]) -> List[SourceDocument]:
        """
        Converts a sequence of BaseNode objects into a list of SourceDocument objects
        organized by their source relationships.

        This method iterates through the provided nodes, groups them by their associated
        source IDs as indicated by their relationships, and returns a list of
        SourceDocument objects, each containing the grouped nodes.

        Args:
            nodes (Sequence[BaseNode]): A sequence of BaseNode objects to be processed into
                SourceDocument objects.

        Returns:
            List[SourceDocument]: A list of SourceDocument objects, each containing nodes
                grouped by their source relationship.
        """
        results:Dict[str, SourceDocument] = {}
        
        for node in nodes:
            source_info = node.relationships[NodeRelationship.SOURCE]
            source_id = source_info.node_id
            if source_id not in results:
                results[source_id] = SourceDocument()
            results[source_id].nodes.append(node)

        return list(results.values())
    
    def extract(self, inputs: Iterable[SourceType]):
        """
        Extracts data from a given input source using multiple processing stages.

        This method processes a collection of input source types to extract
        relevant data by applying a series of pre-processing stages and
        running an ingestion pipeline for extraction. The process includes
        filtering metadata, handling document batches, and decorating the
        extracted output source documents.

        Args:
            inputs (Iterable[SourceType]): An iterable of input source types
                to be processed by the extraction pipeline.

        Yields:
            SourceDocument: Processed and extracted source documents after
                being handled by the extraction pipeline and decorators.
        """
        def get_source_metadata(node):
            if isinstance(node, Document):
                return node.metadata
            else:
                return node.relationships[NodeRelationship.SOURCE].metadata

        input_source_documents = source_documents_from_source_types(inputs)

        for source_documents in iter_batch(input_source_documents, self.batch_size):

            for pre_processor in self.pre_processors:
                source_documents = pre_processor.parse_source_docs(source_documents)

            source_documents = self.id_rewriter.handle_source_docs(source_documents)
            source_documents = self.extraction_decorator.handle_input_docs(source_documents)

            input_nodes = [
                n
                for sd in source_documents
                for n in sd.nodes
            ]
            
            filtered_input_nodes = [
                node 
                for node in input_nodes 
                if self.extraction_filters.filter_source_metadata_dictionary(get_source_metadata(node)) 
            ]

            logger.info(f'Running extraction pipeline [batch_size: {self.batch_size}, num_workers: {self.num_workers}]')
            
            node_batches = node_batcher(
                num_batches=self.num_workers, 
                nodes=filtered_input_nodes
            )
                        
            output_nodes = run_pipeline(
                self.ingestion_pipeline,
                node_batches,
                num_workers=self.num_workers,
                **self.pipeline_kwargs
            )
  
            output_source_documents = self._source_documents_from_base_nodes(output_nodes)
            
            for source_document in output_source_documents:
                yield self.extraction_decorator.handle_output_doc(source_document)

    
   