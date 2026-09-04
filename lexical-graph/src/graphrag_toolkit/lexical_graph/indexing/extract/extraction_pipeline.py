# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import math
import multiprocessing
import time
from pipe import Pipe
from typing import List, Optional, Sequence, Generator, Iterable, Any

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.versioning import EXTRACT_TIMESTAMP
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import run_pipeline, node_batcher
from graphrag_toolkit.lexical_graph.indexing.model import SourceType, SourceDocument, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_decorator import PipelineDecorator
from graphrag_toolkit.lexical_graph.indexing.extract.source_doc_parser import SourceDocParser
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import Checkpoint, CheckpointFilter
from graphrag_toolkit.lexical_graph.indexing.extract.docs_to_nodes import DocsToNodes
from graphrag_toolkit.lexical_graph.indexing.extract.id_rewriter import IdRewriter
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
from graphrag_toolkit.lexical_graph.indexing.extract.bucket_filler import BucketFiller
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import BEDROCK_MIN_BATCH_SIZE, BEDROCK_MAX_BATCH_SIZE
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

from llama_index.core.node_parser import NodeParser
from llama_index.core.utils import iter_batch
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.ingestion.pipeline import run_transformations
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
               include_classification_in_entity_id:Optional[bool]=None,
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
                include_classification_in_entity_id=include_classification_in_entity_id,
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
                 include_classification_in_entity_id:Optional[bool]=None,
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
        # Capture whether the caller explicitly supplied batch_size before it is
        # coalesced to the config default. Under auto-tuning this distinguishes a
        # user-requested per-round document cap from the (irrelevant) default.
        explicit_batch_size = batch_size
        num_workers = coalesce(num_workers, GraphRAGConfig.extraction_num_workers)
        batch_size = coalesce(batch_size, GraphRAGConfig.extraction_batch_size)
        include_classification_in_entity_id = coalesce(include_classification_in_entity_id, GraphRAGConfig.include_classification_in_entity_id)
        extract_timestamp = kwargs.pop('extract_timestamp', None)

        if num_workers > multiprocessing.cpu_count():
            num_workers = multiprocessing.cpu_count()
            logger.debug(f'Setting num_workers to CPU count [num_workers: {num_workers}]')

        for c in components:
            if isinstance(c, BaseExtractor):
                c.show_progress = show_progress

        id_generator=IdGenerator(
            tenant_id=tenant_id, 
            include_classification_in_entity_id=include_classification_in_entity_id,
            source_id_hash_length=GraphRAGConfig.source_id_hash_length
        )
        

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
            if isinstance(c, NodeParser):
                logger.debug(f'Wrapping {type(c).__name__} with IdRewriter')
                return IdRewriter(inner=c, id_generator=id_generator)
            else:
                return c
            
        components = [add_id_rewriter(c) for c in components]
        
        if not any([isinstance(c, IdRewriter) for c in components]):
            logger.debug(f'Adding DocToNodes to components')
            components.insert(0, IdRewriter(inner=DocsToNodes(), id_generator=id_generator))
            
        if checkpoint:
            components = [checkpoint.add_filter(c, tenant_id) for c in components]

        logger.debug(f'Extract pipeline components: {[type(c).__name__ for c in components]}')

        self.ingestion_pipeline = IngestionPipeline(transformations=components, disable_cache=True)
        self.pre_processors = pre_processors or []
        self.extraction_decorator = extraction_decorator or PassThroughDecorator()
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.show_progress = show_progress
        self.id_rewriter = IdRewriter(id_generator=id_generator)
        self.extraction_filters = extraction_filters or FilterConfig()
        self.extract_timestamp = extract_timestamp
        self.pipeline_kwargs = kwargs

        # Detect auto-tuning batch extraction from the batch extractor's config.
        # Auto-tuning is only meaningful when a Bedrock batch extractor is present.
        self._batch_config = self._find_batch_config(components)
        self._auto_tune = bool(self._batch_config and getattr(self._batch_config, 'auto_tune', False))
        self._explicit_batch_size = explicit_batch_size

        if self._auto_tune:
            logger.info(
                f'Auto-tuning batch extraction enabled '
                f'[num_workers: {self.num_workers}, max_batch_size: {self._batch_config.max_batch_size}]'
            )
            if getattr(self._batch_config, 'max_num_concurrent_batches', None):
                logger.info(
                    f'Auto-tuning submits num_workers ({self.num_workers}) batch jobs per round '
                    f'as the unit of concurrency; max_num_concurrent_batches '
                    f'({self._batch_config.max_num_concurrent_batches}) is not used in this mode'
                )

    @staticmethod
    def _unwrap_component(c):
        """Return the underlying component, unwrapping a CheckpointFilter if present."""
        return c.inner if isinstance(c, CheckpointFilter) else c

    @classmethod
    def _find_batch_config(cls, components):
        """Return the BatchConfig from the first BatchExtractorBase component, if any."""
        for c in components:
            inner = cls._unwrap_component(c)
            if isinstance(inner, BatchExtractorBase):
                return inner.batch_config
        return None
    
    def _source_documents_from_base_nodes(self, nodes:Sequence[BaseNode]) -> Generator[SourceDocument, None, None]:
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
        current_source_id = None
        current_source_document = None
        
        for node in nodes:
            source_info = node.relationships[NodeRelationship.SOURCE]
            source_id = source_info.node_id
            
            if not current_source_id:
                current_source_document = SourceDocument()
                current_source_id = source_id
 
            if source_id != current_source_id:
                if current_source_document:
                    yield current_source_document
                current_source_document = SourceDocument()
                current_source_id = source_id
                
            current_source_document.nodes.append(node)
            
        if current_source_document:
            yield current_source_document
    
    def extract(self, inputs: Iterable[SourceType]):
        """
        Extracts data from a given input source using multiple processing stages.

        Dispatches to the auto-tuning path when a batch extractor with
        ``auto_tune=True`` is present, otherwise runs the fixed-``batch_size``
        path with unchanged behavior.

        Args:
            inputs (Iterable[SourceType]): An iterable of input source types
                to be processed by the extraction pipeline.

        Yields:
            SourceDocument: Processed and extracted source documents after
                being handled by the extraction pipeline and decorators.
        """
        if self._auto_tune:
            yield from self._extract_auto_tuned(inputs)
        else:
            yield from self._extract_fixed_batch(inputs)

    def _extract_fixed_batch(self, inputs: Iterable[SourceType]):
        """
        Extracts data using the fixed-``batch_size`` strategy (default behavior).

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

        total_batches = math.ceil(len(inputs) / self.batch_size) if hasattr(inputs, '__len__') else None

        for batch_num, source_documents in enumerate(iter_batch(input_source_documents, self.batch_size), 1):

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

            batch_label = f'{batch_num}/{total_batches}' if total_batches else f'{batch_num}'
            logger.info(f'Running extraction pipeline [batch: {batch_label}, batch_size: {self.batch_size}, num_workers: {self.num_workers}]')
            
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

            extract_timestamp = self.extract_timestamp or int(time.time() * 1000)

            def add_timestamp(node):
                if EXTRACT_TIMESTAMP in node.metadata:
                    return node
                node.metadata[EXTRACT_TIMESTAMP] = extract_timestamp
                return node

            timestamped_nodes = [
                add_timestamp(node)
                for node in output_nodes
            ]
  
            output_source_documents = self._source_documents_from_base_nodes(timestamped_nodes)

            for source_document in output_source_documents:
                yield self.extraction_decorator.handle_output_doc(source_document)

    def _split_transformations(self):
        """Partition the ingestion transformations into a chunking prefix and an
        extractor tail.

        The chunking prefix is everything up to (but not including) the first
        extractor (``BaseExtractor``); the extractor tail is the extractor and
        everything after it. Components may be wrapped by ``CheckpointFilter``,
        which is unwrapped only for the isinstance test - the original (possibly
        wrapped) component is retained so checkpoint filtering is preserved.

        Returns:
            Tuple[List[TransformComponent], List[TransformComponent]]: The
            (chunking_transforms, extractor_transforms) partition.
        """
        transformations = self.ingestion_pipeline.transformations
        split_index = None
        for i, c in enumerate(transformations):
            if isinstance(self._unwrap_component(c), BaseExtractor):
                split_index = i
                break

        if split_index is None:
            # No extractor found; treat everything as chunking (extractor tail empty).
            return list(transformations), []

        return list(transformations[:split_index]), list(transformations[split_index:])

    def _chunk_source_document(self, source_document, chunking_transforms, get_source_metadata):
        """Apply pre-processing, id rewriting, decorator input hook, chunking, and
        extraction filters to a single source document, returning its filtered chunks.

        Mirrors the per-batch preparation in ``_extract_fixed_batch`` but for one
        document at a time so chunk counts are known before bucket assignment.
        """
        source_documents = [source_document]

        for pre_processor in self.pre_processors:
            source_documents = pre_processor.parse_source_docs(source_documents)

        source_documents = self.id_rewriter.handle_source_docs(source_documents)
        source_documents = self.extraction_decorator.handle_input_docs(source_documents)

        input_nodes = [
            n
            for sd in source_documents
            for n in sd.nodes
        ]

        # Run the chunking prefix (node parsers, checkpoint-wrapped or not) in the
        # main process so the resulting chunk count is known before assignment.
        chunked_nodes = run_transformations(
            input_nodes,
            transformations=chunking_transforms,
            in_place=True,
            cache=None,
        ) if chunking_transforms else input_nodes

        filtered_nodes = [
            node
            for node in chunked_nodes
            if self.extraction_filters.filter_source_metadata_dictionary(get_source_metadata(node))
        ]

        return filtered_nodes

    def _run_extractor_round(self, round_buckets, extractor_transforms):
        """Run the extractor tail over a round's pre-sized job buckets.

        Buckets are processed across at most ``num_workers`` worker processes,
        preserving the existing parallelism. Each bucket is <= max_batch_size
        (except a tail-merged final job, which the batch extractor re-splits).
        """
        num_workers = min(self.num_workers, len(round_buckets))

        if not extractor_transforms:
            # No extractor tail; nodes pass through unchanged.
            for bucket in round_buckets:
                for node in bucket:
                    yield node
            return

        extractor_pipeline = IngestionPipeline(transformations=extractor_transforms, disable_cache=True)

        output_nodes = run_pipeline(
            extractor_pipeline,
            round_buckets,
            num_workers=num_workers,
            **self.pipeline_kwargs
        )

        for node in output_nodes:
            yield node

    def _extract_auto_tuned(self, inputs: Iterable[SourceType]):
        """
        Extracts data using auto-tuning batch extraction.

        Streams source documents one at a time, chunks each in the main process,
        and accumulates the chunks in a :class:`BucketFiller`. When a round fills
        (``num_workers x max_batch_size`` chunks), it is sliced contiguously into
        jobs of ``max_batch_size`` and submitted. At input exhaustion the
        remainder is submitted as a final round, or merged into the previous
        round if it is below the Bedrock minimum. The user-supplied ``batch_size``
        (if any) acts only as an upper bound on documents consumed per round.

        Args:
            inputs (Iterable[SourceType]): An iterable of input source types.

        Yields:
            SourceDocument: Processed and extracted source documents.
        """
        def get_source_metadata(node):
            if isinstance(node, Document):
                return node.metadata
            else:
                return node.relationships[NodeRelationship.SOURCE].metadata

        max_batch_size = self._batch_config.max_batch_size

        if self._explicit_batch_size is not None:
            logger.info(
                f'Auto-tuning enabled; interpreting batch_size ({self._explicit_batch_size}) as a '
                f'cap on source documents consumed per round'
            )
        docs_per_round_cap = self._explicit_batch_size

        chunking_transforms, extractor_transforms = self._split_transformations()

        # Under auto-tuning, num_workers is the number of batch jobs per full
        # round (each up to max_batch_size) and the unit of job concurrency. It
        # supersedes max_num_concurrent_batches, which is the per-worker job
        # concurrency of the fixed-batch path.
        filler = BucketFiller(
            num_workers=self.num_workers,
            max_batch_size=max_batch_size,
            min_batch_size=BEDROCK_MIN_BATCH_SIZE,
        )

        # round_state is mutable so the nested closures see updates.
        round_state = {'num': 0, 'docs': 0}

        # Submission is deferred by one round (`held`) so a final remainder below
        # BEDROCK_MIN_BATCH_SIZE can be merged into the previous round's last job
        # rather than emitted as an undersized round that falls back to slow
        # synchronous extraction - mirroring split_nodes' tail-merge.
        held = {'jobs': None, 'docs': 0}

        def emit_jobs(round_jobs, trigger, docs_consumed):
            round_state['num'] += 1
            job_sizes = [len(j) for j in round_jobs]
            logger.info(
                f'Submitting auto-tuned extraction round '
                f'[round: {round_state["num"]}, trigger: {trigger}, jobs: {len(round_jobs)}, '
                f'job_sizes: {job_sizes}, documents_consumed: {docs_consumed}]'
            )
            if len(round_jobs) < self.num_workers:
                logger.info(
                    f'Round holds {len(round_jobs)} job(s) '
                    f'(num_workers={self.num_workers}) - fewer jobs than workers minimises per-job overhead'
                )
            logger.debug(f'Auto-tuned round job sizes: {job_sizes}')
            yield from self._emit_extracted(
                self._run_extractor_round(round_jobs, extractor_transforms)
            )

        def flush(trigger):
            # Drain the current buffer into a round, submit the previously-held
            # round, and hold the newly-drained one for possible tail-merge.
            round_jobs = filler.drain_round()
            if not round_jobs:
                return
            if held['jobs'] is not None:
                yield from emit_jobs(held['jobs'], 'full', held['docs'])
            held['jobs'] = round_jobs
            held['docs'] = round_state['docs']
            round_state['docs'] = 0

        input_source_documents = source_documents_from_source_types(inputs)

        for source_document in input_source_documents:

            chunks = self._chunk_source_document(
                source_document, chunking_transforms, get_source_metadata
            )

            if not chunks:
                continue

            # Flush before a document that would overshoot round capacity, so it
            # isn't split across rounds, or when an explicit batch_size caps the
            # documents per round. Don't flush a buffer below the Bedrock minimum:
            # that would strand a sub-minimum round on synchronous extraction, so
            # carry it forward to fill the round instead.
            over_doc_cap = docs_per_round_cap is not None and round_state['docs'] >= docs_per_round_cap
            if over_doc_cap:
                yield from flush(trigger='batch_size_cap')
            elif filler.would_overshoot(len(chunks)) and filler.pending() >= BEDROCK_MIN_BATCH_SIZE:
                yield from flush(trigger='full')

            if len(chunks) > filler.round_capacity:
                logger.warning(
                    f'Document produced {len(chunks)} chunks, exceeding a full round capacity '
                    f'({filler.round_capacity} = num_workers {self.num_workers} x max_batch_size '
                    f'{max_batch_size}). Its chunks will be split across multiple rounds and may be '
                    f'emitted as more than one SourceDocument. Increase max_batch_size or num_workers '
                    f'to keep large documents whole.'
                )

            filler.add_document_chunks(chunks)
            round_state['docs'] += 1

            # A single document larger than a whole round's capacity is drained
            # in full rounds off the front; the remainder carries forward.
            while filler.pending() >= filler.round_capacity:
                yield from flush(trigger='full')

        # Drain the final remainder and reconcile with the held round.
        final_jobs = filler.drain_round()
        final_docs = round_state['docs']
        final_total = sum(len(j) for j in final_jobs)

        merged_size = (len(held['jobs'][-1]) + final_total) if held['jobs'] is not None else 0
        if (held['jobs'] is not None
                and 0 < final_total < BEDROCK_MIN_BATCH_SIZE
                and merged_size <= BEDROCK_MAX_BATCH_SIZE):
            # Merge the undersized tail into the held round's last job (which may
            # then exceed max_batch_size, as split_nodes' tail-merge does) so it
            # is batched rather than routed to synchronous extraction. Guarded so
            # the merged job never exceeds Bedrock's hard per-job record limit;
            # if it would, the tail is left as its own (sync-fallback) round.
            held['jobs'][-1].extend(node for job in final_jobs for node in job)
            held['docs'] += final_docs
            final_jobs = []

        if held['jobs'] is not None:
            yield from emit_jobs(held['jobs'], 'full', held['docs'])
        if final_jobs:
            yield from emit_jobs(final_jobs, 'exhausted', final_docs)

    def _emit_extracted(self, output_nodes):
        """Apply extract-timestamp and decorator output hook to extracted nodes,
        reconstruct source documents, and yield them - shared post-processing that
        matches the fixed-batch path.
        """
        extract_timestamp = self.extract_timestamp or int(time.time() * 1000)

        def add_timestamp(node):
            if EXTRACT_TIMESTAMP in node.metadata:
                return node
            node.metadata[EXTRACT_TIMESTAMP] = extract_timestamp
            return node

        timestamped_nodes = [
            add_timestamp(node)
            for node in output_nodes
        ]

        output_source_documents = self._source_documents_from_base_nodes(timestamped_nodes)

        for source_document in output_source_documents:
            yield self.extraction_decorator.handle_output_doc(source_document)

    
   