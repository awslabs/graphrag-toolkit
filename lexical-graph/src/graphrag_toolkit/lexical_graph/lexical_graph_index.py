# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from typing import List, Optional, Union, Any
from pipe import Pipe

from graphrag_toolkit.lexical_graph.tenant_id import TenantId, TenantIdType, DEFAULT_TENANT_ID, to_tenant_id
from graphrag_toolkit.lexical_graph.metadata import FilterConfig, SourceMetadataFormatter, DefaultSourceMetadataFormatter, MetadataFiltersType
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, GraphStoreType
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory, VectorStoreType
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.graph import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing import sink
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.extract import ScopedValueProvider, FixedScopedValueProvider, DEFAULT_SCOPE
from graphrag_toolkit.lexical_graph.indexing.extract import GraphScopedValueStore, InMemoryScopedValueStore
from graphrag_toolkit.lexical_graph.indexing.extract import LLMPropositionExtractor, BatchLLMPropositionExtractor
from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor, BatchTopicExtractor
from graphrag_toolkit.lexical_graph.indexing.extract import ExtractionPipeline
from graphrag_toolkit.lexical_graph.indexing.extract import InferClassifications, InferClassificationsConfig
from graphrag_toolkit.lexical_graph.indexing.build import BuildPipeline
from graphrag_toolkit.lexical_graph.indexing.build import VectorIndexing
from graphrag_toolkit.lexical_graph.indexing.build import GraphConstruction
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint
from graphrag_toolkit.lexical_graph.indexing.build import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.build.null_builder import NullBuilder

from llama_index.core.node_parser import SentenceSplitter, NodeParser
from llama_index.core.schema import BaseNode, NodeRelationship

DEFAULT_EXTRACTION_DIR = 'output'

logger = logging.getLogger(__name__)

class ExtractionConfig():
    """
    Configuration for extraction-related operations.

    This class defines the settings and parameters for configuring extraction
    processes such as proposition extraction, entity classification inference,
    and topic or metadata filtering. It provides flexibility in customizing
    the extraction behavior and prompts to tailor it to specific use cases.

    Attributes:
        enable_proposition_extraction (bool): Determines whether proposition
        extraction is enabled. Defaults to True.
        preferred_entity_classifications (List[str]): A list of preferred entity
        classifications to focus on during the extraction process.
        Defaults to DEFAULT_ENTITY_CLASSIFICATIONS if not specified.
        infer_entity_classifications (Union[InferClassificationsConfig, bool]):
        Specifies whether to infer entity classifications, using either a
        configuration object or a boolean flag. Defaults to False.
        extract_propositions_prompt_template (Optional[str]): A template string
        used to prompt proposition extraction. If None, a default prompt is
        assumed.
        extract_topics_prompt_template (Optional[str]): A template string used
        to prompt topic extraction. If None, a default prompt is assumed.
        extraction_filters (Optional[MetadataFiltersType]): Metadata filters to
        be applied during the extraction process. Will be internally converted
        to a FilterConfig object.
    """
    def __init__(self,
                 enable_proposition_extraction: bool = True,
                 preferred_entity_classifications: List[str] = DEFAULT_ENTITY_CLASSIFICATIONS,
                 infer_entity_classifications: Union[InferClassificationsConfig, bool] = False,
                 extract_propositions_prompt_template: Optional[str] = None,
                 extract_topics_prompt_template: Optional[str] = None,
                 extraction_filters: Optional[MetadataFiltersType] = None):
        self.enable_proposition_extraction = enable_proposition_extraction
        self.preferred_entity_classifications = preferred_entity_classifications if preferred_entity_classifications is not None else []
        self.infer_entity_classifications = infer_entity_classifications
        self.extract_propositions_prompt_template = extract_propositions_prompt_template
        self.extract_topics_prompt_template = extract_topics_prompt_template
        self.extraction_filters = FilterConfig(extraction_filters)


class BuildConfig():
    """
    Configuration for the build process.

    This class encapsulates the configuration parameters and settings used during
    the build process. It provides options to specify filters, domain label
    inclusion, and metadata formatting. Users can customize these parameters
    to control build behavior as needed.

    Attributes:
        build_filters (Optional[BuildFilters]): Filters applied during the build
        process to include or exclude specific elements.
        include_domain_labels (Optional[bool]): Flag indicating whether to include
        domain labels as part of the build output.
        source_metadata_formatter (Optional[SourceMetadataFormatter]): Formatter
        responsible for handling source metadata during the build.
    """
    def __init__(self,
                 build_filters: Optional[BuildFilters] = None,
                 include_domain_labels: Optional[bool] = None,
                 include_local_entities: Optional[bool] = None,
                 source_metadata_formatter: Optional[SourceMetadataFormatter] = None):
        """
        Initializes an instance of the class. This constructor allows for the optional
        configuration of filters, domain label inclusion, and a metadata formatter.
        The appropriate default values will be used if no arguments are provided.

        Args:
            build_filters: An optional instance of BuildFilters. If not provided,
            a default BuildFilters instance will be used.
            include_domain_labels: An optional boolean indicating whether to include
            domain labels.
            include_local_entities: An optional boolean indicating whether to include
            local entities in the graph.
            source_metadata_formatter: An optional instance of SourceMetadataFormatter
            to format source metadata. If not provided, a DefaultSourceMetadataFormatter
            instance will be used.
        """
        self.build_filters = build_filters or BuildFilters()
        self.include_domain_labels = include_domain_labels
        self.include_local_entities = include_local_entities
        self.source_metadata_formatter = source_metadata_formatter or DefaultSourceMetadataFormatter()


class IndexingConfig():
    """
    Configuration for indexing data.

    This class encapsulates configurations required for indexing data, including
    chunking, extraction, build, and batch configuration. It is designed to provide
    flexibility in setting up the indexing process with optional configurations for
    data parsing, information extraction, build process, and batching.

    Attributes:
        chunking (Optional[List[NodeParser]]): List of chunking strategies to be
        applied during indexing. If no chunking strategies are provided, a
        default `SentenceSplitter` is used with a chunk size of 256 and an
        overlap of 20.
        extraction (Optional[ExtractionConfig]): Configuration for data extraction,
        defaulting to a new instance of `ExtractionConfig` if not provided.
        build (Optional[BuildConfig]): Build-specific configuration, defaulting to
        a new instance of `BuildConfig` if not provided.
        batch_config (Optional[BatchConfig]): Configuration for batch inference.
        Defaults to None, indicating that batch inference is not used.
    """
    def __init__(self,
                 chunking: Optional[List[NodeParser]] = [],
                 extraction: Optional[ExtractionConfig] = None,
                 build: Optional[BuildConfig] = None,
                 batch_config: Optional[BatchConfig] = None):
        """
        Initializes the instance with configurations for chunking, extraction, building,
        and batch processing. These configurations determine the behavior of the system
        when processing data in terms of splitting, extracting information, building entities,
        and handling batch operations.

        Args:
            chunking (Optional[List[NodeParser]]): A list of node parsers used for chunking
                text into smaller segments. When set to None, no chunking is performed. A
                default SentenceSplitter is added if an empty list is provided.
            extraction (Optional[ExtractionConfig]): Configuration for extracting relevant
                information. If not provided, a default `ExtractionConfig` is used.
            build (Optional[BuildConfig]): Configuration for handling building operations.
                Defaults to a new `BuildConfig` instance when not specified.
            batch_config (Optional[BatchConfig]): Configuration for batch inference
                operations. If None, batch inference is not used.
        """
        if chunking is not None and len(chunking) == 0:
            chunking.append(SentenceSplitter(chunk_size=256, chunk_overlap=20))

        self.chunking = chunking  # None = no chunking
        self.extraction = extraction or ExtractionConfig()
        self.build = build or BuildConfig()
        self.batch_config = batch_config  # None = do not use batch inference

IndexingConfigType = Union[IndexingConfig, ExtractionConfig, BuildConfig, BatchConfig, List[NodeParser]]

def get_topic_scope(node: BaseNode):
    """
    Retrieves the topic scope for a given node.

    This function determines the topic scope of the provided node by examining
    its SOURCE relationship. If the SOURCE relationship does not exist, it
    returns a default scope constant. Otherwise, it retrieves the node ID from
    the SOURCE relationship.

    Args:
        node (BaseNode): The node for which the topic scope is determined.

    Returns:
        str: The determined topic scope of the node. If no SOURCE relationship
        exists, returns a default scope.
    """
    source = node.relationships.get(NodeRelationship.SOURCE, None)
    if not source:
        return DEFAULT_SCOPE
    else:
        return source.node_id
    

def to_indexing_config(indexing_config:Optional[IndexingConfigType]=None) -> IndexingConfig:
    if not indexing_config:
        return IndexingConfig()
    if isinstance(indexing_config, IndexingConfig):
        return indexing_config
    elif isinstance(indexing_config, ExtractionConfig):
        return IndexingConfig(extraction=indexing_config)
    elif isinstance(indexing_config, BuildConfig):
        return IndexingConfig(build=indexing_config)
    elif isinstance(indexing_config, BatchConfig):
        return IndexingConfig(batch_config=indexing_config)
    elif isinstance(indexing_config, list):
        for np in indexing_config:
            if not isinstance(np, NodeParser):
                raise ValueError(f'Invalid indexing config type: {type(np)}')
        return IndexingConfig(chunking=indexing_config)
    else:
        raise ValueError(f'Invalid indexing config type: {type(indexing_config)}')





def to_indexing_config(indexing_config: Optional[IndexingConfigType] = None) -> IndexingConfig:
    """
    Converts a given indexing configuration into an `IndexingConfig` object.

    This function takes an optional parameter `indexing_config`. Depending on the
    type of the input, it creates and returns an `IndexingConfig` object. If no
    input is provided, it returns a default `IndexingConfig` object. The function
    validates the input and raises a `ValueError` if the provided type does not
    match any supported configuration type.

    Args:
        indexing_config: Optional; Can be of type `IndexingConfig`,
            `ExtractionConfig`, `BuildConfig`, `BatchConfig`, `list` of
            `NodeParser`, or `None`. Represents the indexing configuration
            which will be transformed into an `IndexingConfig` object.
            If provided as `list`, each item in the list must be an instance
            of `NodeParser`.

    Returns:
        IndexingConfig: A configured `IndexingConfig` object based on the input.

    Raises:
        ValueError: If `indexing_config` is of an unsupported type, or if it is
        a `list` containing elements that are not of type `NodeParser`.
    """
    if not indexing_config:
        return IndexingConfig()
    if isinstance(indexing_config, IndexingConfig):
        return indexing_config
    elif isinstance(indexing_config, ExtractionConfig):
        return IndexingConfig(extraction=indexing_config)
    elif isinstance(indexing_config, BuildConfig):
        return IndexingConfig(build=indexing_config)
    elif isinstance(indexing_config, BatchConfig):
        return IndexingConfig(batch_config=indexing_config)
    elif isinstance(indexing_config, list):
        for np in indexing_config:
            if not isinstance(np, NodeParser):
                raise ValueError(f'Invalid indexing config type: {type(np)}')
        return IndexingConfig(chunking=indexing_config)
    else:
        raise ValueError(f'Invalid indexing config type: {type(indexing_config)}')


class LexicalGraphIndex():
    """
    Manages the creation of a lexical graph and vector store index for node data extraction
    and indexing. The class integrates multiple pipelines to support tasks including entity
    classification, proposition extraction, topic extraction, and graph-based operations.

    The primary usage of this class is to configure and process input data into structured
    graph and vector store formats suitable for various retrieval and inference tasks. The
    class relies on configurable components for batch processing, classification inference,
    and topic scoping, making it adaptable to different indexing requirements.

    Attributes:
        graph_store (MultiTenantGraphStore): Multi-tenant wrapper for the graph store used for
            indexing and graph-based operations.
        vector_store (MultiTenantVectorStore): Multi-tenant wrapper for the vector store used
            for efficient vector search and retrieval tasks.
        tenant_id (TenantId): The tenant ID associated with the current instance.
        extraction_dir (str): Path to the directory used for temporary storage during data
            extraction.
        indexing_config (IndexingConfig): Configuration object containing various attributes
            to control indexing behavior, such as enabling chunking, proposition extraction,
            and classification inference.
        extraction_pre_processors (list): List of preprocessing steps used for data processing
            during the extraction pipeline.
        extraction_components (list): List of components forming the main data extraction
            pipeline, including proposition and topic extractors.
        allow_batch_inference (bool): Specifies whether batch inference is allowed based on
            indexing configuration settings.
    """

    def __init__(
            self,
            graph_store: Optional[GraphStoreType] = None,
            vector_store: Optional[VectorStoreType] = None,
            tenant_id: Optional[TenantIdType] = None,
            extraction_dir: Optional[str] = None,
            indexing_config: Optional[IndexingConfigType] = None,
    ):

        tenant_id = to_tenant_id(tenant_id)

        self.graph_store = MultiTenantGraphStore.wrap(GraphStoreFactory.for_graph_store(graph_store), tenant_id)
        self.vector_store = MultiTenantVectorStore.wrap(VectorStoreFactory.for_vector_store(vector_store), tenant_id)
        self.tenant_id = tenant_id or TenantId()
        self.extraction_dir = extraction_dir or DEFAULT_EXTRACTION_DIR
        self.indexing_config = to_indexing_config(indexing_config)

        (pre_processors, components) = self._configure_extraction_pipeline(self.indexing_config)

        self.extraction_pre_processors = pre_processors
        self.extraction_components = components

    def _configure_extraction_pipeline(self, config: IndexingConfig):
        """
        Configures and initializes the extraction pipeline based on the given configuration settings. This method
        constructs a series of preprocessing and component steps tailored to support tasks like chunking,
        proposition extraction, entity classification inference, and topic identification. These steps are
        assembled dynamically based on the attributes provided by the `config` parameter.

        The extraction pipeline is built with flexibility to accommodate configurations such as enabling
        batch processing, setting up classification inference, or using different prompt templates for
        proposition and topic extraction. Additionally, providers for entity classification and topic
        scoping are conditionally instantiated depending on the type of graph store used.

        The method returns a tuple containing two lists: `pre_processors` and `components`. The
        `pre_processors` list consists of pre-processing steps like classification inference, while the
        `components` list includes the main extraction pipeline elements like proposition or topic
        extractors.

        Args:
            config (IndexingConfig): The configuration object that specifies various attributes needed for
                building the extraction pipeline, including settings for chunking, proposition extraction,
                entity classifications, and topics.

        Returns:
            tuple: A two-element tuple comprising:
                - `pre_processors` (list): Steps to preprocess the input data before running the main
                  extraction operations.
                - `components` (list): The primary components of the extraction pipeline including
                  proposition and topic extractors.

        Raises:
            ValueError: If any required configuration attributes are missing or invalid.
        """
        pre_processors = []
        components = []

        if config.chunking:
            for c in config.chunking:
                components.append(c)

        if config.extraction.enable_proposition_extraction:
            if config.batch_config:
                components.append(BatchLLMPropositionExtractor(
                    batch_config=config.batch_config,
                    prompt_template=config.extraction.extract_propositions_prompt_template
                ))
            else:
                components.append(LLMPropositionExtractor(
                    prompt_template=config.extraction.extract_propositions_prompt_template
                ))

        entity_classification_value_store = InMemoryScopedValueStore()
        entity_classification_provider = None
        topic_provider = None
        
        classification_label = 'EntityClassification'
        classification_scope = DEFAULT_SCOPE

        if isinstance(self.graph_store, DummyGraphStore):
            entity_classification_provider = FixedScopedValueProvider(
                scoped_values={
                    DEFAULT_SCOPE: config.extraction.preferred_entity_classifications
                }
            )
            topic_provider = FixedScopedValueProvider(
                scoped_values={
                    DEFAULT_SCOPE: []
                }
            )
        else:

            entity_classification_value_store.save_scoped_values(
                classification_label, 
                classification_scope, 
                config.extraction.preferred_entity_classifications
            )

            entity_classification_provider = ScopedValueProvider(
                label=classification_label,
                scoped_value_store=entity_classification_value_store
            )
            
            topic_provider = ScopedValueProvider(
                label='StatementTopic',
                scoped_value_store=GraphScopedValueStore(graph_store=self.graph_store),
                scope_func=get_topic_scope
            )

        if config.extraction.infer_entity_classifications:
            if isinstance(config.extraction.infer_entity_classifications, InferClassificationsConfig):
                infer_config = config.extraction.infer_entity_classifications 
            else:
                infer_config = InferClassificationsConfig()

            pre_processors.append(InferClassifications(
                    classification_label=classification_label,
                    classification_scope=classification_scope,
                    classification_store=entity_classification_value_store,
                    splitter=SentenceSplitter(chunk_size=256, chunk_overlap=20) if config.chunking else None,
                    default_classifications=config.extraction.preferred_entity_classifications,
                    num_samples=infer_config.num_samples,
                    num_iterations=infer_config.num_iterations,
                    num_classifications=infer_config.num_classifications,
                    merge_action=infer_config.on_existing_classifications,
                    prompt_template=infer_config.prompt_template
                )
            )

        topic_extractor = None

        if config.batch_config:
            topic_extractor = BatchTopicExtractor(
                batch_config=config.batch_config,
                source_metadata_field=PROPOSITIONS_KEY if config.extraction.enable_proposition_extraction else None,
                entity_classification_provider=entity_classification_provider,
                topic_provider=topic_provider,
                prompt_template=config.extraction.extract_topics_prompt_template
            )
        else:
            topic_extractor = TopicExtractor(
                source_metadata_field=PROPOSITIONS_KEY if config.extraction.enable_proposition_extraction else None,
                entity_classification_provider=entity_classification_provider,
                topic_provider=topic_provider,
                prompt_template=config.extraction.extract_topics_prompt_template
            )

        components.append(topic_extractor)

        return (pre_processors, components)

    def extract(
            self,
            nodes: List[BaseNode] = [],
            handler: Optional[NodeHandler] = None,
            checkpoint: Optional[Checkpoint] = None,
            show_progress: Optional[bool] = False,
            **kwargs: Any) -> None:
        """
        Executes the extraction process for a given set of nodes using the specified handler,
        checkpoint, and other configuration options. This function manages the construction and
        execution of both the extraction pipeline and build pipeline, leveraging configured
        components and filters to process the input nodes. Integration with a handler for
        custom processing is also supported.

        Args:
            nodes (List[BaseNode], optional): A list of nodes to be processed during the extraction.
            handler (Optional[NodeHandler], optional): A handler to process nodes after extraction.
            checkpoint (Optional[Checkpoint], optional): A checkpoint to manage pipeline state and
                progress during extraction and build stages.
            show_progress (Optional[bool], optional): Indicates whether to display progress during
                the pipeline execution.
            **kwargs (Any): Additional keyword arguments for pipeline configurations or overrides.
        """

        if not self.tenant_id.is_default_tenant():
            logger.warning('TenantId has been set to non-default tenant id, but extraction will use default tenant id')

        extraction_pipeline = ExtractionPipeline.create(
            components=self.extraction_components,
            pre_processors=self.extraction_pre_processors,
            show_progress=show_progress,
            checkpoint=checkpoint,
            tenant_id=DEFAULT_TENANT_ID,
            extraction_filters=self.indexing_config.extraction.extraction_filters,
            **kwargs
        )

        build_pipeline = BuildPipeline.create(
            components=[
                NullBuilder()
            ],
            show_progress=show_progress,
            checkpoint=checkpoint,
            num_workers=1,
            tenant_id=DEFAULT_TENANT_ID,
            **kwargs
        )

        if handler:
            nodes | extraction_pipeline | Pipe(handler.accept) | build_pipeline | sink
        else:
            nodes | extraction_pipeline | build_pipeline | sink

    def build(
            self,
            nodes: List[BaseNode] = [],
            handler: Optional[NodeHandler] = None,
            checkpoint: Optional[Checkpoint] = None,
            show_progress: Optional[bool] = False,
            **kwargs: Any) -> None:
        """
        Builds an indexing pipeline for processing nodes, constructing a graph, and creating
        a vector store index. The function orchestrates the pipeline with optional handlers,
        checkpoints, and progress display settings. The pipeline components are assembled based
        on configuration settings and the provided nodes are processed through the pipeline.

        Args:
            nodes (List[BaseNode]): A list of nodes to be processed in the build pipeline.
            handler (Optional[NodeHandler]): A handler function or object for post-processing
                nodes after indexing. Defaults to None.
            checkpoint (Optional[Checkpoint]): A checkpoint object for saving or resuming the
                progress of the build pipeline. Defaults to None.
            show_progress (Optional[bool]): A flag indicating whether to show progress during
                pipeline execution. Defaults to False.
            **kwargs (Any): Additional keyword arguments for extending or customizing the
                pipeline behavior.

        Returns:
            None
        """

        build_config = self.indexing_config.build

        build_pipeline = BuildPipeline.create(
            components=[
                GraphConstruction.for_graph_store(self.graph_store),
                VectorIndexing.for_vector_store(self.vector_store)
            ],
            show_progress=show_progress,
            checkpoint=checkpoint,
            build_filters=build_config.build_filters,
            source_metadata_formatter=build_config.source_metadata_formatter,
            include_domain_labels=build_config.include_domain_labels,
            include_local_entities=build_config.include_local_entities,
            tenant_id=self.tenant_id,
            **kwargs
        )

        sink_fn = sink if not handler else Pipe(handler.accept)
        nodes | build_pipeline | sink_fn

    def extract_and_build(
            self,
            nodes: List[BaseNode] = [],
            handler: Optional[NodeHandler] = None,
            checkpoint: Optional[Checkpoint] = None,
            show_progress: Optional[bool] = False,
            **kwargs: Any
    ) -> None:
        """
        Extracts data, processes it using a pipeline, and builds structures for storage or further
        usage. It utilizes multiple components for extraction, pre-processing, and construction
        of graph and vector indices. This method also supports a checkpoint mechanism and optional
        progress visualization.

        Args:
            nodes (List[BaseNode], optional): A list of nodes to process. Defaults to an empty list.
            handler (Optional[NodeHandler]): A handler object to manage processed nodes. Defaults to None.
            checkpoint (Optional[Checkpoint]): A checkpoint object for resuming pipelines. Defaults to None.
            show_progress (Optional[bool]): Boolean flag to display pipeline progress. Defaults to False.
            **kwargs (Any): Additional parameters to pass to pipelines.
        """

        if not self.tenant_id.is_default_tenant():
            logger.warning('TenantId has been set to non-default tenant id, but extraction will use default tenant id')

        extraction_pipeline = ExtractionPipeline.create(
            components=self.extraction_components,
            pre_processors=self.extraction_pre_processors,
            show_progress=show_progress,
            checkpoint=checkpoint,
            tenant_id=DEFAULT_TENANT_ID,
            extraction_filters=self.indexing_config.extraction.extraction_filters,
            **kwargs
        )

        build_config = self.indexing_config.build
        
        build_pipeline = BuildPipeline.create(
            components=[
                GraphConstruction.for_graph_store(self.graph_store),
                VectorIndexing.for_vector_store(self.vector_store)
            ],
            show_progress=show_progress,
            checkpoint=checkpoint,
            build_filters=build_config.build_filters,
            source_metadata_formatter=build_config.source_metadata_formatter,
            include_domain_labels=build_config.include_domain_labels,
            tenant_id=self.tenant_id,
            **kwargs
        )

        sink_fn = sink if not handler else Pipe(handler.accept)
        nodes | extraction_pipeline | build_pipeline | sink_fn
