# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import asdict
from typing import List, NamedTuple, Optional, Union, Any, Dict, overload
from pipe import Pipe

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.tenant_id import TenantId, TenantIdType, DEFAULT_TENANT_ID, to_tenant_id
from graphrag_toolkit.lexical_graph.metadata import FilterConfig, SourceMetadataFormatter, DefaultSourceMetadataFormatter, MetadataFiltersType
from graphrag_toolkit.lexical_graph.metadata import to_metadata_filter
from graphrag_toolkit.lexical_graph.versioning import VersioningConfig, VALID_FROM, VALID_TO, EXTRACT_TIMESTAMP, BUILD_TIMESTAMP, VERSIONING_METADATA_KEYS, VERSION_INDEPENDENT_ID_FIELDS, TIMESTAMP_LOWER_BOUND, PREV_VERSIONS
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, GraphStoreType
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory, VectorStoreType
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.graph import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import filter_config_to_opencypher_filters
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing import sink
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.extract import PREFERRED_VALUES_PROVIDER_TYPE, default_preferred_values
from graphrag_toolkit.lexical_graph.indexing.extract import LLMPropositionExtractor, BatchLLMPropositionExtractorSync
from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor, BatchTopicExtractorSync
from graphrag_toolkit.lexical_graph.indexing.extract import ExtractionPipeline
from graphrag_toolkit.lexical_graph.indexing.extract import InferClassifications, InferClassificationsConfig
from graphrag_toolkit.lexical_graph.indexing.extract import OntologyType, to_ontology_config
from graphrag_toolkit.lexical_graph.indexing.extract import OntologyFilter
from graphrag_toolkit.lexical_graph.indexing.build import BuildPipeline
from graphrag_toolkit.lexical_graph.indexing.build import VectorIndexing
from graphrag_toolkit.lexical_graph.indexing.build import GraphConstruction
from graphrag_toolkit.lexical_graph.indexing.build import VersionManager
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint
from graphrag_toolkit.lexical_graph.indexing.build import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.build.null_builder import NullBuilder
from graphrag_toolkit.lexical_graph.indexing.build.delete_sources import DeleteSources
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from graphrag_toolkit.lexical_graph.indexing.progress_monitor import ProgressMonitor

from llama_index.core.node_parser import SentenceSplitter, NodeParser
from llama_index.core.schema import BaseNode
from llama_index.core.llms import LLM

logger = logging.getLogger(__name__)

ExtractionLLMType = Union[str, LLM, LLMCache]

class OntologyConstraints(NamedTuple):
    """The rendered blocks an ontology contributes to each extraction prompt.

    Named rather than a bare tuple because the two are easy to swap and the
    failure would be silent: the propositions prompt would receive the full
    vocabulary and the topics prompt only the class names, and both would still
    render, extract, and look plausible.

    Attributes:
        topics (str): The full vocabulary block, for the topics prompt.
        propositions (str): The class names alone, for the propositions prompt.
    """
    topics:str
    propositions:str

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
        preferred_topics: A list of preferred topic names (or a callable that
        returns them) used to seed the LLM during topic extraction. Defaults
        to an empty list.
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
        extraction_llm (Optional[ExtractionLLMType]): LLM to be used for extracting
        propositions and topics. If None, GraphRAGConfig.extract_llm is used.
        ontology (Optional[OntologyType]): An ontology to guide extraction, as an
        OntologyConfig, an Ontology, an rdflib.Graph, or a path to a Turtle file.
        Normalized to an OntologyConfig at its default authority level ('align') when
        it is not one already. Defaults to None, which leaves extraction exactly
        as it was.
    """
    def __init__(self,
                 enable_proposition_extraction: bool = True,
                 preferred_entity_classifications: PREFERRED_VALUES_PROVIDER_TYPE = DEFAULT_ENTITY_CLASSIFICATIONS,
                 preferred_topics: PREFERRED_VALUES_PROVIDER_TYPE = None,
                 infer_entity_classifications: Union[InferClassificationsConfig, bool] = False,
                 extract_propositions_prompt_template: Optional[str] = None,
                 extract_topics_prompt_template: Optional[str] = None,
                 extraction_filters: Optional[MetadataFiltersType] = None,
                 extraction_llm: Optional[ExtractionLLMType] = None,
                 ontology: Optional[OntologyType] = None):
        self.enable_proposition_extraction = enable_proposition_extraction

        # Whether the user named a list, asked for none, or said nothing at all.
        # Seeding turns on that distinction, and it is only knowable here: the default is a module-level list, so identity against
        # it answers the question exactly, whereas a value check later cannot
        # tell a user who passed the defaults from a user who passed nothing.
        # `None` counts as set - it is a request for no preferences, and reads
        # differently from silence.
        self.preferred_entity_classifications_provided = (
            preferred_entity_classifications is not DEFAULT_ENTITY_CLASSIFICATIONS
        )

        self.preferred_entity_classifications = preferred_entity_classifications if preferred_entity_classifications is not None else []
        self.preferred_topics = preferred_topics if preferred_topics is not None else []
        self.infer_entity_classifications = infer_entity_classifications
        self.extract_propositions_prompt_template = extract_propositions_prompt_template
        self.extract_topics_prompt_template = extract_topics_prompt_template
        self.extraction_filters = FilterConfig(extraction_filters)
        if extraction_llm is not None:
            self.extraction_llm = extraction_llm if isinstance(extraction_llm, LLMCache) else GraphRAGConfig.to_llm(extraction_llm)
        else:
            self.extraction_llm = None

        self.ontology = to_ontology_config(ontology) if ontology is not None else None

        if self.ontology is not None:
            self._validate_ontology_combination()

    def _validate_ontology_combination(self):
        """Reject the one ontology/inference combination that has no coherent reading.

        Inference alongside an ontology is coherent and supported: the ontology's
        class names seed `default_classifications` and inference adds the domain
        terms the ontology does not declare, which is exactly what 'align'
        permits - name what is declared, keep what is not.

        `replace_default_classifications=True` is the exception. It asks for the
        seeded names to be discarded, so the user has configured an ontology and
        then asked for its vocabulary to be thrown away before the model ever
        sees that slot. Silently honouring either half would be a guess, so it
        raises instead.

        Raises:
            ValueError: If inference is configured to replace the seeded
                classifications while an ontology is present.
        """
        infer = self.infer_entity_classifications
        if isinstance(infer, InferClassificationsConfig) and infer.replace_default_classifications:
            raise ValueError(
                'infer_entity_classifications with replace_default_classifications=True '
                'cannot be combined with an ontology: the ontology seeds the preferred '
                'entity classifications and replacing them discards the vocabulary the '
                'ontology was configured to supply. Set '
                'replace_default_classifications=False to let inference extend the '
                "ontology's classes, or remove the ontology."
            )


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
        enable_versioning (Optional[bool]): Whether to enable versioned updates
        during the build stage. Overrides GraphRAGConfig.enable_versioning when
        set.
    """
    def __init__(self,
                 build_filters: Optional[BuildFilters] = None,
                 include_domain_labels: Optional[bool] = None,
                 include_local_entities: Optional[bool] = None,
                 source_metadata_formatter: Optional[SourceMetadataFormatter] = None,
                 enable_versioning: Optional[bool] = None):
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
        self.enable_versioning = enable_versioning

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
        overlap of 25.
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
        if chunking is not None:
            if isinstance(chunking, NodeParser):
                chunking = [chunking]
            if isinstance(chunking, list) and  len(chunking) == 0:
                chunking.append(SentenceSplitter(chunk_size=256, chunk_overlap=25))

        self.chunking = chunking  # None = no chunking
        self.extraction = extraction or ExtractionConfig()
        self.build = build or BuildConfig()
        self.batch_config = batch_config  # None = do not use batch inference

IndexingConfigType = Union[IndexingConfig, ExtractionConfig, BuildConfig, BatchConfig, List[NodeParser]]

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
            pipeline, including proposition and topic extractors, and - when an
            ontology resolves at least one dimension on - an OntologyFilter after
            them.
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
        from llama_index.core.utils import globals_helper
        globals_helper.stopwords

        tenant_id = to_tenant_id(tenant_id)

        self.graph_store = MultiTenantGraphStore.wrap(GraphStoreFactory.for_graph_store(graph_store), tenant_id)
        self.vector_store = MultiTenantVectorStore.wrap(VectorStoreFactory.for_vector_store(vector_store), tenant_id)
        self.tenant_id = tenant_id or TenantId()
        self.extraction_dir = extraction_dir or GraphRAGConfig.local_output_dir
        self.indexing_config = to_indexing_config(indexing_config)

        (pre_processors, components) = self._configure_extraction_pipeline(self.indexing_config)

        self.extraction_pre_processors = pre_processors
        self.extraction_components = components

        # Backend bootstrap (for example index creation) is part of object lifecycle; fail fast if unavailable.
        self.graph_store.init()

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

        # Rendered once, here in the parent process, and handed to the extractors
        # as a plain string. Rendering reads the rdflib graph, which does not
        # cross the spawn boundary; a string does. Empty when there is no
        # ontology or its authority is 'off', and an empty block leaves every
        # prompt byte-identical to what it was.
        ontology_constraints = self._render_ontology_constraints(config.extraction.ontology)

        if config.extraction.enable_proposition_extraction:
            if config.batch_config:
                components.append(BatchLLMPropositionExtractorSync(
                    batch_config=config.batch_config,
                    prompt_template=config.extraction.extract_propositions_prompt_template,
                    llm=config.extraction.extraction_llm,
                    ontology_constraints=ontology_constraints.propositions
                ))
            else:
                components.append(LLMPropositionExtractor(
                    prompt_template=config.extraction.extract_propositions_prompt_template,
                    llm=config.extraction.extraction_llm,
                    ontology_constraints=ontology_constraints.propositions
                ))

        entity_classification_provider = None
        topic_provider = None

        # Unchanged, and deliberately ahead of any ontology seeding: with no real
        # store there is no scoped-value store to read or write, so both providers
        # stay empty. The ontology still reaches the prompt through
        # `ontology_constraints` above - what a DummyGraphStore run cannot show is
        # the seeded `{preferred_entity_classifications}` slot.
        if isinstance(self.graph_store, DummyGraphStore):
            entity_classification_provider = default_preferred_values([])
            topic_provider = default_preferred_values([])
        else:

            # Either the user's list or the ontology's class names, decided once
            # so the three branches below cannot disagree about which it is.
            preferred_entity_classifications = self._preferred_entity_classifications(config.extraction)

            if config.extraction.infer_entity_classifications:

                if isinstance(config.extraction.infer_entity_classifications, InferClassificationsConfig):
                    infer_config = config.extraction.infer_entity_classifications
                else:
                    infer_config = InferClassificationsConfig()

                default_classifications = []

                if isinstance(preferred_entity_classifications, list):
                    default_classifications = preferred_entity_classifications

                entity_classification_provider = InferClassifications(
                    splitter=SentenceSplitter(chunk_size=256, chunk_overlap=20) if config.chunking else None,
                    default_classifications=default_classifications,
                    num_samples=infer_config.num_samples,
                    num_iterations=infer_config.num_iterations,
                    num_classifications=infer_config.num_classifications,
                    prompt_template=infer_config.prompt_template,
                    replace_default_classifications=infer_config.replace_default_classifications,
                    llm=config.extraction.extraction_llm
                )

                pre_processors.append(entity_classification_provider)

            elif isinstance(preferred_entity_classifications, list):
                entity_classification_provider = default_preferred_values(preferred_entity_classifications)
            else:
                entity_classification_provider = preferred_entity_classifications

            if isinstance(config.extraction.preferred_topics, list):
                topic_provider = default_preferred_values(config.extraction.preferred_topics)
            else:
                topic_provider = config.extraction.preferred_topics

        topic_extractor = None

        if config.batch_config:
            topic_extractor = BatchTopicExtractorSync(
                batch_config=config.batch_config,
                source_metadata_field=PROPOSITIONS_KEY if config.extraction.enable_proposition_extraction else None,
                entity_classification_provider=entity_classification_provider,
                topic_provider=topic_provider,
                prompt_template=config.extraction.extract_topics_prompt_template,
                llm=config.extraction.extraction_llm,
                ontology_constraints=ontology_constraints.topics
            )
        else:
            topic_extractor = TopicExtractor(
                source_metadata_field=PROPOSITIONS_KEY if config.extraction.enable_proposition_extraction else None,
                entity_classification_provider=entity_classification_provider,
                topic_provider=topic_provider,
                prompt_template=config.extraction.extract_topics_prompt_template,
                llm=config.extraction.extraction_llm,
                ontology_constraints=ontology_constraints.topics
            )

        components.append(topic_extractor)

        ontology_filter = self._ontology_filter(config.extraction.ontology)

        if ontology_filter is not None:
            components.append(ontology_filter)

        return (pre_processors, components)

    def _typed_properties(self) -> Optional[str]:
        """Where the builders should store coerced attribute values, if anywhere.

        The setting lives on `OntologyConfig` and only there, which is what keeps
        it unreachable without one: with no ontology this returns None, `BuildPipeline`
        coalesces that to `GraphRAGConfig.typed_properties`, and that is `'off'`
        unless something set it programmatically. So a user who never mentioned an
        ontology cannot reach a placement that writes, and no environment variable
        can reach one on their behalf.

        Returned as None rather than as `'off'` so that the `coalesce` chain in
        `BuildPipeline` behaves the same way here as for every other setting - an
        unasked-for value defers to the layer below rather than pinning it.

        Returns:
            The configured placement, or None when there is no ontology.
        """
        ontology_config = self.indexing_config.extraction.ontology
        return None if ontology_config is None else ontology_config.typed_properties

    @staticmethod
    def _ontology_filter(ontology_config) -> Optional[OntologyFilter]:
        """The filter that enforces what the prompt asked for, or None.

        This runs *after* the topic extractor and nowhere else. Everything the
        ontology contributes before this point is advisory - a block of text in a
        prompt, which a model may ignore - and this is the only component that
        makes a level's claim true rather than requested.

        Returns None when there is no ontology, and when every dimension resolves
        to False. The second case is not the same as the first:
        `ontology_authority='off'` still seeds `{preferred_entity_classifications}` from
        the ontology, so the prompt differs from the no-ontology prompt even
        though no component here does. What `off` does guarantee is that nothing
        rewrites or discards a fact the model produced, and the way it guarantees
        it is by this method returning None - not by a filter that runs with every
        flag off. A no-op in the pipeline would still round-trip `TOPICS_KEY`
        through `model_validate` / `model_dump` and would still annotate, which is
        exactly the difference `off` rules out.

        Args:
            ontology_config: The normalized `OntologyConfig`, or None.

        Returns:
            A configured `OntologyFilter`, or None if it would have nothing to do.
        """
        if ontology_config is None or not ontology_config.filter_required():
            return None

        # Spread the resolved dimensions rather than naming them one by one. The
        # field names of `ResolvedDimensions` and the flags of `OntologyFilter`
        # are deliberately the same six words, and a hand-written argument list
        # can omit one - which would leave a gate the user asked for silently not
        # running, the one failure in this feature that looks like success.
        return OntologyFilter(
            index=ontology_config.ontology.index(),
            report_violations=ontology_config.report_violations,
            **asdict(ontology_config.resolved()),
        )

    @staticmethod
    def _render_ontology_constraints(ontology_config) -> OntologyConstraints:
        """Render the two prompt blocks an ontology contributes, once.

        Both extraction stages get a block, and they are not the same block: the
        topics prompt gets the full vocabulary, the propositions prompt gets the
        class names alone, because classifying the entities it names is the only
        thing that stage does which an ontology can steer.

        Args:
            ontology_config: The normalized `OntologyConfig`, or None.

        Returns:
            The two rendered blocks, both empty when there is no ontology.
        """
        if ontology_config is None:
            return OntologyConstraints('', '')

        ontology = ontology_config.ontology
        ontology_authority = ontology_config.ontology_authority

        # `vocabulary_format` reaches the topics block only. The propositions
        # block is a class-name list by design - that stage classifies the
        # entities it names and extracts nothing else - so there is no property
        # vocabulary there for a serialization to present differently.
        return OntologyConstraints(
            topics=ontology.format_as_prompt_constraint(
                ontology_authority, ontology_config.vocabulary_format
            ),
            propositions=ontology.format_as_proposition_constraint(ontology_authority)
        )

    @staticmethod
    def _preferred_entity_classifications(extraction_config: ExtractionConfig) -> PREFERRED_VALUES_PROVIDER_TYPE:
        """Decide what fills the `{preferred_entity_classifications}` prompt slot.

        With an ontology and no user list, the slot is seeded
        from the ontology's rendered class names, so it cannot name a class
        differently from the way the vocabulary block above it does.

        A user who named their own list keeps it, with a
        warning. Honouring the ontology instead would discard a setting the user
        made deliberately, and merging the two would produce a vocabulary neither
        of them asked for.

        Args:
            extraction_config: The extraction configuration to read.

        Returns:
            The user's value, unchanged, unless an ontology is present and the
            user said nothing - in which case the ontology's class names.
        """
        if extraction_config.ontology is None:
            return extraction_config.preferred_entity_classifications

        if extraction_config.preferred_entity_classifications_provided:
            logger.warning(
                'Both an ontology and preferred_entity_classifications were configured. '
                'Honouring preferred_entity_classifications: %s. The ontology still '
                'supplies the vocabulary block in the extraction prompt, but its classes '
                'will not be offered as preferred classifications. Remove '
                'preferred_entity_classifications to seed that slot from the ontology.',
                extraction_config.preferred_entity_classifications
            )
            return extraction_config.preferred_entity_classifications

        return extraction_config.ontology.ontology.class_names()

    def extract(
            self,
            nodes: List[BaseNode] = [],
            handler: Optional[NodeHandler] = None,
            checkpoint: Optional[Checkpoint] = None,
            show_progress: Optional[bool] = False,
            progress_monitor: Optional[ProgressMonitor] = None,
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
            progress_monitor (Optional[ProgressMonitor], optional): A monitor to receive progress
                callbacks as documents complete LLM extraction. Receives
                increment_llm_processed_documents and increment_llm_processed_chunks calls at
                document boundaries.
            **kwargs (Any): Additional keyword arguments for pipeline configurations or overrides.

        Example::

            from graphrag_toolkit.lexical_graph import ProgressMonitor, NoOpProgressMonitor

            class MyMonitor(NoOpProgressMonitor):
                def __init__(self):
                    self.docs_done = 0

                def increment_llm_processed_documents(self, count=1):
                    self.docs_done += count
                    print(f"Extracted {self.docs_done} documents")

            monitor = MyMonitor()
            index.extract(nodes, progress_monitor=monitor)
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
            builders=[],
            show_progress=show_progress,
            checkpoint=checkpoint,
            num_workers=1,
            tenant_id=DEFAULT_TENANT_ID,
            **kwargs
        )

        if progress_monitor:
            extraction_monitor = self._create_extraction_monitor_pipe(progress_monitor)
            if handler:
                nodes | extraction_pipeline | extraction_monitor | Pipe(handler.accept) | build_pipeline | sink
            else:
                nodes | extraction_pipeline | extraction_monitor | build_pipeline | sink
        else:
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
            progress_monitor: Optional[ProgressMonitor] = None,
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
            progress_monitor (Optional[ProgressMonitor]): A monitor to receive progress
                callbacks as documents complete graph and vector indexing. Receives
                increment_graph_processed_documents, increment_graph_processed_chunks,
                increment_vector_processed_documents, and increment_vector_processed_chunks
                calls at document batch boundaries. Graph and vector increments fire together
                since both stages execute within the same worker process.
            **kwargs (Any): Additional keyword arguments for extending or customizing the
                pipeline behavior.

        Returns:
            None
        """

        build_config = self.indexing_config.build

        enable_versioning =  coalesce(kwargs.get('enable_versioning', None), build_config.enable_versioning, GraphRAGConfig.enable_versioning)

        components = []

        if enable_versioning:
            components.append(VersionManager.for_graph_and_vector_store(self.graph_store, self.vector_store))

        components.extend([
            GraphConstruction.for_graph_store(self.graph_store),
            VectorIndexing.for_vector_store(self.vector_store)
        ])

        build_pipeline = BuildPipeline.create(
            components=components,
            show_progress=show_progress,
            checkpoint=checkpoint,
            build_filters=build_config.build_filters,
            source_metadata_formatter=build_config.source_metadata_formatter,
            include_domain_labels=build_config.include_domain_labels,
            include_local_entities=build_config.include_local_entities,
            typed_properties=self._typed_properties(),
            tenant_id=self.tenant_id,
            progress_monitor=progress_monitor,
            **kwargs
        )

        sink_fn = sink if not handler else Pipe(handler)
        nodes | build_pipeline | sink_fn

    def extract_and_build(
            self,
            nodes: List[BaseNode] = [],
            handler: Optional[NodeHandler] = None,
            checkpoint: Optional[Checkpoint] = None,
            show_progress: Optional[bool] = False,
            progress_monitor: Optional[ProgressMonitor] = None,
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
            progress_monitor (Optional[ProgressMonitor]): A monitor to receive progress
                callbacks across both extraction and build stages. The same instance receives
                LLM increment calls during extraction, then graph/vector increment calls
                during build.
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

        enable_versioning =  coalesce(kwargs.get('enable_versioning', None), build_config.enable_versioning, GraphRAGConfig.enable_versioning)

        build_components = []

        if enable_versioning:
            build_components.append(VersionManager.for_graph_and_vector_store(self.graph_store, self.vector_store))

        build_components.extend([
            GraphConstruction.for_graph_store(self.graph_store),
            VectorIndexing.for_vector_store(self.vector_store)
        ])
        
        build_pipeline = BuildPipeline.create(
            components=build_components,
            show_progress=show_progress,
            checkpoint=checkpoint,
            build_filters=build_config.build_filters,
            source_metadata_formatter=build_config.source_metadata_formatter,
            include_domain_labels=build_config.include_domain_labels,
            include_local_entities=build_config.include_local_entities,
            typed_properties=self._typed_properties(),
            tenant_id=self.tenant_id,
            progress_monitor=progress_monitor,
            **kwargs
        )

        sink_fn = sink if not handler else Pipe(handler)
        if progress_monitor:
            extraction_monitor = self._create_extraction_monitor_pipe(progress_monitor)
            nodes | extraction_pipeline | extraction_monitor | build_pipeline | sink_fn
        else:
            nodes | extraction_pipeline | build_pipeline | sink_fn

    @staticmethod
    def _create_extraction_monitor_pipe(progress_monitor: ProgressMonitor) -> Pipe:
        def _monitor_extraction(source_documents):
            for doc in source_documents:
                # TODO: Chunk-level reporting is batched at document boundaries.
                # All chunks for a document are reported at once, not as each chunk
                # finishes within a worker process. A future improvement could use
                # multiprocessing-safe mechanisms for true per-chunk progress.
                try:
                    progress_monitor.increment_llm_processed_documents(1)
                    progress_monitor.increment_llm_processed_chunks(len(doc.nodes))
                except Exception:
                    logger.warning("ProgressMonitor raised an exception during extraction tracking", exc_info=True)
                yield doc
        return Pipe(_monitor_extraction)

    def get_stats(self) -> Dict[str, Any]:

        stats = {}

        labels = ['Source', 'Chunk', 'Topic', 'Statement', 'Fact', 'Entity']

        for label in labels:
            cypher = f'MATCH (n:`__{label}__`) RETURN count(n) AS count'
            results = self.graph_store.execute_query(cypher)
            stats[label.lower()] = results[0]['count']
        
        cypher = """MATCH (t:`__Topic__`)-[r:`__MENTIONED_IN__`]->()
        WITH t, count(r) AS connectingNumChunks WHERE connectingNumChunks > 1
        RETURN count(t) AS numTopics, connectingNumChunks ORDER BY connectingNumChunks DESC"""

        results = self.graph_store.execute_query(cypher)

        stats['numChunksPerTopic'] = results

        cypher = """MATCH (f:`__Fact__`)-[r:`__SUPPORTS__`]->()
        WITH f, count(r) AS connectingNumStatements WHERE connectingNumStatements > 1
        RETURN count(f) AS numFacts, connectingNumStatements ORDER BY connectingNumStatements DESC"""

        results = self.graph_store.execute_query(cypher)

        stats['numStatementsPerFact'] = results

        localConnectivity = 0
        globalConnectivity = 0

        if stats['topic'] > 0:
            localConnectivity = round(
                sum([i['connectingNumChunks'] * i['numTopics'] for i in stats['numChunksPerTopic']]) / stats['topic'], 
                5
            )
        
        if stats['fact'] > 0:
            globalConnectivity = round(
                sum([i['connectingNumStatements'] * i['numFacts'] for i in stats['numStatementsPerFact']]) / stats['fact'], 
                5
            )

        stats['localConnectivity'] = localConnectivity
        stats['globalConnectivity'] = globalConnectivity

        return stats

    @overload
    def get_sources(self, 
                    source_id:str=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...
    
    @overload
    def get_sources(self, 
                    source_ids:List[str]=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    @overload
    def get_sources(self, 
                    filter:FilterConfig=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    @overload
    def get_sources(self, 
                    filter:Dict[str, Any]=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    @overload
    def get_sources(self, 
                    filter:List[Dict[str, Any]]=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    def get_sources(self,
                    source_id:str=None,
                    source_ids:List[str]=None,
                    filter:Union[FilterConfig, Dict[str, Any], List[Dict[str, Any]]]=None,
                    versioning_config:VersioningConfig=None,
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:

        source_where_clause = None
        metadata_where_clause = None
        order_by_clause = ''
        parameters = {}

        if order_by:
            order_by = [order_by] if isinstance(order_by, str) else order_by
            order_by_clause = ' '.join([f'result.metadata.{o},' for o in order_by])

        order_by_clause = f'ORDER BY {order_by_clause} result.versioning.valid_from ASC'

        source_ids = source_id or source_ids
        
        if source_ids is not None:
            source_ids = [source_ids] if isinstance(source_ids, str) else source_ids
            source_where_clause = f'({self.graph_store.node_id("source.sourceId")} in $sourceIds)'
            parameters['sourceIds'] = source_ids

        filter = filter or FilterConfig()
        versioning_config = versioning_config or VersioningConfig()
        
        filter_config = to_metadata_filter(filter)
        filter_config = versioning_config.apply(filter_config)
        
        metadata_where_clause = filter_config_to_opencypher_filters(filter_config)

        where_clause = ''
        if source_where_clause and metadata_where_clause:
            where_clause = f'WHERE {source_where_clause} AND {metadata_where_clause}'
        elif source_where_clause:
            where_clause = f'WHERE {source_where_clause}'
        elif metadata_where_clause:
            where_clause = f'WHERE {metadata_where_clause}'

        cypher = f'''// get source info from source ids
        MATCH (source:`__Source__`)
        {where_clause}
        RETURN {{ 
            sourceId: {self.graph_store.node_id("source.sourceId")}, 
            metadata: properties(source), 
            versioning: {{
                valid_from: coalesce(source.{VALID_FROM}, {TIMESTAMP_LOWER_BOUND}), 
                valid_to: coalesce(source.{VALID_TO}, {TIMESTAMP_LOWER_BOUND}),
                extract_timestamp: coalesce(source.{EXTRACT_TIMESTAMP}, {TIMESTAMP_LOWER_BOUND}),
                build_timestamp: coalesce(source.{BUILD_TIMESTAMP}, {TIMESTAMP_LOWER_BOUND}),
                id_fields: split(coalesce(source.{VERSION_INDEPENDENT_ID_FIELDS}, ""), ";"),
                prev_versions: split(coalesce(source.{PREV_VERSIONS}, ""), ";")
            }}  
        }} AS result {order_by_clause}
        '''

        results = self.graph_store.execute_query(cypher, parameters)

        def reformat(source):
            
            for key in VERSIONING_METADATA_KEYS:
                if key in source['metadata']:
                    del source['metadata'][key]

            return source

        return [reformat(result['result']) for result in results]
    
    @overload
    def delete_sources(self, 
                    source_id:str=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...
    
    @overload
    def delete_sources(self, 
                    source_ids:List[str]=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    @overload
    def delete_sources(self, 
                    filter:FilterConfig=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    @overload
    def delete_sources(self, 
                    filter:Dict[str, Any]=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    @overload
    def delete_sources(self, 
                    filter:List[Dict[str, Any]]=None, 
                    versioning_config:VersioningConfig=None, 
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        ...

    def delete_sources(self,
                    source_id:str=None,
                    source_ids:List[str]=None,
                    filter:Union[FilterConfig, Dict[str, Any], List[Dict[str, Any]]]=None,
                    versioning_config:VersioningConfig=None,
                    order_by:Union[str, List[str]]=None) -> List[Dict[str, Any]]:
        
        sources = self.get_sources(source_id, source_ids, filter, versioning_config)
        source_ids = [s['sourceId'] for s in sources]

        delete_sources = DeleteSources(graph_store=self.graph_store, vector_store=self.vector_store)

        return delete_sources.delete_source_documents(source_ids)
