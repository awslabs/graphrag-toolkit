# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional, Union, Any
from pipe import Pipe

from graphrag_toolkit.lexical_graph.tenant_id import TenantId, TenantIdType, DEFAULT_TENANT_ID, to_tenant_id
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, GraphStoreType
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory, VectorStoreType
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.graph import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore
from graphrag_toolkit.lexical_graph.storage.constants import LEXICAL_GRAPH_LABELS
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing import sink
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.extract import ScopedValueProvider, FixedScopedValueProvider, DEFAULT_SCOPE
from graphrag_toolkit.lexical_graph.indexing.extract import GraphScopedValueStore
from graphrag_toolkit.lexical_graph.indexing.extract import LLMPropositionExtractor, BatchLLMPropositionExtractor
from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor, BatchTopicExtractor
from graphrag_toolkit.lexical_graph.indexing.extract import ExtractionPipeline
from graphrag_toolkit.lexical_graph.indexing.extract import InferClassifications, InferClassificationsConfig
from graphrag_toolkit.lexical_graph.indexing.build import BuildPipeline
from graphrag_toolkit.lexical_graph.indexing.build import VectorIndexing
from graphrag_toolkit.lexical_graph.indexing.build import GraphConstruction
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint
from graphrag_toolkit.lexical_graph.indexing.build import BuildFilter
from graphrag_toolkit.lexical_graph.indexing.build.null_builder import NullBuilder

from llama_index.core.node_parser import SentenceSplitter, NodeParser
from llama_index.core.schema import BaseNode, NodeRelationship

DEFAULT_EXTRACTION_DIR = 'output'

logger = logging.getLogger(__name__)

class ExtractionConfig():
    def __init__(self, 
                 enable_proposition_extraction:bool=True,
                 preferred_entity_classifications:List[str]=DEFAULT_ENTITY_CLASSIFICATIONS,
                 infer_entity_classifications:Union[InferClassificationsConfig, bool]=False,
                 extract_propositions_prompt_template:Optional[str]=None,
                 extract_topics_prompt_template:Optional[str]=None):
        
        self.enable_proposition_extraction = enable_proposition_extraction
        self.preferred_entity_classifications = preferred_entity_classifications
        self.infer_entity_classifications = infer_entity_classifications
        self.extract_propositions_prompt_template = extract_propositions_prompt_template
        self.extract_topics_prompt_template = extract_topics_prompt_template

class BuildConfig():
    def __init__(self,
                 filter:Optional[BuildFilter]=None,
                 include_domain_labels:Optional[bool]=None):
        self.filter = filter
        self.include_domain_labels = include_domain_labels
        
class IndexingConfig():
    def __init__(self,
                 chunking:Optional[List[NodeParser]]=[],
                 extraction:Optional[ExtractionConfig]=None,
                 build:Optional[BuildConfig]=None,
                 batch_config:Optional[BatchConfig]=None):
        
        if chunking is not None and len(chunking) == 0:
            chunking.append(SentenceSplitter(chunk_size=256, chunk_overlap=20))
        
        self.chunking = chunking # None = no chunking
        self.extraction = extraction or ExtractionConfig()
        self.build = build or BuildConfig()
        self.batch_config = batch_config # None = do not use batch inference

def get_topic_scope(node:BaseNode):
    source = node.relationships.get(NodeRelationship.SOURCE, None)
    if not source:
        return DEFAULT_SCOPE
    else:
        return source.node_id

class LexicalGraphIndex():
    """
    Extracts graph elements from source documents and builds a graph and vector index.

    Args:
        graph_store (Optional[GraphStoreType], optional):
            GraphStore instance or GraphStore connection string. If None, defaults to a DummyGraphStore.
        vector_store (Optional[VectorStoreType], optional):
            VectorStore instance or VectorStore connection string. If None, defaults to a VectorStore with DummyVectorIndexes.
        tenant_id (TenantId, optional):
            Tenant id. Defaults to None (default tenant).
        extraction_dir (List[TransformComponent], optional):
            Directory to which intermediate artefacts (e.g. checkpoints) will be written. Defaults to DEFAULT_EXTRACTION_DIR.
        indexing_config (Optional[IndexingConfig], optional):
            If None, defaults to using default IndexingConfig values.

    Examples:
        ```python
        from graphrag_toolkit.lexical_graph import TopicGraphIndex

        index = TopicGraphIndex(
            'neptune-db://gr-1123456789.cluster-abcdefghijk.us-east-1.neptune.amazonaws.com', 
            'aoss://https://abcdefghijk.us-east-1.aoss.amazonaws.com'
        )
        index.extract_and_build(documents, show_progress=True)
        ```
    """

    def __init__(
            self,
            graph_store:Optional[GraphStoreType]=None,
            vector_store:Optional[VectorStoreType]=None,
            tenant_id:Optional[TenantIdType]=None,
            extraction_dir:Optional[str]=None,
            indexing_config:Optional[IndexingConfig]=None,
        ):

        tenant_id = to_tenant_id(tenant_id)

        self.graph_store =  MultiTenantGraphStore.wrap(GraphStoreFactory.for_graph_store(graph_store), tenant_id) 
        self.vector_store = MultiTenantVectorStore.wrap(VectorStoreFactory.for_vector_store(vector_store), tenant_id)
        self.tenant_id = tenant_id or TenantId()
        self.extraction_dir = extraction_dir or DEFAULT_EXTRACTION_DIR
        self.indexing_config = indexing_config or IndexingConfig()

        (pre_processors, components) = self._configure_extraction_pipeline(self.indexing_config)

        self.extraction_pre_processors = pre_processors
        self.extraction_components = components
        self.allow_batch_inference = self.indexing_config.batch_config is not None


    def _configure_extraction_pipeline(self, config:IndexingConfig):
        
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

        entity_classification_provider = None
        topic_provider = None

        classification_label = 'EntityClassification'
        classification_scope = DEFAULT_SCOPE
        
        if isinstance(self.graph_store, DummyGraphStore):
            entity_classification_provider = FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: config.extraction.preferred_entity_classifications})
            topic_provider = FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: []})
        else:
            initial_scope_values = [] if config.extraction.infer_entity_classifications else config.extraction.preferred_entity_classifications
            entity_classification_provider = ScopedValueProvider(
                label=classification_label,
                scoped_value_store=GraphScopedValueStore(graph_store=self.graph_store),
                initial_scoped_values = { classification_scope: initial_scope_values }
            )           
            topic_provider = ScopedValueProvider(
                label='StatementTopic',
                scoped_value_store=GraphScopedValueStore(graph_store=self.graph_store),
                scope_func=get_topic_scope
            )

        if config.extraction.infer_entity_classifications:
            infer_config = config.extraction.infer_entity_classifications if isinstance(config.extraction.infer_entity_classifications, InferClassificationsConfig) else InferClassificationsConfig()
            pre_processors.append(InferClassifications(
                classification_label=classification_label,
                classification_scope=classification_scope,
                classification_store=GraphScopedValueStore(graph_store=self.graph_store),
                splitter=SentenceSplitter(chunk_size=256, chunk_overlap=20) if config.chunking else None,
                default_classifications=config.extraction.preferred_entity_classifications,
                num_samples=infer_config.num_samples,
                num_iterations=infer_config.num_iterations,
                merge_action=infer_config.on_existing_classifications,
                prompt_template=infer_config.prompt_template
            ))

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
            nodes:List[BaseNode]=[],
            handler:Optional[NodeHandler]=None,
            checkpoint:Optional[Checkpoint]=None,
            show_progress:Optional[bool]=False,
            **kwargs:Any) -> None:
        """
        Run a series of transformations on a set of nodes to extract graph elements.

        Args:
            nodes (List[BaseNode], optional): Set of nodes from which graph elements are to be extracted.
            handler (Optional[NodeHandler], optional): Handles nodes emitted at the end of the series of transformations. 
            checkpoint (Optional[Checkpoint], optional): Nodes that are successfully processed by all transformations are 
                checkpointed to the extraction directory, so that they are not reprocessed on subsequent invocations.
            show_progress (bool, optional): Shows execution progress bar(s). Defaults to False.

        Examples:
        ```python
        from graphrag_toolkit.lexical_graph.indexing.load import FileBasedChunks

        index.extract(documents, handler=FileBasedChunks('./output/file-based-chunks/'), show_progress=True)
        ```
        """

        if not self.tenant_id.is_default_tenant():
            logger.warning('TenantId has been set to non-default tenant id, but extraction will use default tenant id')

        extraction_pipeline = ExtractionPipeline.create(
            components=self.extraction_components,
            pre_processors=self.extraction_pre_processors,
            show_progress=show_progress,
            checkpoint=checkpoint,
            num_workers=1 if self.allow_batch_inference else None,
            tenant_id=DEFAULT_TENANT_ID,
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
            nodes:List[BaseNode]=[],
            handler:Optional[NodeHandler]=None,
            checkpoint:Optional[Checkpoint]=None,
            show_progress:Optional[bool]=False,
            **kwargs:Any) -> None:
        """
        Build a graph and vector index from previously extracted nodes.

        Args:
            nodes (List[BaseNode], optional): Set of previously extracted nodes.
            handler (Optional[NodeHandler], optional): Handles nodes emitted at the end of the build process.
            checkpoint (Optional[Checkpoint], optional): Nodes that are successfully processed by all stages of the build process
                are checkpointed to the extraction directory, so that they are not reprocessed on subsequent invocations.
            show_progress (bool, optional): Shows execution progress bar(s). Defaults to False.

        Examples:
        ```python
        from graphrag_toolkit.lexical_graph.indexing.load import FileBasedChunks

        index.build(FileBasedChunks('./output/file-based-chunks/'), show_progress=True)
        ```
        """

        build_pipeline = BuildPipeline.create(
            components=[
                GraphConstruction.for_graph_store(self.graph_store),
                VectorIndexing.for_vector_store(self.vector_store)
            ],
            show_progress=show_progress,
            checkpoint=checkpoint,
            filter=self.indexing_config.build.filter,
            include_domain_labels=self.indexing_config.build.include_domain_labels,
            tenant_id=self.tenant_id,
            **kwargs
        )

        sink_fn = sink if not handler else Pipe(handler.accept)
        nodes | build_pipeline | sink_fn
        
    def extract_and_build(
            self, 
            nodes:List[BaseNode]=[], 
            handler:Optional[NodeHandler]=None,
            checkpoint:Optional[Checkpoint]=None,
            show_progress:Optional[bool]=False,
            **kwargs:Any
        ) -> None:
        """
        Extract graph elements from a set of nodes and then build a graph and vector index.

        Args:
            nodes (List[BaseNode], optional): Set of nodes from which graph elements are to be extracted.
            handler (Optional[NodeHandler], optional): Handles nodes emitted at the end of the build process.
            checkpoint (Optional[Checkpoint], optional): Nodes that are successfully processed by all stages of the build process
                are checkpointed to the extraction directory, so that they are not reprocessed on subsequent invocations.
            show_progress (bool, optional): Shows execution progress bar(s). Defaults to False.

        Examples:
        ```python
        index.extract_and_build(documents, show_progress=True)
        ```
        """

        if not self.tenant_id.is_default_tenant():
            logger.warning('TenantId has been set to non-default tenant id, but extraction will use default tenant id')

        extraction_pipeline = ExtractionPipeline.create(
            components=self.extraction_components,
            pre_processors=self.extraction_pre_processors,
            show_progress=show_progress,
            checkpoint=checkpoint,
            num_workers=1 if self.allow_batch_inference else None,
            tenant_id=DEFAULT_TENANT_ID,
            **kwargs
        )
        
        build_pipeline = BuildPipeline.create(
            components=[
                GraphConstruction.for_graph_store(self.graph_store),
                VectorIndexing.for_vector_store(self.vector_store)
            ],
            show_progress=show_progress,
            checkpoint=checkpoint,
            filter=self.indexing_config.build.filter,
            include_domain_labels=self.indexing_config.build.include_domain_labels,
            tenant_id=self.tenant_id,
            **kwargs
        )

        sink_fn = sink if not handler else Pipe(handler.accept)
        nodes | extraction_pipeline | build_pipeline | sink_fn