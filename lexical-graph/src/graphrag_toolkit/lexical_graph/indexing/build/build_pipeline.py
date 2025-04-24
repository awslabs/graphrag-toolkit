# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import multiprocessing
import math
from typing import Any, List, Optional, Iterable
from pipe import Pipe

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing import NodeHandler, IdGenerator
from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import run_pipeline
from graphrag_toolkit.lexical_graph.indexing.model import SourceType, SourceDocument, source_documents_from_source_types
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import Checkpoint, CheckpointWriter
from graphrag_toolkit.lexical_graph.indexing.build.metadata_to_nodes import MetadataToNodes
from graphrag_toolkit.lexical_graph.indexing.build.build_filter import BuildFilter

from llama_index.core.utils import iter_batch
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.schema import TransformComponent, BaseNode

logger = logging.getLogger(__name__)

class NodeFilter(TransformComponent):
      
    def __call__(self, nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:
        return nodes

class BuildPipeline():

    @staticmethod
    def create(components: List[TransformComponent], 
               num_workers:Optional[int]=None, 
               batch_size:Optional[int]=None, 
               batch_writes_enabled:Optional[bool]=None, 
               batch_write_size:Optional[int]=None, 
               builders:Optional[List[NodeBuilder]]=[], 
               show_progress=False, 
               checkpoint:Optional[Checkpoint]=None,
               filter:Optional[BuildFilter]=None,
               include_domain_labels:Optional[bool]=None,
               tenant_id:Optional[TenantId]=None,
               **kwargs:Any
            ):
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
                filter=filter,
                include_domain_labels=include_domain_labels,
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
                 filter:Optional[BuildFilter]=None,
                 include_domain_labels:Optional[bool]=None,
                 tenant_id:Optional[TenantId]=None,
                 **kwargs:Any
            ):
        
        components = components or []
        num_workers = num_workers or GraphRAGConfig.build_num_workers
        batch_size = batch_size or GraphRAGConfig.build_batch_size
        batch_writes_enabled = batch_writes_enabled or GraphRAGConfig.batch_writes_enabled
        batch_write_size = batch_write_size or GraphRAGConfig.build_batch_write_size
        include_domain_labels = include_domain_labels or GraphRAGConfig.include_domain_labels
        
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
        self.metadata_to_nodes = MetadataToNodes(builders=builders, filter=filter, id_generator=IdGenerator(tenant_id=tenant_id))
        self.node_filter = NodeFilter() if not checkpoint else checkpoint.add_filter(NodeFilter())
        self.pipeline_kwargs = kwargs
    
    def _to_node_batches(self, source_doc_batches:Iterable[Iterable[SourceDocument]]) -> List[List[BaseNode]]:

        results = []
    
        for source_documents in source_doc_batches:
        
            chunk_node_batches = [
                self.node_filter(source_document.nodes)
                for source_document in source_documents
            ]

            node_batches = [
                self.metadata_to_nodes(chunk_nodes) 
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
                **self.pipeline_kwargs
            )

            for node in output_nodes:
                yield node       

