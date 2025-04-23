# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import logging
import random
from typing import Sequence, List, Any, Optional

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.extract.infer_config import OnExistingClassifications
from graphrag_toolkit.lexical_graph.indexing.extract.source_doc_parser import SourceDocParser
from graphrag_toolkit.lexical_graph.indexing.extract import ScopedValueStore, DEFAULT_SCOPE
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.prompts import DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT

from llama_index.core.schema import BaseNode
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.bridge.pydantic import Field
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

DEFAULT_NUM_SAMPLES = 5
DEFAULT_NUM_ITERATIONS = 1

class InferClassifications(SourceDocParser):

    classification_store:ScopedValueStore = Field(
        description='Classification store'
    )

    classification_label:str = Field(
        description='Classification label'
    )

    classification_scope:str = Field(
        description='Classification scope'
    )

    num_samples:int = Field(
        description='Number of chunks to sample per iteration'
    )

    num_iterations:int = Field(
        description='Number times to sample documents'
    )

    splitter:Optional[SentenceSplitter] = Field(
        description='Chunk splitter'
    )

    llm: Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )

    prompt_template:str = Field(
        description='Prompt template'
    )

    default_classifications:List[str] = Field(
        'Default classifications'
    )

    merge_action:OnExistingClassifications = Field(
        'Action to take if there are existing classifications'
    )

    def __init__(self,
                 classification_store:ScopedValueStore,
                 classification_label:str,
                 classification_scope:Optional[str]=None,
                 num_samples:Optional[int]=None, 
                 num_iterations:Optional[int]=None,
                 splitter:Optional[SentenceSplitter]=None,
                 llm:Optional[LLMCacheType]=None,
                 prompt_template:Optional[str]=None,
                 default_classifications:Optional[List[str]]=None,
                 merge_action:Optional[OnExistingClassifications]=None
            ):
        
        super().__init__(
            classification_store=classification_store,
            classification_label=classification_label,
            classification_scope=classification_scope or DEFAULT_SCOPE,
            num_samples=num_samples or DEFAULT_NUM_SAMPLES,
            num_iterations=num_iterations or DEFAULT_NUM_ITERATIONS,
            splitter=splitter,
            llm=llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            default_classifications=default_classifications or DEFAULT_ENTITY_CLASSIFICATIONS,
            merge_action=merge_action or OnExistingClassifications.RETAIN_EXISTING
        )

        logger.debug(f'Prompt template: {self.prompt_template}')

    def _parse_classifications(self, response_text:str) -> Optional[List[str]]:

        pattern = r'<entity_classifications>(.*?)</entity_classifications>'
        match = re.search(pattern, response_text, re.DOTALL)

        classifications = []

        if match:
            classifications.extend([
                line.strip() 
                for line in match.group(1).strip().split('\n') 
                if line.strip()
            ])
                
        if classifications:
            logger.info(f'Successfully parsed {len(classifications)} domain-specific classifications')
            return classifications
        else:
            logger.warning(f'Unable to parse classifications from response: {response_text}')
            return classifications
            
       
    def _parse_nodes(
        self,
        nodes: Sequence[BaseNode],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> List[BaseNode]:
        
        current_values = self.classification_store.get_scoped_values(self.classification_label, self.classification_scope)
        if current_values and self.merge_action == OnExistingClassifications.RETAIN_EXISTING:
            logger.info(f'Domain-specific classifications already exist [label: {self.classification_label}, scope: {self.classification_scope}, classifications: {current_values}]')
            return nodes

        chunks = self.splitter(nodes) if self.splitter else nodes

        classifications = set()

        for i in range(1, self.num_iterations + 1):

            sample_chunks = random.sample(chunks, self.num_samples) if len(chunks) > self.num_samples else chunks

            logger.info(f'Analyzing {len(sample_chunks)} chunks for domain adaptation [iteration: {i}, merge_action: {self.merge_action}]')

            formatted_chunks = '\n'.join(f'<chunk>{chunk.text}</chunk>' for chunk in sample_chunks)
                
            response = self.llm.predict(
                PromptTemplate(self.prompt_template),
                text_chunks=formatted_chunks
            )

            classifications.update(self._parse_classifications(response))

        if current_values and self.merge_action == OnExistingClassifications.MERGE_EXISTING:
            classifications.update(current_values)
            
        classifications = list(classifications)

        if classifications:
            logger.info(f'Domain adaptation succeeded [label: {self.classification_label}, scope: {self.classification_scope}, classifications: {classifications}]')
            self.classification_store.save_scoped_values(self.classification_label, self.classification_scope, classifications)
        else:
            logger.warning(f'Domain adaptation failed, using default classifications [label: {self.classification_label}, scope: {self.classification_scope}, classifications: {self.default_classifications}]')
            self.classification_store.save_scoped_values(self.classification_label, self.classification_scope, self.default_classifications)

        return nodes
    
    def _parse_source_docs(self, source_documents):

        source_docs = [
            source_doc for source_doc in source_documents
        ]

        nodes = [
            n
            for sd in source_docs
            for n in sd.nodes
        ]

        self._parse_nodes(nodes)

        return source_docs