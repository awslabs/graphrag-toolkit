# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import asyncio
from typing import Tuple, List, Optional, Sequence, Dict

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import parse_extracted_topics, format_list, format_text
from graphrag_toolkit.lexical_graph.indexing.extract.scoped_value_provider import ScopedValueProvider, FixedScopedValueProvider, DEFAULT_SCOPE
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_TOPICS_PROMPT

from llama_index.core.schema import BaseNode
from llama_index.core.bridge.pydantic import Field
from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.prompts import PromptTemplate
from llama_index.core.async_utils import run_jobs

logger = logging.getLogger(__name__)

class TopicExtractor(BaseExtractor):

    llm: Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )
        
    prompt_template: str = Field(
        description='Prompt template'
    )
        
    source_metadata_field: Optional[str] = Field(
        description='Metadata field from which to extract information'
    )

    entity_classification_provider:ScopedValueProvider = Field(
        description='Entity classification provider'
    )

    topic_provider:ScopedValueProvider = Field(
        description='Topic provider'
    )

    @classmethod
    def class_name(cls) -> str:
        return 'TopicExtractor'

    def __init__(self, 
                 llm:LLMCacheType=None,
                 prompt_template=None,
                 source_metadata_field=None,
                 num_workers:Optional[int]=None,
                 entity_classification_provider=None,
                 topic_provider=None
                 ):
                 
        super().__init__(
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or EXTRACT_TOPICS_PROMPT, 
            source_metadata_field=source_metadata_field,
            num_workers=num_workers or GraphRAGConfig.extraction_num_threads_per_worker,
            entity_classification_provider=entity_classification_provider or FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: DEFAULT_ENTITY_CLASSIFICATIONS}),
            topic_provider=topic_provider or FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: []})
        )

        logger.debug(f'Prompt template: {self.prompt_template}')
    
    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        fact_entries = await self._extract_for_nodes(nodes)
        return [fact_entry for fact_entry in fact_entries]
    
    async def _extract_for_nodes(self, nodes):    
        jobs = [
            self._extract_for_node(node) for node in nodes
        ]
        return await run_jobs(
            jobs, 
            show_progress=self.show_progress, 
            workers=self.num_workers, 
            desc=f'Extracting topics [nodes: {len(nodes)}, num_workers: {self.num_workers}]'
        )
        
    def _get_metadata_or_default(self, metadata, key, default):
        value = metadata.get(key, default)
        return value or default
        
    async def _extract_for_node(self, node):

        logger.debug(f'Extracting topics for node {node.node_id}')
        
        (entity_classification_scope, current_entity_classifications) = self.entity_classification_provider.get_current_values(node)
        (topic_scope, current_topics) = self.topic_provider.get_current_values(node)
        
        text = format_text(self._get_metadata_or_default(node.metadata, self.source_metadata_field, node.text) if self.source_metadata_field else node.text)
        (topics, garbage) = await self._extract_topics(text, current_entity_classifications, current_topics)
        
        node_entity_classifications = [
            entity.classification 
            for topic in topics.topics
            for entity in topic.entities
            if entity.classification
        ]
        self.entity_classification_provider.update_values(entity_classification_scope, current_entity_classifications, node_entity_classifications)

        node_topics = [
            topic.value
            for topic in topics.topics
            if topic.value
        ]
        self.topic_provider.update_values(topic_scope, current_topics, node_topics)
        
        return {
            TOPICS_KEY: topics.model_dump()
        }
            
    async def _extract_topics(self, text:str, preferred_entity_classifications:List[str], preferred_topics:List[str]) -> Tuple[TopicCollection, List[str]]:
        
        def blocking_llm_call():
            return self.llm.predict(
                PromptTemplate(template=self.prompt_template),
                text=text,
                preferred_entity_classifications=format_list(preferred_entity_classifications),
                preferred_topics=format_list(preferred_topics)
            )
        
        coro = asyncio.to_thread(blocking_llm_call)
        
        raw_response = await coro

        (topics, garbage) = parse_extracted_topics(raw_response)
        return (topics, garbage)