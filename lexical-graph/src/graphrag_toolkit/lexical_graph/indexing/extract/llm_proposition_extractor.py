# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import asyncio
from typing import List, Optional, Sequence, Dict

from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.model import Propositions
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_PROPOSITIONS_PROMPT

from llama_index.core.schema import BaseNode
from llama_index.core.bridge.pydantic import Field
from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.prompts import PromptTemplate
from llama_index.core.async_utils import run_jobs


logger = logging.getLogger(__name__)

class LLMPropositionExtractor(BaseExtractor):

    llm: Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )
        
    prompt_template: str = Field(
        description='Prompt template'
    )
        
    source_metadata_field: Optional[str] = Field(
        description='Metadata field from which to extract propositions'
    )

    @classmethod
    def class_name(cls) -> str:
        return 'LLMPropositionExtractor'

    def __init__(self, 
                 llm:LLMCacheType=None,
                 prompt_template=None,
                 source_metadata_field=None,
                 num_workers:Optional[int]=None):
                 
        super().__init__(
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or EXTRACT_PROPOSITIONS_PROMPT, 
            source_metadata_field=source_metadata_field,
            num_workers=num_workers or GraphRAGConfig.extraction_num_threads_per_worker
        )

        logger.debug(f'Prompt template: {self.prompt_template}')

    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        proposition_entries = await self._extract_propositions_for_nodes(nodes)
        return [proposition_entry for proposition_entry in proposition_entries]
    
    async def _extract_propositions_for_nodes(self, nodes):    
        jobs = [
            self._extract_propositions_for_node(node) for node in nodes
        ]
        return await run_jobs(
            jobs, 
            show_progress=self.show_progress, 
            workers=self.num_workers, 
            desc=f'Extracting propositions [nodes: {len(nodes)}, num_workers: {self.num_workers}]'
        )
        
    async def _extract_propositions_for_node(self, node):
        logger.debug(f'Extracting propositions for node {node.node_id}')
        text = node.metadata.get(self.source_metadata_field, node.text) if self.source_metadata_field else node.text
        proposition_collection = await self._extract_propositions(text)
        if logger.isEnabledFor(logging.DEBUG):
            s = f"""====================================
text: {text}
------------------------------------
propositions: {proposition_collection}
"""
            logger.debug(s)
            
        return {
            PROPOSITIONS_KEY: proposition_collection.model_dump()['propositions']
        }
            
    async def _extract_propositions(self, text):
        
        def blocking_llm_call():
            return self.llm.predict(
                PromptTemplate(template=self.prompt_template),
                text=text
            )
        
        coro = asyncio.to_thread(blocking_llm_call)
        
        raw_response = await coro

        propositions = raw_response.split('\n')

        unique_propositions = {p : None for p in propositions if p}

        return Propositions(propositions=list(unique_propositions.keys()))
    