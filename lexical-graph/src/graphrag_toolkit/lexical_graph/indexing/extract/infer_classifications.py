# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
import logging
import random
from typing import Sequence, List, Any, Optional

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.extract.source_doc_parser import SourceDocParser
from graphrag_toolkit.lexical_graph.indexing.extract.preferred_values import PreferredValuesProvider
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.prompts import DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
from graphrag_toolkit.lexical_graph.indexing.prompts import RANK_ENTITY_CLASSIFICATIONS_PROMPT
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

from llama_index.core.schema import BaseNode
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.bridge.pydantic import Field
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

DEFAULT_NUM_SAMPLES = 5
DEFAULT_NUM_ITERATIONS = 1
DEFAULT_NUM_CLASSIFICATIONS = 15

class InferClassifications(SourceDocParser, PreferredValuesProvider):

    num_samples:int = Field(
        description='Number of chunks to sample per iteration'
    )

    num_iterations:int = Field(
        description='Number times to sample documents'
    )

    num_classifications:int = Field(
        description='Number of classifications to return'
    )

    splitter:Optional[SentenceSplitter] = Field(
        description='Chunk splitter'
    )

    llm:Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )

    prompt_template:str = Field(
        description='Prompt template'
    )

    rank_prompt_template:str = Field(
        description='Prompt template'
    )

    classifications:List[str] = Field(
        'classifications'
    )

    default_classifications:List[str] = Field(
        'Default classifications'
    )

    replace_default_classifications:bool = Field(
        'Replace default classifications with new classifications after each cycle'
    )

    def __init__(self,
                 num_samples:Optional[int]=None, 
                 num_iterations:Optional[int]=None,
                 num_classifications:Optional[int]=None,
                 splitter:Optional[SentenceSplitter]=None,
                 llm:Optional[LLMCacheType]=None,
                 prompt_template:Optional[str]=None,
                 rank_prompt_template:Optional[str]=None,
                 default_classifications:Optional[List[str]]=DEFAULT_ENTITY_CLASSIFICATIONS,
                 replace_default_classifications:Optional[bool]=False   
            ):
        
        super().__init__(
            num_samples=coalesce(num_samples, DEFAULT_NUM_SAMPLES),
            num_iterations=coalesce(num_iterations, DEFAULT_NUM_ITERATIONS),
            num_classifications=coalesce(num_classifications, DEFAULT_NUM_CLASSIFICATIONS),
            splitter=splitter,
            llm=llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            rank_prompt_template=rank_prompt_template or RANK_ENTITY_CLASSIFICATIONS_PROMPT,
            default_classifications=[] if default_classifications is None else default_classifications,
            replace_default_classifications=replace_default_classifications
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

        chunks = self.splitter(nodes) if self.splitter else nodes

        classifications = set()

        logger.info(f'Default classifications: {self.default_classifications}')   

        for i in range(1, self.num_iterations + 1):

            sample_chunks = random.sample(chunks, self.num_samples) if len(chunks) > self.num_samples else chunks

            logger.info(f'Analyzing {len(sample_chunks)} chunks for domain adaptation [iteration: {i}]')

            formatted_chunks = '\n'.join(f'<chunk>{chunk.text}</chunk>' for chunk in sample_chunks)
                
            response = self.llm.predict(
                PromptTemplate(self.prompt_template),
                text_chunks=formatted_chunks,
                existing_classifications='\n'.join(self.default_classifications)
            )

            classifications.update(self._parse_classifications(response))

        all_classifications = list(classifications)

        if all_classifications:
            
            formatted_classifications = '\n'.join([c.title() for c in all_classifications])
            response = self.llm.predict(
                PromptTemplate(self.rank_prompt_template),
                classifications=formatted_classifications
            )
            ranked_classifications = self._parse_classifications(response)[:self.num_classifications]

            logger.info(f'Domain adaptation succeeded [all_classifications: {all_classifications}, ranked_classification: {ranked_classifications}]')

            self.classifications = ranked_classifications
            
        else:
            logger.warning(f'Domain adaptation failed, using default classifications: {self.default_classifications}')
            self.classifications = self.default_classifications

        if self.replace_default_classifications:
            self.default_classifications = self.classifications

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
    
    def __call__(self, node:BaseNode) -> List[str]:
        return self.classifications