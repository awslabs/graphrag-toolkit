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
from llama_index.core.schema import NodeRelationship


logger = logging.getLogger(__name__)

class LLMPropositionExtractor(BaseExtractor):
    """Handles proposition extraction using a language model (LLM).

    This class implements functionality to extract propositions from input
    text using a specified language model (LLM). It can process nodes
    individually or in batches and extracts propositions based on a prompt
    template and optional metadata fields. The extraction process utilizes
    asynchronous methods to achieve efficient parallel processing.

    Attributes:
        llm (Optional[LLMCache]): The LLM instance used for proposition
            extraction. If not provided, a default LLM configuration is used.
        prompt_template (str): The template used to construct prompts for
            proposition extraction.
        source_metadata_field (Optional[str]): The metadata field in the input
            nodes from which propositions are extracted. If not specified,
            the node text is used instead.
    """
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
        """
        Extracts and provides the class name of the given class.

        Returns:
            str: A string representing the name of the class.
        """
        return 'LLMPropositionExtractor'

    def __init__(self, 
                 llm:LLMCacheType=None,
                 prompt_template=None,
                 source_metadata_field=None,
                 num_workers:Optional[int]=None):
        """
        Initializes the class with configuration options for processing language model outputs.

        This constructor allows setting of the language model, prompt template, source metadata
        field, and the number of workers to perform parallel processing.

        Args:
            llm: Language model cache or configuration for language model interaction.
            prompt_template: Template for the prompt to guide the language model's response
                generation.
            source_metadata_field: Field name key to store or retrieve associated metadata
                from source data.
            num_workers: Number of worker threads to use for processing tasks.
        """
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
        """
        Asynchronously extracts proposition entries from the given nodes.

        This method processes a sequence of nodes to extract propositions associated
        with each node. The result is a list of dictionaries, where each dictionary
        represents the proposition data for a specific node.

        Args:
            nodes: Sequence of nodes from which propositions should be extracted.

        Returns:
            A list of dictionaries containing proposition data for the given nodes.
        """
        proposition_entries = await self._extract_propositions_for_nodes(nodes)
        return [proposition_entry for proposition_entry in proposition_entries]
    
    async def _extract_propositions_for_nodes(self, nodes):
        """
        Asynchronously extracts propositions for multiple nodes.

        This method schedules asynchronous jobs for extracting propositions for
        each node in the given list of nodes and executes them concurrently. The
        number of workers and whether to show progress are parameters that control
        the execution behavior. A progress description is displayed when jobs
        are being processed.

        Args:
            nodes: A list of nodes for which propositions need to be extracted.

        Returns:
            A list of results from the processed jobs. Results correspond to the
            propositions extracted for each node in the input list.
        """
        jobs = [
            self._extract_propositions_for_node(node) for node in nodes
        ]
        return await run_jobs(
            jobs, 
            show_progress=self.show_progress, 
            workers=self.num_workers, 
            desc=f'Extracting propositions [nodes: {len(jobs)}, num_workers: {self.num_workers}]'
        )
        
    async def _extract_propositions_for_node(self, node):
        """
        Extracts propositions for a given node by analyzing its associated text. The text
        is determined based on whether the `source_metadata_field` attribute is set. If
        set, the metadata value of the specified field is used; otherwise, the node's text
        is used. The method delegates the actual extraction of propositions to the
        `_extract_propositions` method and logs debug information if logging is enabled.
        The extracted propositions are returned in a dictionary with a predefined key.

        Args:
            node (Node): The node object for which propositions will be extracted. The
                node must include metadata and text attributes.

        Returns:
            dict: A dictionary containing extracted propositions under the key
                `PROPOSITIONS_KEY`.
        """
        logger.debug(f'Extracting propositions for node {node.node_id}')
        text = node.metadata.get(self.source_metadata_field, node.text) if self.source_metadata_field else node.text

        source = node.relationships.get(NodeRelationship.SOURCE, None)
        if source:
            source_info = '\n'.join([str(v) for v in source.metadata.values()])
        else:
            source_info = ''

        proposition_collection = await self._extract_propositions(text, source_info)
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
            
    async def _extract_propositions(self, text, source_info):
        """
        Extracts unique propositions from the given text asynchronously.

        This method interacts with a large language model (LLM) to extract a list of
        unique propositions based on a provided text input. The process involves
        executing a blocking LLM call in an asynchronous-friendly manner through the
        use of asyncio's to_thread. The resulting LLM response is then split into
        lines, and duplicate propositions are filtered out to ensure uniqueness.

        Args:
            text: The input string for which propositions need to be extracted.

        Returns:
            Propositions: An object containing a list of unique propositions extracted
            from the text.
        """
        def blocking_llm_call():
            return self.llm.predict(
                PromptTemplate(template=self.prompt_template),
                text=text,
                source_info=source_info,
                exclude_cache_keys=['source_info']
            )
        
        coro = asyncio.to_thread(blocking_llm_call)
        
        raw_response = await coro

        propositions = raw_response.split('\n')

        unique_propositions = {p : None for p in propositions if p}

        return Propositions(propositions=list(unique_propositions.keys()))
    