# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import asyncio
from typing import Tuple, List, Optional, Sequence, Dict

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import parse_extracted_topics, format_list, format_text
from graphrag_toolkit.lexical_graph.indexing.extract.preferred_values import PreferredValuesProvider, default_preferred_values
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
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

    entity_classification_provider:PreferredValuesProvider = Field(
        description='Entity classification provider'
    )

    topic_provider:PreferredValuesProvider = Field(
        description='Topic provider'
    )

    @classmethod
    def class_name(cls) -> str:
        """
        Returns the name of the class as a string.

        The class_name method is a convenient way to retrieve the name of the class it
        is called on. It is designed to be a class-level method and provides a means of
        returning a standardized name for the class, which can be useful for logging,
        debugging, or any functionality that requires the identification of the class.

        Returns:
            str: The name of the class, which is 'TopicExtractor' in this case.
        """
        return 'TopicExtractor'

    def __init__(self, 
                 llm:LLMCacheType=None,
                 prompt_template=None,
                 source_metadata_field=None,
                 num_workers:Optional[int]=None,
                 entity_classification_provider=None,
                 topic_provider=None
                 ):
        """
        Initializes the instance with the provided or default parameters to facilitate
        operations with LLMCache, prompt templates, source metadata fields, multiple
        workers, and providers for entity classification and topics.

        Args:
            llm (LLMCacheType, optional): The large language model cache used for
                extraction purposes. Defaults to an LLMCache instance configured with
                the extraction LLM and caching behavior.
            prompt_template (str, optional): Prompt template used for topic
                extraction. Defaults to a predefined extraction prompt.
            source_metadata_field (str, optional): Metadata field from the source
                to extract information. If not provided, it will be set to None.
            num_workers (int, optional): Number of worker threads for processing.
                Defaults to a value defined in GraphRAGConfig for threads per worker.
            entity_classification_provider (FixedScopedValueProvider, optional):
                Provider for entity classification data. Defaults to a fixed-scoped
                value provider initialized with default entity classifications.
            topic_provider (FixedScopedValueProvider, optional): Provider for topics.
                Defaults to a fixed-scoped value provider initialized with an empty
                list.
        """
        super().__init__(
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or EXTRACT_TOPICS_PROMPT, 
            source_metadata_field=source_metadata_field,
            num_workers=num_workers or GraphRAGConfig.extraction_num_threads_per_worker,
            entity_classification_provider=entity_classification_provider or default_preferred_values([]),
            topic_provider=topic_provider or default_preferred_values([])
        )

        logger.debug(f'Prompt template: {self.prompt_template}')
    
    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        fact_entries = await self._extract_for_nodes(nodes)
        return [fact_entry for fact_entry in fact_entries]
    
    async def _extract_for_nodes(self, nodes):
        """
        Executes asynchronous extraction tasks for a list of nodes using concurrent workers and
        optionally displays progress.

        Args:
            nodes: A list of nodes for which extraction tasks will be executed.

        Returns:
            A list of results from the executed extraction tasks, maintaining the order of the
            input nodes.
        """
        jobs = [
            self._extract_for_node(node) for node in nodes
        ]
        return await run_jobs(
            jobs, 
            show_progress=self.show_progress, 
            workers=self.num_workers, 
            desc=f'Extracting topics [nodes: {len(jobs)}, num_workers: {self.num_workers}]'
        )
        
    def _get_metadata_or_default(self, metadata, key, default):
        """
        Get the value associated with a key in the metadata or return a default value.

        This function retrieves the value of a specified key from a given metadata
        dictionary. If the key does not exist in the metadata, the provided default
        value is returned. In cases where the retrieved value is considered falsy,
        the default value is returned as a fallback.

        Args:
            metadata (dict): The dictionary containing metadata from which the key
                value is to be fetched.
            key (str): The key for which the value is to be retrieved from the metadata.
            default: The default value to return if the specified key is not present in
                the metadata or the retrieved value is falsy.

        Returns:
            The value associated with the specified key in the metadata if present
            and truthy; otherwise, the default value.
        """
        value = metadata.get(key, default)
        return value or default
        
    async def _extract_for_node(self, node):
        """
        Extracts and processes topics for a given node, leveraging the provided entity classification
        and topic providers. The method analyzes the node's text to identify topics and associated
        entities, updates classifications, and returns structured topic data.

        Args:
            node: The node object containing metadata and text for topic extraction.

        Returns:
            dict: A dictionary containing extracted topics and associated metadata.

        Raises:
            None
        """
        logger.debug(f'Extracting topics for node {node.node_id}')

        preferred_entity_classifications = self.entity_classification_provider(node)
        preferred_topics = self.topic_provider(node)
        
        text = format_text(self._get_metadata_or_default(node.metadata, self.source_metadata_field, node.text) if self.source_metadata_field else node.text)
        (topics, garbage) = await self._extract_topics(text, preferred_entity_classifications, preferred_topics)
        
        return {
            TOPICS_KEY: topics.model_dump()
        }
            
    async def _extract_topics(self, text:str, preferred_entity_classifications:List[str], preferred_topics:List[str]) -> Tuple[TopicCollection, List[str]]:
        """
        Asynchronously extracts topics from the given text by calling a Language Learning
        Model (LLM). The function aims to retrieve topics based on the preferred
        classifications and topics provided. It uses a blocking LLM call in conjunction
        with asyncio's to_thread method to keep the extraction process non-blocking.
        The extracted topics are parsed and returned as a tuple comprising a
        TopicCollection and the remaining unprocessed data.

        Args:
            text (str): The input text from which topics are to be extracted.
            preferred_entity_classifications (List[str]): A list of preferred entity
                classifications to refine the topic extraction process.
            preferred_topics (List[str]): A list of preferred topics to provide more
                targeted results.

        Returns:
            Tuple[TopicCollection, List[str]]: A tuple containing a TopicCollection
            object with extracted topics and a list of unprocessed or residual data.
        """
        def blocking_llm_call():
            return self.llm.predict(
                PromptTemplate(template=self.prompt_template),
                text=text,
                preferred_entity_classifications=format_list(preferred_entity_classifications),
                preferred_topics=format_list(preferred_topics),
                #exclude_cache_keys=['preferred_entity_classifications', 'preferred_topics']
            )
        
        coro = asyncio.to_thread(blocking_llm_call)
        
        raw_response = await coro

        (topics, garbage) = parse_extracted_topics(raw_response)
        return (topics, garbage)