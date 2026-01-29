# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from typing import Optional

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_TOPICS_PROMPT
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
from graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor import TopicExtractor
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import parse_extracted_topics, format_list, format_text
from graphrag_toolkit.lexical_graph.indexing.extract.preferred_values import PreferredValuesProvider, default_preferred_values

from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import get_request_body

from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import TextNode
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)


class BatchTopicExtractorSync(BatchExtractorBase):

    entity_classification_provider:PreferredValuesProvider = Field( description='Entity classification provider')
    topic_provider:PreferredValuesProvider = Field(description='Topic provider')

    @classmethod
    def class_name(cls) -> str:
        return 'BatchTopicExtractorSync'
    
    def __init__(self, 
                 batch_config:BatchConfig,
                 llm:LLMCacheType=None,
                 prompt_template:str = None,
                 source_metadata_field:Optional[str] = None,
                 batch_inference_dir:str = None,
                 entity_classification_provider:Optional[PreferredValuesProvider]=None,
                 topic_provider:Optional[PreferredValuesProvider]=None):
        
        super().__init__(
            batch_config = batch_config,
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or EXTRACT_TOPICS_PROMPT,
            source_metadata_field=source_metadata_field,
            batch_inference_dir=batch_inference_dir or os.path.join('output', 'batch-topics'),
            description='Topic',
            entity_classification_provider=entity_classification_provider or default_preferred_values([]),
            topic_provider=topic_provider or default_preferred_values([])
        )
    
    def _get_json(self, node, llm, inference_parameters):
        classifications = self.entity_classification_provider(node)
        topics = self.topic_provider(node)
        text = format_text(
            self._get_metadata_or_default(node.metadata, self.source_metadata_field, node.text) 
            if self.source_metadata_field 
            else node.text
        )
        messages = llm._get_messages(
            PromptTemplate(self.prompt_template), 
            text=text,
            preferred_entity_classifications=format_list(classifications),
            preferred_topics=format_list(topics)
        )
        return {
            'recordId': node.node_id,
            'modelInput': get_request_body(llm, messages, inference_parameters)
        }
    
    def _run_non_batch_extractor(self, nodes):
        
        all_nodes = [node for node in nodes]

        extractor = TopicExtractor( 
            prompt_template=self.prompt_template, 
            source_metadata_field=self.source_metadata_field,
            entity_classification_provider=self.entity_classification_provider,
            topic_provider=self.topic_provider
        )

        extracted = extractor.extract(all_nodes)
        
        results = [{n.id_: e[TOPICS_KEY]} for (n, e) in zip(all_nodes, extracted)]
        
        return results
    
    def _update_node(self, node:TextNode, node_metadata_map):
        if node.node_id in node_metadata_map:
            topic_data = node_metadata_map[node.node_id]
            if isinstance(topic_data, dict):
                node.metadata[TOPICS_KEY] = topic_data
            else:
                (topics, _) = parse_extracted_topics(topic_data)
                node.metadata[TOPICS_KEY] = topics.model_dump()             
        else:
            node.metadata[TOPICS_KEY] = []
        return node