# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from typing import Optional

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.model import Propositions
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_PROPOSITIONS_PROMPT
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor import LLMPropositionExtractor

from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import get_request_body

from llama_index.core.schema import TextNode
from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import NodeRelationship

logger = logging.getLogger(__name__)


class BatchLLMPropositionExtractorSync(BatchExtractorBase):

    @classmethod
    def class_name(cls) -> str:
       return 'BatchLLMPropositionExtractorSync'
    
    def __init__(self, 
                 batch_config:BatchConfig,
                 llm:LLMCacheType=None,
                 prompt_template:str = None,
                 source_metadata_field:Optional[str] = None,
                 batch_inference_dir:str = None):
        
        super().__init__(
            batch_config = batch_config,
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or EXTRACT_PROPOSITIONS_PROMPT,
            source_metadata_field=source_metadata_field,
            batch_inference_dir=batch_inference_dir or os.path.join('output', 'batch-propositions'),
            description='Proposition'
        )

    def _get_json(self, node, llm, inference_parameters):
        text = node.metadata.get(self.source_metadata_field, node.text) if self.source_metadata_field else node.text
        source = node.relationships.get(NodeRelationship.SOURCE, None)
        if source:
            source_info = '\n'.join([str(v) for v in source.metadata.values()])
        else:
            source_info = ''
        
        messages = llm._get_messages(PromptTemplate(self.prompt_template), text=text, source_info=source_info)
        return {
            'recordId': node.node_id,
            'modelInput': get_request_body(llm, messages, inference_parameters)
        }
    
    def _run_non_batch_extractor(self, nodes):
        
        all_nodes = [node for node in nodes]

        extractor = LLMPropositionExtractor(
            prompt_template=self.prompt_template, 
            source_metadata_field=self.source_metadata_field
        )
        
        extracted = extractor.extract(all_nodes)
        
        results = [{n.node_id: e[PROPOSITIONS_KEY]} for (n, e) in zip(all_nodes, extracted)]
        
        return results
    
    def _update_node(self, node:TextNode, node_metadata_map):
        if node.node_id in node_metadata_map:
            proposition_data = node_metadata_map[node.node_id]
            if isinstance(proposition_data, list):
                node.metadata[PROPOSITIONS_KEY] = proposition_data
            else:
                propositions = proposition_data.split('\n')
                propositions_model = Propositions(propositions=[p for p in propositions if p])
                node.metadata[PROPOSITIONS_KEY] = propositions_model.model_dump()['propositions']                
        else:
            node.metadata[PROPOSITIONS_KEY] = []
        return node

    
