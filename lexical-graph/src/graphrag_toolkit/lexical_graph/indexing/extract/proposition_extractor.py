# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import json
from json.decoder import JSONDecodeError
from typing import List, Optional, Sequence, Dict, Any

from graphrag_toolkit.lexical_graph.indexing.model import Propositions
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY

from llama_index.core.schema import BaseNode
from llama_index.core.bridge.pydantic import Field, PrivateAttr
from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.async_utils import run_jobs

DEFAULT_PROPOSITION_MODEL = 'chentong00/propositionizer-wiki-flan-t5-large'


logger = logging.getLogger(__name__)

class PropositionExtractor(BaseExtractor):

    proposition_model_name: str = Field(
        default=DEFAULT_PROPOSITION_MODEL,
        description='The model name of the AutoModelForSeq2SeqLM model to use.',
    )
   
    device: Optional[str] = Field(
        default=None, 
        description="Device to run model on, i.e. 'cuda', 'cpu'"
    )
        
    source_metadata_field: Optional[str] = Field(
        description='Metadata field from which to extract propositions and entities'
    )

    _proposition_tokenizer: Optional[Any] = PrivateAttr(default=None)
    _proposition_model: Optional[Any] = PrivateAttr(default=None)

    @classmethod
    def class_name(cls) -> str:
        return 'PropositionExtractor'
    
    @property
    def proposition_tokenizer(self):
        if self._proposition_tokenizer is None:
            try:
                from transformers import AutoTokenizer
                self._proposition_tokenizer = AutoTokenizer.from_pretrained(self.proposition_model_name)
            except ImportError as e:
                raise ImportError(
                        "transformers package not found, install with 'pip install transformers'"
                    ) from e
        return self._proposition_tokenizer
    
    @property
    def proposition_model(self):
        if self._proposition_model is None:
            try:
                import torch
                from transformers import AutoModelForSeq2SeqLM
                device = self.device or ('cuda' if torch.cuda.is_available() else 'cpu')
                self._proposition_model = AutoModelForSeq2SeqLM.from_pretrained(self.proposition_model_name).to(device)
            except ImportError as e:
                raise ImportError(
                        "torch and/or transformers packages not found, install with 'pip install torch transformers'"
                    ) from e
        return self._proposition_model
    

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
        
        title = ''
        section = ''
        
        input_text = f'Title: {title}. Section: {section}. Content: {text}'
        
        input_ids = self.proposition_tokenizer(input_text, return_tensors='pt').input_ids
        outputs = self.proposition_model.generate(input_ids.to(self.device), max_length=1024).cpu()
        
        output_text = self.proposition_tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        propositions = []

        if output_text:
        
            try:
                propositions = json.loads(output_text)
            except JSONDecodeError as e:
                # sometimes there are missing double quotes at the end of a proposition
                if output_text[-2] != '"':
                    # add missing double quotes to end of last entry
                    output_text = output_text[0:-1] + '"]'
                # add missing double quotes to other entries
                xss = [[str(i) for i in p.split(', "')] for p in output_text[2:-2].split('", "')]
                cleaned = [
                    x
                    for xs in xss
                    for x in xs
                ]                               
                try:
                    propositions = json.loads(json.dumps(cleaned))
                except JSONDecodeError as e:            
                    logger.exception(f'Failed to parse output text as JSON: {output_text}')

        return Propositions(propositions=[p for p in propositions])
