# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import os
import json
import boto3
from typing import Optional, List, Sequence, Dict
from datetime import datetime

from graphrag_toolkit import GraphRAGConfig, BatchJobError
from graphrag_toolkit.indexing.model import Propositions
from graphrag_toolkit.indexing.constants import PROPOSITIONS_KEY
from graphrag_toolkit.indexing.prompts import EXTRACT_PROPOSITIONS_PROMPT
from graphrag_toolkit.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.indexing.utils.batch_inference_utils import create_inference_inputs, create_and_run_batch_job, download_output_files, process_batch_output, split_nodes

from llama_index.core.extractors.interface import BaseExtractor
from llama_index.llms.bedrock import Bedrock
from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import TextNode, BaseNode

logger = logging.getLogger(__name__)

class BatchLLMPropositionExtractor(BaseExtractor):

    batch_config:BatchConfig = Field('Batch inference config')
    llm:Optional[Bedrock] = Field(description='The LLM to use for extraction')
    prompt_template:str = Field(description='Prompt template')
    source_metadata_field:Optional[str] = Field(description='Metadata field from which to extract propositions')
    batch_inference_dir:str = Field(description='Directory for batch inputs and results results')
    

    @classmethod
    def class_name(cls) -> str:
        return 'BatchLLMPropositionExtractor'
    
    def __init__(self, 
                 batch_config:BatchConfig,
                 llm:Optional[Bedrock] = None,
                 prompt_template:str = None,
                 source_metadata_field:Optional[str] = None,
                 batch_inference_dir:str = None):
        
        super().__init__(
            batch_config = batch_config,
            llm = llm or GraphRAGConfig.extraction_llm,
            prompt_template=prompt_template or  EXTRACT_PROPOSITIONS_PROMPT,
            source_metadata_field=source_metadata_field,
            batch_inference_dir=batch_inference_dir or os.path.join('output', 'batch-propositions')
        )

        self._prepare_directory(self.batch_inference_dir)

    def _prepare_directory(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        return dir
    
    async def process_single_batch(self, batch_index:int, node_batch:List[TextNode], s3_client, bedrock_client):
        try:
            timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            input_filename = f'proposition_extraction_{timestamp}_batch_{batch_index}.jsonl'

            # 1 - Create Record Files (.jsonl)
            prompts = []
            for node in node_batch:
                text = node.metadata.get(self.source_metadata_field, node.text) if self.source_metadata_field else node.text
                prompt = self.prompt_template.format(text=text)
                prompts.append(prompt)

            json_inputs = create_inference_inputs(
                self.llm,
                node_batch, 
                prompts
            )

            input_dir = os.path.join(self.batch_inference_dir, timestamp, str(batch_index), 'inputs')
            output_dir = os.path.join(self.batch_inference_dir, timestamp, str(batch_index), 'outputs')
            self._prepare_directory(input_dir)
            self._prepare_directory(output_dir)

            input_filepath = os.path.join(input_dir, input_filename)
            with open(input_filepath, 'w') as file:
                for item in json_inputs:
                    json.dump(item, file)
                    file.write('\n')

            # 2 - Upload records to s3
            s3_input_key = None
            s3_output_path = None
            if self.batch_config.key_prefix:
                s3_input_key = os.path.join(self.batch_config.key_prefix, 'batch-propositions', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join(self.batch_config.key_prefix, 'batch-propositions', timestamp, str(batch_index), 'outputs/')
            else:
                s3_input_key = os.path.join('batch-propositions', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join('batch-propositions', timestamp, str(batch_index), 'outputs/')

            await asyncio.to_thread(s3_client.upload_file, input_filepath, self.batch_config.bucket_name, s3_input_key)
            logger.debug(f'Uploaded {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}]')

            # 3 - Invoke batch job
            await asyncio.to_thread(create_and_run_batch_job,
                'extract-propositions',
                bedrock_client, 
                timestamp, 
                batch_index,
                self.batch_config,
                s3_input_key, 
                s3_output_path,
                self.llm.model
            )

            await asyncio.to_thread(download_output_files, s3_client, self.batch_config.bucket_name, s3_output_path, input_filename, output_dir)

            # 4 - Once complete, process batch output
            batch_results = await process_batch_output(output_dir, input_filename, self.llm)
            logger.debug(f'Completed processing of batch {batch_index}')
            return batch_results
        
        except Exception as e:
            raise BatchJobError(f'Error processing batch {batch_index}: {str(e)}') from e 
            
            
    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        s3_client = boto3.client('s3', region_name=self.batch_config.region)
        bedrock_client = boto3.client('bedrock', region_name=self.batch_config.region)

        # 1 - Split nodes into batches (if needed)
        node_batches = split_nodes(nodes, self.batch_config.max_batch_size)
        logger.debug(f'Split nodes into {len(node_batches)} batches [sizes: {[len(b) for b in node_batches]}]')

        # 2 - Process batches concurrently
        all_results = {}
        semaphore = asyncio.Semaphore(self.batch_config.max_num_concurrent_batches)

        async def process_batch_with_semaphore(batch_index, node_batch):
            async with semaphore:
                return await self.process_single_batch(batch_index, node_batch, s3_client, bedrock_client)

        tasks = [process_batch_with_semaphore(i, batch) for i, batch in enumerate(node_batches)]
        batch_results = await asyncio.gather(*tasks)

        for result in batch_results:
            all_results.update(result)

        # 3 - Process proposition nodes
        return_results = []
        for node in nodes:
            if node.node_id in all_results:
                raw_response = all_results[node.node_id]
                propositions = raw_response.split('\n')
                propositions_model = Propositions(propositions=[p for p in propositions if p])
                return_results.append({
                    PROPOSITIONS_KEY: propositions_model.model_dump()['propositions']
                })
            else:
                return_results.append({PROPOSITIONS_KEY: []})

        return return_results

    
