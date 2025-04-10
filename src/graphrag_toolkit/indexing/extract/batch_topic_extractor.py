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
from graphrag_toolkit.utils import LLMCache, LLMCacheType
from graphrag_toolkit.indexing.utils.topic_utils import parse_extracted_topics, format_list, format_text
from graphrag_toolkit.indexing.utils.batch_inference_utils import create_inference_inputs, create_inference_inputs_for_messages, create_and_run_batch_job, download_output_files, process_batch_output, split_nodes
from graphrag_toolkit.indexing.constants import TOPICS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.indexing.prompts import EXTRACT_TOPICS_PROMPT
from graphrag_toolkit.indexing.extract.topic_extractor import TopicExtractor
from graphrag_toolkit.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.indexing.extract.scoped_value_provider import ScopedValueProvider, FixedScopedValueProvider, DEFAULT_SCOPE
from graphrag_toolkit.indexing.utils.batch_inference_utils import BEDROCK_MIN_BATCH_SIZE

from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import TextNode, BaseNode
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

class BatchTopicExtractor(BaseExtractor):
    batch_config:BatchConfig = Field('Batch inference config')
    llm:Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )
    prompt_template:str = Field(description='Prompt template')
    source_metadata_field:Optional[str] = Field(description='Metadata field from which to extract propositions')
    batch_inference_dir:str = Field(description='Directory for batch inputs and results results')
    entity_classification_provider:ScopedValueProvider = Field( description='Entity classification provider')
    topic_provider:ScopedValueProvider = Field(description='Topic provider')

    @classmethod
    def class_name(cls) -> str:
        return 'BatchTopicExtractor'
    
    def __init__(self, 
                 batch_config:BatchConfig,
                 llm:LLMCacheType=None,
                 prompt_template:str = None,
                 source_metadata_field:Optional[str] = None,
                 batch_inference_dir:str = None,
                 entity_classification_provider:Optional[ScopedValueProvider]=None,
                 topic_provider:Optional[ScopedValueProvider]=None):
        
        super().__init__(
            batch_config = batch_config,
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or  EXTRACT_TOPICS_PROMPT,
            source_metadata_field=source_metadata_field,
            batch_inference_dir=batch_inference_dir or os.path.join('output', 'batch-topics'),
            entity_classification_provider=entity_classification_provider or FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: DEFAULT_ENTITY_CLASSIFICATIONS}),
            topic_provider=topic_provider or FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: []})
        )

        self._prepare_directory(self.batch_inference_dir)

    def _prepare_directory(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        return dir
    
    def _get_metadata_or_default(self, metadata, key, default):
        value = metadata.get(key, default)
        return value or default
    
    async def process_single_batch(self, batch_index:int, node_batch:List[TextNode], s3_client, bedrock_client):
        try:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S") 
            input_filename = f'topic_extraction_{timestamp}_{batch_index}.jsonl'

            # 1 - Create Record Files (.jsonl)
            # prompts = []
            # for node in node_batch:
            #     (_, current_entity_classifications) = self.entity_classification_provider.get_current_values(node)
            #     (_, current_topics) = self.topic_provider.get_current_values(node)
            #     text = format_text(
            #         self._get_metadata_or_default(node.metadata, self.source_metadata_field, node.text) 
            #         if self.source_metadata_field 
            #         else node.text
            #     )
            #     prompt = self.prompt_template.format(
            #         text=text,
            #         preferred_entity_classifications=format_list(current_entity_classifications),
            #         preferred_topics=format_list(current_topics)
            #     )
            #     prompts.append(prompt)

            # json_inputs = create_inference_inputs(
            #     self.llm.llm,
            #     node_batch, 
            #     prompts
            # )

            messages_batch = []
            for node in node_batch:
                (_, current_entity_classifications) = self.entity_classification_provider.get_current_values(node)
                (_, current_topics) = self.topic_provider.get_current_values(node)
                text = format_text(
                    self._get_metadata_or_default(node.metadata, self.source_metadata_field, node.text) 
                    if self.source_metadata_field 
                    else node.text
                )
                messages = self.llm.llm._get_messages(
                    PromptTemplate(self.prompt_template), 
                    text=text,
                    preferred_entity_classifications=format_list(current_entity_classifications),
                    preferred_topics=format_list(current_topics)
                )
                messages_batch.append(messages)

            json_inputs = create_inference_inputs_for_messages(
                self.llm.llm, 
                node_batch, 
                messages_batch
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
                s3_input_key = os.path.join(self.batch_config.key_prefix, 'batch-topics', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join(self.batch_config.key_prefix, 'batch-topics', timestamp, str(batch_index), 'outputs/')
            else:
                s3_input_key = os.path.join('batch-topics', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join('batch-topics', timestamp, str(batch_index), 'outputs/')

            await asyncio.to_thread(s3_client.upload_file, input_filepath, self.batch_config.bucket_name, s3_input_key)
            logger.debug(f'Uploaded {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}]')

            # 3 - Invoke batch job
            await asyncio.to_thread(create_and_run_batch_job,
                'extract-topics',
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

        if len(nodes) < BEDROCK_MIN_BATCH_SIZE:
            logger.debug(f'List of nodes contains fewer records ({len(nodes)}) than the minimum required by Bedrock ({BEDROCK_MIN_BATCH_SIZE}), so running TopicExtractor instead')
            extractor = TopicExtractor( 
                prompt_template=self.prompt_template, 
                source_metadata_field=self.source_metadata_field,
                entity_classification_provider=self.entity_classification_provider,
                topic_provider=self.topic_provider
            )
            return await extractor.aextract(nodes)


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

        # 3 - Process topic nodes
        return_results = []
        for node in nodes:
            record_id = node.node_id
            if record_id in all_results:
                (topics, _) = parse_extracted_topics(all_results[record_id])
                return_results.append({
                    TOPICS_KEY: topics.model_dump()
                })

        return return_results