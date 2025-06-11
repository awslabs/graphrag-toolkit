# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import os
import json
import time
import shutil

from typing import Optional, List, Sequence, Dict
from datetime import datetime

from graphrag_toolkit.lexical_graph import GraphRAGConfig, BatchJobError
from graphrag_toolkit.lexical_graph.indexing.model import Propositions
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_PROPOSITIONS_PROMPT
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor import LLMPropositionExtractor
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import create_inference_inputs, create_inference_inputs_for_messages, create_and_run_batch_job, download_output_files, process_batch_output, split_nodes
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import get_file_size_mb, get_file_sizes_mb
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import BEDROCK_MIN_BATCH_SIZE

from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import TextNode, BaseNode
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

class BatchLLMPropositionExtractor(BaseExtractor):
    """
    Handles the extraction of propositions using a batch processing approach via an LLM.

    This class is designed to process large sets of nodes by splitting them into smaller
    batches, generating prompts for LLM-based proposition extraction, and handling results
    from batch inference jobs. It manages resources such as directories, uploads and downloads
    to/from S3, and processes responses to extract propositions. The functionality supports
    concurrent processing of multiple batches using asynchronous methods.

    Attributes:
        batch_config (BatchConfig): Configuration for batch inference, including batch size,
            key prefixes, and maximum concurrent batches.
        llm (Optional[LLMCache]): The language model cache to be used for extracting propositions.
        prompt_template (str): Template for generating prompts used in LLM extraction processes.
        source_metadata_field (Optional[str]): Metadata field used as the source for extracting
            text for proposition generation.
        batch_inference_dir (str): Directory location for managing batch inputs and outputs.
    """
    batch_config:BatchConfig = Field('Batch inference config')
    llm:Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )
    prompt_template:str = Field(description='Prompt template')
    source_metadata_field:Optional[str] = Field(description='Metadata field from which to extract propositions')
    batch_inference_dir:str = Field(description='Directory for batch inputs and outputs')
    

    @classmethod
    def class_name(cls) -> str:
        """
        Returns the name of the class as a string. This method is designed to
        provide a human-readable representation of the class name, mainly for
        use in debugging, logging, or other identification purposes.

        Returns:
            str: The name of the class.
        """
        return 'BatchLLMPropositionExtractor'
    
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
            batch_inference_dir=batch_inference_dir or os.path.join('output', 'batch-propositions')
        )

        logger.debug(f'Prompt template: {self.prompt_template}')

        self._prepare_directory(self.batch_inference_dir)

    def _prepare_directory(self, dir):
        """
        Ensures a specified directory exists by creating it if it does not already exist.

        If the directory is not present, it is created along with any intermediate
        directories.

        Args:
            dir (str): The path to the directory to check or create.

        Returns:
            str: The path to the directory that was confirmed to exist or newly created.
        """
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        return dir
    
    async def process_single_batch(self, batch_index:int, node_batch:List[TextNode], s3_client, bedrock_client):
        """
        Processes a single batch of proposition extraction asynchronously by performing several
        steps including creating record files, uploading them to S3, invoking a batch job,
        downloading output files, and processing batch results.

        Args:
            batch_index (int): The index identifier of the current batch being processed.
            node_batch (List[TextNode]): A list of `TextNode` objects representing the data
                batch to be processed.
            s3_client: The S3 client instance used to upload and download files from Amazon S3.
            bedrock_client: The Bedrock client instance used to create and run batch jobs.

        Returns:
            List[Dict]: A list of dictionaries containing the processed results of the batch.

        Raises:
            BatchJobError: If any error occurs during the batch processing.
        """
        try:
            batch_start = time.time()
            timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            input_filename = f'proposition_extraction_{timestamp}_batch_{batch_index}.jsonl'

            messages_batch = []
            for node in node_batch:
                text = node.metadata.get(self.source_metadata_field, node.text) if self.source_metadata_field else node.text
                messages = self.llm.llm._get_messages(PromptTemplate(self.prompt_template), text=text)
                messages_batch.append(messages)

            json_inputs = create_inference_inputs_for_messages(
                self.llm.llm, 
                node_batch, 
                messages_batch
            )

            root_dir = os.path.join(self.batch_inference_dir, timestamp, str(batch_index))
            input_dir = os.path.join(root_dir, 'inputs')
            output_dir = os.path.join(root_dir, 'outputs')
            self._prepare_directory(input_dir)
            self._prepare_directory(output_dir)

            input_filepath = os.path.join(input_dir, input_filename)

            logger.debug(f'[Proposition batch inputs] Writing {len(json_inputs)} records to {input_filename}')

            with open(input_filepath, 'w') as file:
                for item in json_inputs:
                    json.dump(item, file)
                    file.write('\n')

            logger.debug(f'[Proposition batch inputs] Batch input file ready [file: {input_filepath} ({get_file_size_mb(input_filepath)} MB)]')

            # 2 - Upload records to s3
            if self.batch_config.key_prefix:
                s3_input_key = os.path.join(self.batch_config.key_prefix, 'batch-propositions', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join(self.batch_config.key_prefix, 'batch-propositions', timestamp, str(batch_index), 'outputs/')
            else:
                s3_input_key = os.path.join('batch-propositions', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join('batch-propositions', timestamp, str(batch_index), 'outputs/')

            upload_start = time.time()
            logger.debug(f'[Proposition batch inputs] Started uploading {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}]')
            await asyncio.to_thread(s3_client.upload_file, input_filepath, self.batch_config.bucket_name, s3_input_key)
            upload_end = time.time()
            logger.debug(f'[Proposition batch inputs] Finished uploading {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}] ({int((upload_end - upload_start) * 1000)} millis)')

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

            download_start = time.time()
            logger.debug(f'[Proposition batch outputs] Started downloading outputs to {output_dir} from S3 [bucket: {self.batch_config.bucket_name}, key: {s3_output_path}]')
            await asyncio.to_thread(download_output_files, s3_client, self.batch_config.bucket_name, s3_output_path, input_filename, output_dir)
            download_end = time.time()
            logger.debug(f'[Proposition batch outputs] Finished downloading outputs to {output_dir} from S3 [bucket: {self.batch_config.bucket_name}, key: {s3_output_path}]  ({int((download_end - download_start) * 1000)} millis)')
            
            output_file_stats = [f'{f} ({size} MB)' for f, size in get_file_sizes_mb(output_dir).items()]
            logger.debug(f'[Proposition batch outputs] Batch output files ready [files: {output_file_stats}]')

            # 4 - Once complete, process batch output
            batch_results = await process_batch_output(output_dir, input_filename, self.llm)
            batch_end = time.time()
            logger.debug(f'[Proposition batch outputs] Completed processing of batch {batch_index} ({int(batch_end-batch_start)} seconds)')
            
            if self.batch_config.delete_on_success:
                def log_delete_error(function, path, excinfo):
                    logger.error(f'[Proposition batch] Error deleteing {path} - {str(excinfo[1])}' )

                logger.debug(f'[Proposition batch] Deleting batch directory: {root_dir}' )
                shutil.rmtree(root_dir, onerror=log_delete_error)
            
            return batch_results
        
        except Exception as e:
            batch_end = time.time()
            raise BatchJobError(f'[Proposition batch] Error processing batch {batch_index} ({int(batch_end-batch_start)} seconds): {str(e)}') from e 
            
            
    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        """
        Asynchronously extracts propositions from a list of nodes. This method divides the input nodes into batches, processes
        the batches concurrently using Bedrock or a fallback extractor, and processes the results to generate structured"""
        if len(nodes) < BEDROCK_MIN_BATCH_SIZE:
            logger.info(f'[Proposition batch] Not enough records to run batch extraction. List of nodes contains fewer records ({len(nodes)}) than the minimum required by Bedrock ({BEDROCK_MIN_BATCH_SIZE}), so running LLMPropositionExtractor instead.')
            extractor = LLMPropositionExtractor(
                prompt_template=self.prompt_template, 
                source_metadata_field=self.source_metadata_field
            )
            return await extractor.aextract(nodes)

        s3_client = GraphRAGConfig.s3
        bedrock_client = GraphRAGConfig.bedrock

        # 1 - Split nodes into batches (if needed)
        node_batches = split_nodes(nodes, self.batch_config.max_batch_size)
        logger.debug(f'[Proposition batch] Split nodes into batches [num_batches: {len(node_batches)}, sizes: {[len(b) for b in node_batches]}]')

        # 2 - Process batches concurrently
        all_results = {}
        semaphore = asyncio.Semaphore(self.batch_config.max_num_concurrent_batches)

        async def process_batch_with_semaphore(batch_index, node_batch):
            """
            A class for extracting propositions from a batch of nodes using a language model.
            This class handles asynchronous extraction by processing batches of nodes with
            a semaphore to limit concurrency. It inherits from BaseExtractor, providing
            an implementation for extracting proposition data.

            Attributes:
                semaphore (asyncio.Semaphore): A semaphore to control the concurrency of
                    batch processing.
            """
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

    

