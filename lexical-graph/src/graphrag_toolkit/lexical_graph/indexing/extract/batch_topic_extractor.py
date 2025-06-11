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
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import parse_extracted_topics, format_list, format_text
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import create_inference_inputs, create_inference_inputs_for_messages, create_and_run_batch_job, download_output_files, process_batch_output, split_nodes
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import get_file_size_mb, get_file_sizes_mb
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY, DEFAULT_ENTITY_CLASSIFICATIONS
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_TOPICS_PROMPT
from graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor import TopicExtractor
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.scoped_value_provider import ScopedValueProvider, FixedScopedValueProvider, DEFAULT_SCOPE
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import BEDROCK_MIN_BATCH_SIZE

from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import TextNode, BaseNode
from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)

class BatchTopicExtractor(BaseExtractor):
    """
    Handles batch topic extraction processes, utilizing LLMs and external services for generating
    and processing topic-related data for text inputs in batches.

    This class is primarily designed to facilitate the efficient processing of large datasets by
    splitting input data into manageable batches, generating topic extraction prompts, and
    managing asynchronous tasks for data upload, batch processing, and output handling. It
    employs LLM (Language Model) inference and integrates with external systems like S3
    and Bedrock for input handling, batch processing, and result retrieval.

    Attributes:
        batch_config (BatchConfig): Configuration object containing batch inference settings.
        llm (Optional[LLMCache]): An instance of LLMCache to manage caching and interaction
            with the specified language model.
        prompt_template (str): A template to define the format of prompts for topic extraction.
        source_metadata_field (Optional[str]): Metadata field used as input for topic
            extraction, if specified.
        batch_inference_dir (str): Directory path where batch input and result files are stored.
        entity_classification_provider (ScopedValueProvider): Provider for retrieving entity
            classifications in the current scope.
        topic_provider (ScopedValueProvider): Provider for retrieving topics in the current
            scope.
    """
    batch_config:BatchConfig = Field('Batch inference config')
    llm:Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )
    prompt_template:str = Field(description='Prompt template')
    source_metadata_field:Optional[str] = Field(description='Metadata field from which to extract propositions')
    batch_inference_dir:str = Field(description='Directory for batch inputs and outputs')
    entity_classification_provider:ScopedValueProvider = Field( description='Entity classification provider')
    topic_provider:ScopedValueProvider = Field(description='Topic provider')
    

    @classmethod
    def class_name(cls) -> str:
        """
        Returns the class name of the batch topic extractor.

        This method is a class method and provides the name of the class as a string.

        Returns:
            str: The name of the class, which is 'BatchTopicExtractor'.
        """
        return 'BatchTopicExtractor'
    
    def __init__(self, 
                 batch_config:BatchConfig,
                 llm:LLMCacheType=None,
                 prompt_template:str = None,
                 source_metadata_field:Optional[str] = None,
                 batch_inference_dir:str = None,
                 entity_classification_provider:Optional[ScopedValueProvider]=None,
                 topic_provider:Optional[ScopedValueProvider]=None):
        """
        Initializes an instance of the class with configuration details for batch processing,
        language model, prompt templates, metadata fields, directory for batch inference,
        entity classification provider, and topic provider.

        Args:
            batch_config: Configuration object specifying batch processing details.
            llm: Instance of a language model cache or a language model, defining the desired
                behavior for processing.
            prompt_template: Template string used to create prompts for processing.
            source_metadata_field: Optional metadata field in the source data to be considered
                during processing.
            batch_inference_dir: Directory path where batch inference outputs will be stored.
            entity_classification_provider: Scoped value provider for entity classification settings.
            topic_provider: Scoped value provider for topic settings based on the given scope.

        """
        super().__init__(
            batch_config = batch_config,
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template or EXTRACT_TOPICS_PROMPT,
            source_metadata_field=source_metadata_field,
            batch_inference_dir=batch_inference_dir or os.path.join('output', 'batch-topics'),
            entity_classification_provider=entity_classification_provider or FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: DEFAULT_ENTITY_CLASSIFICATIONS}),
            topic_provider=topic_provider or FixedScopedValueProvider(scoped_values={DEFAULT_SCOPE: []})
        )

        logger.debug(f'Prompt template: {self.prompt_template}')

        self._prepare_directory(self.batch_inference_dir)

    def _prepare_directory(self, dir):
        """
        Prepares a directory by creating it if it does not already exist.

        This function ensures that the specified directory exists. If the directory
        does not exist, it is created with the specified permissions.

        Args:
            dir (str): The path of the directory to prepare.

        Returns:
            str: The path to the prepared directory.
        """
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        return dir
    
    def _get_metadata_or_default(self, metadata, key, default):
        """
        Retrieve the value associated with a given key from the metadata
        dictionary. If the key does not exist, return the provided default.
        The function ensures that if the retrieved value is falsy, the
        default value is returned instead.

        Args:
            metadata (dict): The metadata dictionary to search.
            key (Any): The key whose value needs to be retrieved.
            default (Any): The default value to return if the key is not
                found or its value is falsy.

        Returns:
            Any: The value associated with the given key or the default
            value if the key is not present or its value is falsy.
        """
        value = metadata.get(key, default)
        return value or default
    
    async def process_single_batch(self, batch_index:int, node_batch:List[TextNode], s3_client, bedrock_client):
        """
        Processes a single batch of text nodes through multiple workflow stages, including record creation, S3 bucket
        upload, batch job invocation using Bedrock, and result processing.

        This function performs the following steps:
        1. Creates input files for batch inference, including formatting text based on metadata, generating message prompts,
           and preparing JSONL files.
        2. Uploads the generated input files to an AWS S3 bucket based on the configuration.
        3. Invokes a Bedrock batch job for topic extraction using the prepared inputs.
        4. Downloads and processes the output files generated during the batch job.
        5. Returns the processed batch results for further utilization.

        Args:
            batch_index (int): The index of the current batch being processed.
            node_batch (List[TextNode]): A list of text nodes to process in this batch.
            s3_client: The S3 client instance used for file handling with the S3 bucket.
            bedrock_client: The Bedrock client instance for invoking and interacting with the batch job.

        Returns:
            List: The results after processing the outputs of the batch job.

        Raises:
            BatchJobError: If any error occurs during batch processing, an exception is raised, providing details
            of the batch index and error context.
        """
        try:
            batch_start = time.time()
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S") 
            input_filename = f'topic_extraction_{timestamp}_{batch_index}.jsonl'

            # 1 - Create Record Files (.jsonl)

            entity_classification_map = {}
            topic_map = {}

            def get_classifications(node):
                scope = self.entity_classification_provider.scope_func(node)
                if scope not in entity_classification_map:
                    (_, current_entity_classifications) = self.entity_classification_provider.get_current_values(node)
                    entity_classification_map[scope] = current_entity_classifications
                return entity_classification_map[scope]
            
            def get_topics(node):
                scope = self.topic_provider.scope_func(node)
                if scope not in topic_map:
                    # It's unlikely there'll be any topics for this node, but just in case...
                    (_, current_topics) = self.topic_provider.get_current_values(node)
                    topic_map[scope] = current_topics
                return topic_map[scope]

            messages_batch = []
            for node in node_batch:
                classifications = get_classifications(node)
                topics = get_topics(node)
                text = format_text(
                    self._get_metadata_or_default(node.metadata, self.source_metadata_field, node.text) 
                    if self.source_metadata_field 
                    else node.text
                )
                messages = self.llm.llm._get_messages(
                    PromptTemplate(self.prompt_template), 
                    text=text,
                    preferred_entity_classifications=format_list(classifications),
                    preferred_topics=format_list(topics)
                )
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

            logger.debug(f'[Topic batch inputs] Writing {len(json_inputs)} records to {input_filename}')

            with open(input_filepath, 'w') as file:
                for item in json_inputs:
                    json.dump(item, file)
                    file.write('\n')

            logger.debug(f'[Topic batch inputs] Batch input file ready [file: {input_filepath} ({get_file_size_mb(input_filepath)} MB)]')

            # 2 - Upload records to s3
            if self.batch_config.key_prefix:
                s3_input_key = os.path.join(self.batch_config.key_prefix, 'batch-topics', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join(self.batch_config.key_prefix, 'batch-topics', timestamp, str(batch_index), 'outputs/')
            else:
                s3_input_key = os.path.join('batch-topics', timestamp, str(batch_index), 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join('batch-topics', timestamp, str(batch_index), 'outputs/')

            upload_start = time.time()
            logger.debug(f'[Topic batch inputs] Started uploading {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}]')
            await asyncio.to_thread(s3_client.upload_file, input_filepath, self.batch_config.bucket_name, s3_input_key)
            upload_end = time.time()
            logger.debug(f'[Topic batch inputs] Finished uploading {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}] ({int((upload_end - upload_start) * 1000)} millis)')
            
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

            download_start = time.time()
            logger.debug(f'[Topic batch outputs] Started downloading outputs to {output_dir} from S3 [bucket: {self.batch_config.bucket_name}, key: {s3_output_path}]')
            await asyncio.to_thread(download_output_files, s3_client, self.batch_config.bucket_name, s3_output_path, input_filename, output_dir)
            download_end = time.time()
            logger.debug(f'[Topic batch outputs] Finished downloading outputs to {output_dir} from S3 [bucket: {self.batch_config.bucket_name}, key: {s3_output_path}]  ({int((download_end - download_start) * 1000)} millis)')

            output_file_stats = [f'{f} ({size} MB)' for f, size in get_file_sizes_mb(output_dir).items()]
            logger.debug(f'[Topic batch outputs] Batch output files ready [files: {output_file_stats}]')

            # 4 - Once complete, process batch output
            batch_results = await process_batch_output(output_dir, input_filename, self.llm)
            batch_end = time.time()
            logger.debug(f'[Topic batch outputs] Completed processing of batch {batch_index} ({int(batch_end-batch_start)} seconds)')

            if self.batch_config.delete_on_success:
                def log_delete_error(function, path, excinfo):
                    logger.error(f'[Topic batch] Error deleteing {path} - {str(excinfo[1])}' )

                logger.debug(f'[Topic batch] Deleting batch directory: {root_dir}' )
                shutil.rmtree(root_dir, onerror=log_delete_error)
            
            return batch_results
        
        except Exception as e:
            batch_end = time.time()
            raise BatchJobError(f'[Topic batch] Error processing batch {batch_index} ({int(batch_end-batch_start)} seconds): {str(e)}') from e 
           
        
    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        """
        Asynchronously processes a sequence of nodes, extracting topics based on Bedrock configurations
        or fallback TopicExtractor, and parallelizes processing in batches for performance efficiency.

        Args:
            nodes (Sequence[BaseNode]): A sequence of nodes to process and extract topics from.

        Returns:
            List[Dict]: A list of dictionaries containing extracted topics for each node.

        Raises:
            No specific exceptions explicitly raised by this function.

        """
        if len(nodes) < BEDROCK_MIN_BATCH_SIZE:
            logger.info(f'Not enough records to run batch extraction. List of nodes contains fewer records ({len(nodes)}) than the minimum required by Bedrock ({BEDROCK_MIN_BATCH_SIZE}), so running TopicExtractor instead.')
            extractor = TopicExtractor( 
                prompt_template=self.prompt_template, 
                source_metadata_field=self.source_metadata_field,
                entity_classification_provider=self.entity_classification_provider,
                topic_provider=self.topic_provider
            )
            return await extractor.aextract(nodes)


        s3_client = GraphRAGConfig.s3
        bedrock_client = GraphRAGConfig.bedrock

        # 1 - Split nodes into batches (if needed)
        node_batches = split_nodes(nodes, self.batch_config.max_batch_size)
        logger.debug(f'[Topic batch] Split nodes into batches [num_batches: {len(node_batches)}, sizes: {[len(b) for b in node_batches]}]')

        # 2 - Process batches concurrently
        all_results = {}
        semaphore = asyncio.Semaphore(self.batch_config.max_num_concurrent_batches)

        async def process_batch_with_semaphore(batch_index, node_batch):
            """
            An asynchronous extractor that processes batches of nodes to perform topic
            extraction. This class extends the BaseExtractor and provides functionality
            to handle batch extraction in an asynchronous manner. It leverages semaphores
            to control concurrent access to shared resources while processing batches.

            Methods:
                aextract: The main asynchronous function to extract topics from a
                sequence of nodes in batches.
            """
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