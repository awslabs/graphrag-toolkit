# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import time
import concurrent.futures
import uuid
import json
import shutil
from os.path import join
from abc import abstractmethod
from typing import Optional, Sequence, List, Dict, Any, cast, Iterable
from dataclasses import replace
from datetime import datetime

from graphrag_toolkit.lexical_graph import GraphRAGConfig, BatchJobError
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import (
    COMPLETE,
    SUBMITTED,
    PartitionRecord,
    batch_job_name,
    partition_id,
)

from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import get_file_size_mb, get_file_sizes_mb, split_nodes, create_and_run_batch_job, download_output_files, process_batch_output_sync, wait_for_job_completion
from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import BEDROCK_MIN_BATCH_SIZE
from graphrag_toolkit.lexical_graph.utils.id_validation import validate_id_segment

from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.bridge.pydantic import Field
from llama_index.core.schema import BaseNode, TextNode
from llama_index.core.schema import NodeRelationship

logger = logging.getLogger(__name__)

# Bedrock's states for a job that may still complete. Waiting on one is cheaper
# than resubmitting: the first job bills to the end either way. A job that is
# Stopping can only end Stopped, so it is not among them.
LIVE_JOB_STATES = ('Submitted', 'Validating', 'Scheduled', 'InProgress')


class BatchExtractorBase(BaseExtractor):

    batch_config:BatchConfig = Field('Batch inference config')
    llm:Optional[LLMCache] = Field(
        description='The LLM to use for extraction'
    )
    prompt_template:str = Field(description='Prompt template')
    source_metadata_field:Optional[str] = Field(description='Metadata field from which to extract propositions')
    batch_inference_dir:str = Field(description='Directory for batch inputs and outputs')
    description:str = Field(description='Description')
    manifest_store:Optional[Any] = Field(
        default=None,
        description='Where this run records what it did with each partition. None for a run that keeps no record'
    )

    @classmethod
    def class_name(cls) -> str:
       return 'BatchExtractorBase'
    
    def __init__(self, 
                 batch_config:BatchConfig,
                 llm:LLMCacheType=None,
                 prompt_template:str=None,
                 source_metadata_field:Optional[str]=None,
                 batch_inference_dir:str=None,
                 description:str=None,
                 **kwargs
        ):
        
        super().__init__(
            batch_config = batch_config,
            llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
                llm=llm or GraphRAGConfig.extraction_llm,
                enable_cache=GraphRAGConfig.enable_cache
            ),
            prompt_template=prompt_template,
            source_metadata_field=source_metadata_field,
            batch_inference_dir=batch_inference_dir or os.path.join(GraphRAGConfig.local_output_dir, f'batch-{description}s'),
            description=description,
            **kwargs
        )

        logger.debug(f'Prompt template: {self.prompt_template}')

        self._prepare_directory(self.batch_inference_dir)

    def _prepare_directory(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        return dir
    
    async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
        raise NotImplemented()
    
    def _get_metadata_or_default(self, metadata, key, default):
        value = metadata.get(key, default)
        return value or default
    
    @abstractmethod
    def _get_json(self, node, llm, inference_parameters):
        raise NotImplemented()
    
    @abstractmethod
    def _run_non_batch_extractor(self, nodes):
        raise NotImplemented()
    
    @abstractmethod
    def _update_node(self, node:TextNode, node_metadata_map):
        raise NotImplemented()
        
    def _partition_of(self, node_batch:List[TextNode]) -> Optional[str]:
        """
        What this job's partition is called, or None when the run keeps no
        record. Named from the nodes, which is all a worker is handed.
        """
        if self.manifest_store is None:
            return None

        return partition_id([node.node_id for node in node_batch], stage=self.description)

    def _describes_every_node(self, described:int, expected:int, partition:str, job_name:Optional[str]) -> bool:
        """
        Whether a job's output answers for the whole partition. A node the
        output does not describe would be extracted as nothing, and a record
        marked complete means no restart comes back for it, so a short output
        is never recorded complete.
        """
        if described == expected:
            return True

        logger.warning(
            f'[{self.description} batch] The job described {described} of {expected} nodes; '
            f'the partition is left for a restart to redo [partition: {partition}, job_name: {job_name}]'
        )
        return False

    def _resubmitting(self, record:PartitionRecord, why:str, level:int=logging.INFO) -> None:
        """Say why an earlier run's job is not being reused. The caller then resubmits."""
        logger.log(
            level,
            f'[{self.description} batch] {why}, resubmitting as attempt {record.attempt + 1} '
            f'[partition: {record.partition_id}, job_name: {record.job_name}, job_arn: {record.job_arn}]'
        )
        return None

    def _recover_partition(self, record:PartitionRecord, expected:int, s3_client, bedrock_client):
        """
        The results of a job an earlier run already paid for, or None if there
        are none to be had.

        A job still running is waited on: it will bill whether or not it is
        resubmitted, so waiting is the cheaper of the two. A completed job is
        downloaded and has to answer for every node in the partition. Anything
        else, including an output that cannot be read, counts as having produced
        nothing: the assumption that costs a resubmission rather than a silent gap.
        """
        if not record.job_arn or not record.output_path:
            return None

        try:
            status = bedrock_client.get_model_invocation_job(jobIdentifier=record.job_arn)['status']
        except Exception as e:
            return self._resubmitting(record, f'Could not read the job an earlier run submitted [error: {e!s}]', logging.WARNING)

        if status in LIVE_JOB_STATES:
            logger.info(
                f'[{self.description} batch] An earlier run left this partition {status}, '
                f'waiting on it rather than paying for it twice '
                f'[partition: {record.partition_id}, job_name: {record.job_name}]'
            )
            try:
                wait_for_job_completion(bedrock_client, record.job_arn, record.input_filename)
                status = 'Completed'
            except BatchJobError as e:
                return self._resubmitting(record, f'The job an earlier run left running did not complete [error: {e!s}]')

        if status != 'Completed':
            return self._resubmitting(record, f'An earlier run left this partition in {status}')

        logger.info(
            f'[{self.description} batch] Downloading the output of a job an earlier run '
            f'completed [partition: {record.partition_id}, job_name: {record.job_name}]'
        )

        # A second recovery of the same partition must not read the first one's
        # files alongside its own.
        output_dir = os.path.join(self.batch_inference_dir, 'recovered', record.partition_id)
        shutil.rmtree(output_dir, ignore_errors=True)
        self._prepare_directory(output_dir)

        try:
            download_output_files(
                s3_client, self.batch_config.bucket_name, record.output_path,
                record.input_filename, output_dir
            )
            results = list(process_batch_output_sync(output_dir, record.input_filename, self.llm))
        except Exception as e:
            return self._resubmitting(record, f'Could not read the output of a completed job [error: {e!s}]', logging.WARNING)

        if not self._describes_every_node(len(results), expected, record.partition_id, record.job_name):
            return None

        if self.batch_config.delete_on_success:
            shutil.rmtree(output_dir, ignore_errors=True)

        return results

    def _process_single_batch(self, batch_index:int, node_batch:Iterable[TextNode], s3_client, bedrock_client):
        try:

            batch_start = time.time()

            node_batch = list(node_batch)
            partition = self._partition_of(node_batch)
            recorded = self.manifest_store.read(partition, s3_client) if partition else None

            if recorded is not None:
                recovered = self._recover_partition(recorded, len(node_batch), s3_client, bedrock_client)
                if recovered is not None:
                    for (node_id, text) in recovered:
                        yield (node_id, text)
                    self.manifest_store.write(replace(recorded, state=COMPLETE), s3_client)
                    return

            attempt = recorded.attempt + 1 if recorded is not None else 1

            timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            batch_suffix = f'{batch_index}-{uuid.uuid4().hex[:5]}'
            input_filename = f'{self.description.lower()}-extraction-{timestamp}-batch-{batch_suffix}.jsonl'

            root_dir = os.path.join(self.batch_inference_dir, timestamp, batch_suffix)
            input_dir = os.path.join(root_dir, 'inputs')
            output_dir = os.path.join(root_dir, 'outputs')
            self._prepare_directory(input_dir)
            self._prepare_directory(output_dir)

            input_filepath = os.path.join(input_dir, input_filename)

            logger.debug(f'[{self.description} batch inputs] Writing records to {input_filename}')

            llm = self.llm.llm

            inference_parameters = llm._get_all_kwargs() 
            record_count = 0

            with open(input_filepath, 'w') as file:

                for node in node_batch:
                    json_structure = self._get_json(node, llm, inference_parameters)
                    json.dump(json_structure, file)
                    file.write('\n')

                    record_count += 1

            logger.debug(f'[{self.description} batch inputs] Batch input file ready [num_records: {record_count}, file: {input_filepath} ({get_file_size_mb(input_filepath)} MB)]')

            # 2 - Upload records to s3
            if self.batch_config.key_prefix:
                s3_input_key = os.path.join(self.batch_config.key_prefix, f'batch-{self.description.lower()}s', timestamp, batch_suffix, 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join(self.batch_config.key_prefix, f'batch-{self.description.lower()}s', timestamp, batch_suffix, 'outputs/')
            else:
                s3_input_key = os.path.join(f'batch-{self.description.lower()}s', timestamp, batch_suffix, 'inputs', os.path.basename(input_filename))
                s3_output_path = os.path.join(f'batch-{self.description.lower()}s', timestamp, batch_suffix, 'outputs/')

            upload_start = time.time()
            logger.debug(f'[{self.description} batch inputs] Started uploading {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}]')
            s3_client.upload_file(input_filepath, self.batch_config.bucket_name, s3_input_key)
            upload_end = time.time()
            logger.debug(f'[{self.description} batch inputs] Finished uploading {input_filename} to S3 [bucket: {self.batch_config.bucket_name}, key: {s3_input_key}] ({int((upload_end - upload_start) * 1000)} millis)')

            # 3 - Invoke batch job
            job_name_prefix = f'extract-{self.description.lower()}s'
            submitted = {}

            def record(state):
                return PartitionRecord(
                    partition_id=partition,
                    attempt=attempt,
                    state=state,
                    job_name=submitted.get('job_name'),
                    job_arn=submitted.get('job_arn'),
                    output_path=s3_output_path,
                    input_filename=input_filename,
                )

            def record_submission(job_arn, job_name):
                # Written before the wait, which is what can outlive the
                # process. An unrecorded job is one a restart pays for twice.
                submitted.update(job_arn=job_arn, job_name=job_name)
                self.manifest_store.write(record(SUBMITTED), s3_client)

            create_and_run_batch_job(
                job_name_prefix,
                bedrock_client,
                timestamp,
                batch_suffix,
                self.batch_config,
                s3_input_key,
                s3_output_path,
                self.llm.model,
                job_name=(
                    batch_job_name(
                        job_name_prefix, self.manifest_store.run_id, partition, attempt,
                        suffix=batch_suffix
                    )
                    if partition else None
                ),
                on_submitted=record_submission if partition else None,
            )

            download_start = time.time()
            logger.debug(f'[{self.description} batch outputs] Started downloading outputs to {output_dir} from S3 [bucket: {self.batch_config.bucket_name}, key: {s3_output_path}]')
            download_output_files(s3_client, self.batch_config.bucket_name, s3_output_path, input_filename, output_dir)
            download_end = time.time()
            logger.debug(f'[{self.description} batch outputs] Finished downloading outputs to {output_dir} from S3 [bucket: {self.batch_config.bucket_name}, key: {s3_output_path}]  ({int((download_end - download_start) * 1000)} millis)')
            
            output_file_stats = [f'{f} ({size} MB)' for f, size in get_file_sizes_mb(output_dir).items()]
            logger.debug(f'[{self.description} batch outputs] Batch output files ready [files: {output_file_stats}]')

            # 4 - Once complete, process batch output
            described = 0
            for (node_id, text) in process_batch_output_sync(output_dir, input_filename, self.llm):
                described += 1
                yield (node_id, text)

            # Left as submitted when the output is short, so a restart redoes it.
            if partition and self._describes_every_node(described, len(node_batch), partition, submitted.get('job_name')):
                self.manifest_store.write(record(COMPLETE), s3_client)

            batch_end = time.time()
            logger.debug(f'[{self.description} batch outputs] Completed processing of batch {batch_index} ({int(batch_end-batch_start)} seconds)')
            
            if self.batch_config.delete_on_success:
                def log_delete_error(function, path, excinfo):
                    logger.error(f'[{self.description} batch] Error deleting {path} - {str(excinfo[1])}' )

                logger.debug(f'[{self.description} batch] Deleting batch directory: {root_dir}' )
                shutil.rmtree(root_dir, onerror=log_delete_error)

        except Exception as e:
            batch_end = time.time()
            raise BatchJobError(f'[{self.description} batch] Error processing batch {batch_index} ({int(batch_end-batch_start)} seconds): {str(e)}') from e 
    
    
    def _get_nodes_from_temp_dir(self, node_file_paths:List[str]):
        
        for node_file_path in node_file_paths:
            with open(node_file_path) as f:
                node = TextNode.from_json(f.read())
                yield node
    
    def _to_results_generator(self, node_metadata_list:List[Dict]):
        for node_metadata in node_metadata_list:
            for node_id, text in node_metadata.items():
                yield (node_id, text)

    def _post_process_node(self, 
        node, 
        excluded_embed_metadata_keys: Optional[List[str]] = None,
        excluded_llm_metadata_keys: Optional[List[str]] = None):
        
        if excluded_embed_metadata_keys is not None:
            node.excluded_embed_metadata_keys.extend(excluded_embed_metadata_keys)
        if excluded_llm_metadata_keys is not None:
            node.excluded_llm_metadata_keys.extend(excluded_llm_metadata_keys)
        if not self.disable_template_rewrite:
            if isinstance(node, TextNode):
                cast(TextNode, node).text_template = self.node_text_template 
        return node
    
    def _save_node_in_temp_dir(self, node:TextNode, temp_dir:str):
        validate_id_segment(node.node_id, 'node_id')
        node_output_path = join(temp_dir, f'{node.node_id}.json')
        with open(node_output_path, 'w') as f:
            json.dump(node.to_dict(), f, indent=4)
        return node_output_path

    def _process_nodes(self, 
        nodes:Sequence[BaseNode],
        excluded_embed_metadata_keys: Optional[List[str]] = None,
        excluded_llm_metadata_keys: Optional[List[str]] = None,
        **kwargs: Any
    ):
        
        # 0 - Save nodes to temp dir
        node_file_paths = {}

        temp_dir = join(self.batch_inference_dir, 'temp', str(uuid.uuid4()))
        logger.debug(f'Creating temp node directory: {temp_dir}')
        self._prepare_directory(temp_dir)

        for node in nodes:
            node_output_path = self._save_node_in_temp_dir(node, temp_dir)
            node_file_paths[node_output_path] = None

        nodes = None

        results_generators = []

        if len(node_file_paths.keys()) < BEDROCK_MIN_BATCH_SIZE:

            logger.warning(f'[{self.description} batch] Not enough records to run batch extraction. List of nodes contains fewer records ({len(node_file_paths.keys())}) than the minimum required by Bedrock ({BEDROCK_MIN_BATCH_SIZE}), so running non-batch extractor instead.')
            results_generators.append(self._to_results_generator(self._run_non_batch_extractor(self._get_nodes_from_temp_dir(node_file_paths.keys()))))

        else:

            s3_client = GraphRAGConfig.s3
            bedrock_client = GraphRAGConfig.bedrock

            # 1 - Split nodes into batches (if needed)
            node_file_path_batches = split_nodes(list(node_file_paths.keys()), self.batch_config.max_batch_size)
            logger.debug(f'[{self.description} batch] Split nodes into batches [num_batches: {len(node_file_path_batches)}, sizes: {[len(b) for b in node_file_path_batches]}]')

            # 2 - Process batches concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.batch_config.max_num_concurrent_batches) as executor:

                futures = [
                    executor.submit(self._process_single_batch, batch_index, self._get_nodes_from_temp_dir(node_batch), s3_client, bedrock_client)
                    for batch_index, node_batch in enumerate(node_file_path_batches)
                ]
                
                executor.shutdown()

                for future in futures:
                    results_generators.append(future.result())

        final_node_file_paths = []

        # 3 - Process results
        for results_generator in results_generators:
            for (node_id, text) in results_generator:

                # The id comes back from the extraction results (the batch output's
                # recordId on the batch path), so it is no more trusted than the one
                # on the way in. A read, but still a read of whatever path it names.
                validate_id_segment(node_id, 'node_id')
                node_file_path = join(temp_dir, f'{node_id}.json')
                node = next(self._get_nodes_from_temp_dir([node_file_path]))

                node = self._update_node(node, { node.node_id: text })
                node = self._post_process_node(node, excluded_embed_metadata_keys, excluded_llm_metadata_keys)

                self._save_node_in_temp_dir(node, temp_dir)
                
                del node_file_paths[node_file_path]
                final_node_file_paths.append(node_file_path)
        
        # 4 - Handle nodes that have not been extracted
        if len(node_file_paths.keys()) > 0:
            logger.debug(f'[{self.description} batch] {len(node_file_paths.keys())} nodes without extracted data')

        for node_file_path in node_file_paths.keys():
            node = next(self._get_nodes_from_temp_dir([node_file_path]))
            node = self._update_node(node, { node.node_id: None })
            node = self._post_process_node(node, excluded_embed_metadata_keys, excluded_llm_metadata_keys)

            self._save_node_in_temp_dir(node, temp_dir)

            del node_file_paths[node_file_path]
            final_node_file_paths.append(node_file_path)

        
        final_node_file_paths.sort()

        for node in self._get_nodes_from_temp_dir(final_node_file_paths):
            yield node

        if self.batch_config.delete_on_success:

            def log_delete_error(function, path, excinfo):
                logger.error(f'[{self.description} batch] Error deleting {path} - {str(excinfo[1])}' )

            logger.debug(f'[{self.description} batch] Deleting temp node directory: {temp_dir}' )
            shutil.rmtree(temp_dir, onerror=log_delete_error)

        
    def __call__(self, 
        nodes: Sequence[BaseNode],
        excluded_embed_metadata_keys: Optional[List[str]] = None,
        excluded_llm_metadata_keys: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        return [
            node 
            for node in self._process_nodes(nodes, excluded_embed_metadata_keys, excluded_llm_metadata_keys, **kwargs)
        ]
    
