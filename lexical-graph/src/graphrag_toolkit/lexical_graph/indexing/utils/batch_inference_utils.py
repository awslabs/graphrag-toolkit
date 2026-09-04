# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import asyncio
import time
import os
import json
from typing import Any, Callable, List, Dict
from dataclasses import dataclass
from os import stat, listdir
from os.path import isfile, join

from tenacity import retry, stop_after_attempt, wait_exponential
from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph import BatchJobError
from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig

from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.llms.anthropic.utils import messages_to_anthropic_messages
from llama_index.llms.bedrock_converse.utils import messages_to_converse_messages
from llama_index.core.schema import TextNode
from llama_index.core.prompts import PromptTemplate
from llama_index.core.base.llms.types import ChatMessage


logger = logging.getLogger(__name__)

BEDROCK_MIN_BATCH_SIZE = 100
BEDROCK_MAX_BATCH_SIZE = 50000

def get_file_size_mb(filepath):
    file_stats = stat(filepath)
    return round(file_stats.st_size / (1024 * 1024), 2)

def get_file_sizes_mb(dir):
    files = [f for f in listdir(dir) if isfile(join(dir, f))]
    return {
        f:get_file_size_mb(join(dir, f)) for f in files
    }


def split_nodes(nodes: List[Any], batch_size: int) -> List[List[Any]]:   
    
    if batch_size < BEDROCK_MIN_BATCH_SIZE:
        raise BatchJobError(f'Batch size ({batch_size}) is smaller than the minimum required by Bedrock ({BEDROCK_MIN_BATCH_SIZE})')
    if batch_size > BEDROCK_MAX_BATCH_SIZE:
        raise BatchJobError(f'Batch size ({batch_size}) is larger than the maximum required by Bedrock ({BEDROCK_MAX_BATCH_SIZE})')
    if not nodes:
        raise BatchJobError('Empty list of records')
    if len(nodes) < BEDROCK_MIN_BATCH_SIZE:
        raise BatchJobError(f'Job contains fewer records ({len(nodes)}) than the minimum required by Bedrock ({BEDROCK_MIN_BATCH_SIZE})')
    
    i = 0
    results = []

    while i < len(nodes):
        if len(nodes) - (i + batch_size) < BEDROCK_MIN_BATCH_SIZE:
            results.append(nodes[i:])
            break
        else:
            results.append(nodes[i:i + batch_size])
        i += batch_size
   
    return results

# --- Bedrock batch (InvokeModel JSONL) provider registry -------------------
#
# Batch inference does not go through the Converse API; each record's
# `modelInput`/`modelOutput` uses the provider-specific InvokeModel schema, so
# each family is described by one BATCH_MODEL_PROVIDERS entry:
#
#   * build_request   - builds `modelInput` (the request body genuinely differs
#                       per family, so this stays a small function)
#   * output_path     - where the generated text lives in a record, walked from
#                       the record root (so a family can read under 'modelOutput'
#                       or at the top level)
#   * output_mode     - 'blocks' joins a [{text: ...}] content list; 'text'
#                       returns a scalar string node as-is
#
# To add a family, verify its InvokeModel request/response schema against the AWS
# Bedrock docs and add one entry - no edits to get_request_body /
# get_parse_output_text_fn. match_prefixes are substrings tested against
# llm.model; they match through a cross-region inference-profile prefix (e.g.
# 'us.anthropic.claude...' contains 'anthropic.claude') and across model versions
# (Claude Opus 5, Nova 2, Llama 4).


def _build_nova_request(messages: List[ChatMessage], params: dict) -> dict:
    converse_messages, system_prompt = messages_to_converse_messages(messages)
    request_body = {
        'messages': converse_messages,
        'inferenceConfig': {
            'maxTokens': params['max_tokens'],
            'temperature': params['temperature'],
        }
    }
    if system_prompt:
        request_body['system'] = [{'text': system_prompt}]
    return request_body


def _build_claude_request(messages: List[ChatMessage], params: dict) -> dict:
    anthropic_messages, system_prompt = messages_to_anthropic_messages(messages)
    request_body = {
        'anthropic_version': params.get('anthropic_version', 'bedrock-2023-05-31'),
        'messages': anthropic_messages,
        'max_tokens': params['max_tokens'],
        'temperature': params['temperature']
    }
    if system_prompt:
        request_body['system'] = system_prompt
    return request_body


def _build_llama_request(messages: List[ChatMessage], params: dict) -> dict:
    converse_messages, system_prompt = messages_to_converse_messages(messages)
    return {
        'messages': converse_messages,
        'parameters': {
            'max_new_tokens': params['max_tokens'],
            'temperature': params['temperature'],
        }
    }


@dataclass(frozen=True)
class BatchModelProvider:
    """A model family's batch (InvokeModel JSONL) request builder and output spec."""
    name: str
    match_prefixes: tuple
    build_request: Callable[[List[ChatMessage], dict], dict]
    output_path: tuple
    output_mode: str = 'blocks'


BATCH_MODEL_PROVIDERS: List[BatchModelProvider] = [
    BatchModelProvider(
        name='amazon.nova',
        match_prefixes=('amazon.nova',),
        build_request=_build_nova_request,
        output_path=('modelOutput', 'output', 'message', 'content'),
    ),
    BatchModelProvider(
        name='anthropic.claude',
        match_prefixes=('anthropic.claude',),
        build_request=_build_claude_request,
        output_path=('modelOutput', 'content'),
    ),
    BatchModelProvider(
        name='meta.llama',
        match_prefixes=('meta.llama',),
        build_request=_build_llama_request,
        output_path=('generation',),
        output_mode='text',
    ),
]


def _resolve_batch_model_provider(model_id: str) -> BatchModelProvider:
    for provider in BATCH_MODEL_PROVIDERS:
        if any(prefix in model_id for prefix in provider.match_prefixes):
            return provider
    supported = ', '.join(provider.name for provider in BATCH_MODEL_PROVIDERS)
    raise ValueError(
        f'Unrecognized model_id: batch extraction for {model_id} is not supported. '
        f'Supported model families: {supported}'
    )


def _parse_output_text(json_data: dict, output_path: tuple, output_mode: str) -> str:
    """Extract generated text from a batch output record per a provider's output spec."""
    node = json_data
    for key in output_path:
        if not isinstance(node, dict):
            node = None
            break
        node = node.get(key)
    if output_mode == 'text':
        return node if isinstance(node, str) else ''
    return ''.join(block.get('text', '') for block in (node or []))


def get_request_body(llm:BedrockConverse, messages:List[ChatMessage], inference_parameters: dict):
    return _resolve_batch_model_provider(llm.model).build_request(messages, inference_parameters)


def create_inference_inputs_for_messages(llm:BedrockConverse, nodes: List[TextNode], messages_batch: List[List[ChatMessage]], **kwargs) -> List[Dict[str, Any]]:
    inference_parameters = llm._get_all_kwargs(**kwargs)   
    json_outputs = []
    for node, messages in zip(nodes, messages_batch):        
        json_structure = {
            'recordId': node.node_id,
            'modelInput': get_request_body(llm, messages, inference_parameters)
        }
        json_outputs.append(json_structure)
    return json_outputs

def create_inference_inputs(llm:BedrockConverse, nodes: List[TextNode], prompts: List[str], **kwargs) -> List[Dict[str, Any]]:
    all_kwargs = llm._get_all_kwargs(**kwargs)   
    json_outputs = []
    for node, prompt in zip(nodes, prompts):    
        prompt = llm.completion_to_prompt(prompt)
        json_structure = {
            'recordId': node.node_id,
            'modelInput': llm._provider.get_request_body(prompt, all_kwargs)
        }
        json_outputs.append(json_structure)
    return json_outputs

def create_and_run_batch_job(job_name_prefix:str,
                             bedrock_client: Any, 
                             timestamp:str, 
                             batch_suffix:str,
                             batch_config:BatchConfig,
                             input_key:str,
                             output_path:str, 
                             model_id:str) -> None:
    """Create and run a Bedrock batch inference job."""
    try:
        input_data_config = {
            's3InputDataConfig': {'s3Uri': f's3://{batch_config.bucket_name}/{input_key}'}
        }
        output_data_config = {
            's3OutputDataConfig': {'s3Uri': f's3://{batch_config.bucket_name}/{output_path}'}
        }

        if batch_config.s3_encryption_key_id:
            output_data_config['s3EncryptionKeyId'] = batch_config.s3_encryption_key_id

        start = time.time()

        
        response = None
        if batch_config.subnet_ids and batch_config.security_group_ids:
            response = bedrock_client.create_model_invocation_job(
                roleArn=batch_config.role_arn,
                modelId=model_id,
                jobName=f'{job_name_prefix}-{timestamp}-{batch_suffix}',
                inputDataConfig=input_data_config,
                outputDataConfig=output_data_config,
                vpcConfig={
                    'subnetIds': batch_config.subnet_ids,
                    'securityGroupIds': batch_config.security_group_ids
                }
            )
        else:
            response = bedrock_client.create_model_invocation_job(
                roleArn=batch_config.role_arn,
                modelId=model_id,
                jobName=f'{job_name_prefix}-{timestamp}-{batch_suffix}',
                inputDataConfig=input_data_config,
                outputDataConfig=output_data_config
            )

        job_arn = response.get('jobArn')

        input_file = input_key.split('/')[-1]

        logger.info(f'Created batch job [job_arn: {job_arn}, input_file: {input_file}]')

        wait_for_job_completion(bedrock_client, job_arn, input_file)

        end = time.time()

        logger.debug(f'Batch job completed successfully [job_arn: {job_arn}, input_file: {input_file}] ({int(end - start)} seconds)')

    except ClientError as e:
        logger.error(f'Error creating or running batch job: {str(e)}')
        raise BatchJobError(f'{e!s}') from e 

def wait_for_job_completion(bedrock_client: Any, job_arn: str, input_file:str) -> None:
    """Wait for a Bedrock batch job to complete."""
    status = 'Started'
    while status not in ['Completed', 'Failed', 'Stopped', 'PartiallyCompleted', 'Expired']:
        time.sleep(60)
        logger.debug(f'Waiting for batch job to complete... [job_arn: {job_arn}, input_file: {input_file}, status: {status}]')
        response = bedrock_client.get_model_invocation_job(jobIdentifier=job_arn)
        status = response['status']
    
    if status != 'Completed':
        logger.error(f'Batch job failed [job_arn: {job_arn}, input_file: {input_file}, status: {status}]')
        raise BatchJobError(f"Batch job failed [job_arn: {job_arn}, input_file: {input_file}, status: {status}] - {response['message']}") 
    
    

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_output_files(s3_client: Any, bucket_name:str, output_path:str, input_filename:str, local_directory:str) -> None:
    """Download output files from S3 by searching for a folder containing a file matching the input filename."""
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=output_path)

    output_folder = None
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if os.path.basename(key).startswith(input_filename):
                output_folder = os.path.dirname(key)
                break
        if output_folder:
            break

    if not output_folder:
        raise BatchJobError(f"No folder containing a file matching '{input_filename}' was found in bucket {bucket_name}.")

    output_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=output_folder)
    for obj in output_files.get('Contents', []):
        key = obj['Key']
        if key.endswith('/'):
            continue
        
        local_file_path = os.path.join(local_directory, os.path.relpath(key, output_folder))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        logger.debug(f'Started downloading {key} to {local_file_path}')
        s3_client.download_file(Bucket=bucket_name, Key=key, Filename=local_file_path)
        logger.debug(f'Finished downloading {key} to {local_file_path}')

def get_parse_output_text_fn(model_id:str):
    provider = _resolve_batch_model_provider(model_id)
    def parse_output(json_data):
        return _parse_output_text(json_data, provider.output_path, provider.output_mode)
    return parse_output

async def process_batch_output(local_output_directory:str, input_filename:str, llm:LLMCache) -> Dict[str, str]:
    """Process batch output files and return results."""
    results = {}
    failed_records = []

    process_output_start = time.time()

    parse_output_text = get_parse_output_text_fn(llm.llm.model)

    logger.debug(f'[Batch outputs] Started processing all outputs for {input_filename}')

    for filename in os.listdir(local_output_directory):
        if filename.startswith(input_filename):
            output_filepath = os.path.join(local_output_directory, filename)
            logger.debug(f'[Batch outputs] Started parsing output file {output_filepath}')
            with open(output_filepath, 'r', encoding='utf-8') as jsonl_file:
                for line in jsonl_file:
                    json_data = json.loads(line)
                    record_id = json_data.get('recordId')
                    error = json_data.get('error')
                    if not error:
                        model_output_text = parse_output_text(json_data)
                        results[record_id] = model_output_text
                    else:
                        failed_records.append((record_id, json_data.get('modelInput', {}).get('messages', [{}])[0].get('content', [{}])[0].get('text', '')))

    logger.debug(f'[Batch outputs] Finished parsing all outputs for {input_filename} [succeeded: {len(results.keys())}, failed: {len(failed_records)}]')

    async def process_failed_record(record):
        record_id, text = record
        
        def blocking_llm_call():
            return llm.predict(PromptTemplate(text))
        
        try:
            coro = asyncio.to_thread(blocking_llm_call)
            response = await coro
            logger.debug(f'[Batch outputs] Successfully processed failed record {record_id}')
            return record_id, response
        except Exception as e:
            logger.error(f'[Batch outputs] Error processing failed record {record_id}: {str(e)}')
            return record_id, None

    if failed_records:
        logger.debug(f'[Batch outputs] Processing failed records for {input_filename}')
    
    failed_results = await asyncio.gather(*[process_failed_record(record) for record in failed_records])
    
    for record_id, response in failed_results:
        if response:
            results[record_id] = response

    process_output_end = time.time()

    logger.debug(f'[Batch outputs] Finished processing all outputs for {input_filename} ({int((process_output_end - process_output_start) * 1000)} millis)')

    return results

def process_batch_output_sync(local_output_directory:str, input_filename:str, llm:LLMCache):
    
    success_count = 0
    failure_count = 0
   
    process_output_start = time.time()

    parse_output_text = get_parse_output_text_fn(llm.llm.model)

    logger.debug(f'[Batch outputs] Started processing all outputs for {input_filename}')

    def process_failed_record(record_id, text, error):
        try:
            response = llm.predict(PromptTemplate(text))
            return (record_id, response)
        except Exception as e:
            logger.error(f'[Batch outputs] Error processing failed record {record_id}: {str(e)} [original_error: {str(error)}]')
            return (record_id, None)


    for filename in os.listdir(local_output_directory):
        if filename.startswith(input_filename):
            output_filepath = os.path.join(local_output_directory, filename)
            logger.debug(f'[Batch outputs] Started parsing output file {output_filepath}')
            with open(output_filepath, 'r', encoding='utf-8') as jsonl_file:
                for line in jsonl_file:
                    json_data = json.loads(line)
                    record_id = json_data.get('recordId')
                    error = json_data.get('error')
                    if not error:
                        model_output_text = parse_output_text(json_data)
                        success_count += 1
                        yield (record_id, model_output_text)
                    else:
                        (record_id, response) = process_failed_record(record_id, json_data.get('modelInput', {}).get('messages', [{}])[0].get('content', [{}])[0].get('text', ''), error)
                        if response:
                            success_count += 1
                        else:
                            failure_count += 1
                            logger.debug
                        yield (record_id, response)

    process_output_end = time.time()
                        
    logger.debug(f'[Batch outputs] Finished parsing all outputs for {input_filename} [succeeded: {success_count}, failed: {failure_count}] ({int((process_output_end - process_output_start) * 1000)} millis)')
