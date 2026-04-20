# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import sys
import time
import datetime
import boto3
import json
import logging
import argparse
import importlib
import importlib.metadata
from dotenv import load_dotenv

from graphrag_toolkit_tests import *
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

TIMEOUT_SECONDS = 120

def get_lexical_graph_version():

    try:
        return importlib.metadata.version('graphrag-lexical-graph')
    except importlib.metadata.PackageNotFoundError:
        try:
            from graphrag_toolkit.lexical_graph._version import __version__
            return __version__
        except ImportError:
            return 'unknown'

def to_test_class(t):
    parts = t.split('.')
    module_name = f'graphrag_toolkit_tests.{parts[0]}'
    class_name = parts[1]
    m = importlib.import_module(module_name)
    c = getattr(m, class_name)
    return c

def clear_test_output_dirs():
    def clear_dir(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
            
    clear_dir('test-logs')
    clear_dir('test-results')
    
def notify(topic_arn, subject, msg):
    sns_client = boto3.client('sns')
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=msg,
            Subject=subject
        )    
        
def publish_test_run_metadata(test_run, metadata, test_list, s3_results_bucket, s3_results_prefix):
    
    s3_client = boto3.client('s3')
    
    metadata_copy = metadata.copy()
    metadata_copy['tests'] = [t for t in test_list if t]
    metadata_json = json.dumps(metadata_copy, indent=2)
    
    s3_key = f'{s3_results_prefix}/test-runs/{test_run}/metadata.json'
    
    s3_client.put_object(
        Bucket=s3_results_bucket,
        Key=s3_key,
        Body=(bytes(json.dumps(metadata_copy, indent=2).encode('UTF-8'))),
        ContentType='application/json',
        ServerSideEncryption='AES256'
    )
            
          
def wait_till_stack_complete(stack_id):
    
    cfn_client = boto3.client('cloudformation')
    
    status = 'CREATE_IN_PROGRESS'
    
    while status == 'CREATE_IN_PROGRESS':
        
        time.sleep(5)
    
        response = cfn_client.describe_stacks(
            StackName=stack_id
        )
        
        status = response['Stacks'][0]['StackStatus']
        
        print(f'Status: {status}')
        
    return status 
    
def delete_stack(stack_id, delete_stack_role):
    
    cfn_client = boto3.client('cloudformation')
    
    if delete_stack_role:
    
        print('Deleting stack...')
        
        response = cfn_client.describe_stacks(
            StackName=stack_id
        )
        
        parent_id = response['Stacks'][0]['ParentId']
        
        
        cfn_client.delete_stack(
            StackName=parent_id,
            RoleARN=delete_stack_role
        )
    else:
         print('No role ARN, so not deleting stack')
         
         
def test_suite_result(test_results):
    return 'FAIL' if any([test_result['result'] == 'FAIL' for test_result in test_results]) else 'PASS'

def run_test_suite():
    
    load_dotenv()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-mode', nargs='?', help = 'Test mode')
    parser.add_argument('--test', nargs='*')
    args, _ = parser.parse_known_args()
    args_dict = { k:v for k,v in vars(args).items() if v}
    
    s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
    s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
    stack_id = os.environ['STACK_ID']
    application_id = os.environ.get('APPLICATION_ID', 'gr-test')
    topic_arn = os.environ.get('TOPIC_ARN', None)
    aws_region_name = os.environ.get('AWS_REGION_NAME', None)
    neptune_engine_version = os.environ.get('NEPTUNE_ENGINE_VERSION', None)
    test_description = os.environ.get('TEST_DESCRIPTION', 'graphrag-toolkit integration test')
    delete_on_pass = os.environ.get('DELETE_ON_PASS', 'False').lower() == 'true'
    fail_fast = os.environ.get('FAIL_FAST', 'False').lower() == 'true'
    delete_stack_role = os.environ.get('DELETE_STACK_ROLE', None)
    test_list = args_dict.get('test', os.environ.get('TESTS', '').strip().split(' '))

    print(f"S3 results bucket: {s3_results_bucket}")
    print(f"S3 results prefix: {s3_results_prefix}")
    print(f"StackId          : {stack_id}")
    print(f"TopicArn         : {topic_arn}")
    print(f"Region           : {aws_region_name}")
    print(f"Neptune version  : {neptune_engine_version}")
    print(f"Delete on pass   : {delete_on_pass}")
    print(f"Fail fast        : {fail_fast}")
    print(f"Delete stack role: {delete_stack_role}")
    print(f"Test list        : {test_list}")
    
    status = wait_till_stack_complete(stack_id)
    
    print(f'Stack finished with status: {status}')
    
    if not test_list:
        print('Exiting without running tests because test list is empty')
        return None

    if status == 'CREATE_COMPLETE':
        
        print('Running tests...')
        
        clear_test_output_dirs()
        
        params = {}
        test_results = []
        
        start = int(time.time())
        
        results = {
            'description': test_description,
            'start': str(datetime.datetime.utcfromtimestamp(start)),
            'end': None,
            'duration_seconds': None,
            'env': {
                'graphrag_toolkit': os.environ['GRAPHRAG_TOOLKIT_S3_URI'],
                'lexical_graph_version': get_lexical_graph_version(),
                'graph_store': os.environ['GRAPH_STORE'],
                'vector_store': os.environ['VECTOR_STORE'],
                'delete_on_pass': delete_on_pass
            }
        }
        
        env_type = f"{ os.environ['GRAPH_STORE'].split(':')[0]}-{ os.environ['VECTOR_STORE'].split(':')[0]}"
        
        if env_type.startswith('neptune-db'):
            results['env']['neptune_engine_version'] = neptune_engine_version
            
        def update_results_env(env_key, results_env_key):
            value = os.environ.get(env_key, None)
            if value:
                results['env'][results_env_key] = value
                
        update_results_env('TEST_EXTRACTION_LLM', 'extraction_llm')
        update_results_env('TEST_RESPONSE_LLM', 'response_llm')
        update_results_env('EMBEDDINGS_MODEL', 'embeddings_model')
        update_results_env('LEXICAL_GRAPH_INSTALL_URI', 'lexical_graph_install_uri')
        update_results_env('BYOKG_RAG_INSTALL_URI', 'byokg_rag_install_uri')
        
        if 'embeddings_model' not in results['env']:
            try:
                from graphrag_toolkit.lexical_graph import GraphRAGConfig
            except (ImportError, AttributeError):
                results['env']['embeddings_model'] = 'unknown'
                results['env']['embeddings_dimensions'] = 'unknown'
            else:
                embed_model = GraphRAGConfig.embed_model
                model_name = getattr(embed_model, 'model_name', str(embed_model))
                results['env']['embeddings_model'] = f'{model_name} (default)'
                results['env']['embeddings_dimensions'] = GraphRAGConfig.embed_dimensions
        else:
            embeddings_dimensions = os.environ.get('EMBEDDINGS_DIMENSIONS', None)
            if embeddings_dimensions:
                results['env']['embeddings_dimensions'] = embeddings_dimensions
        
        publish_test_run_metadata(start, results, test_list, s3_results_bucket, s3_results_prefix)
        
        tests = [to_test_class(t) for t in test_list if t]
        
        for index, t in enumerate(tests):
            if test_suite_result(test_results) == 'FAIL' and fail_fast:
                break
            with IntegrationTestHandler(start, index, s3_results_bucket, s3_results_prefix, test_results) as handler:
                try:
                    test = t()
                    test.init_test_details(handler, params)
                    start_wait_time = time.time()
                    while test.wait():
                        time.sleep(5)
                        end_wait_time = time.time()
                        if int(end_wait_time - start_wait_time) > TIMEOUT_SECONDS:
                            raise RuntimeError(f'Wait timed out after {TIMEOUT_SECONDS} seconds')
                    test.run_test(handler, params)
                    handler.succeeded()
                except Exception as e:
                    handler.add_exception(e)
                    
        end = int(time.time())
                    
        result = 'FAIL' if any([test_result['result'] == 'FAIL' for test_result in test_results]) else 'PASS'
        subject = f'{application_id} - {env_type} - {result} ({aws_region_name})'
        
        results['end'] = str(datetime.datetime.utcfromtimestamp(end))
        results['duration_seconds'] = end-start
        results['result_summary'] = result
        results['results'] = test_results
        
        results_json = json.dumps(results, indent=2)
        
        print(results_json)
        
        notify(topic_arn, subject, results_json)
        
        if result == 'PASS' and delete_on_pass:
            delete_stack(stack_id, delete_stack_role)
            
            
    else:
        print('Exiting tests because of CloudFormation failure')


if __name__ == '__main__':
    start = time.time()
    run_test_suite()
    end = time.time()

    print(end-start)