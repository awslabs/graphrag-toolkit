# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
import json
import boto3
import logging
import os
import unittest
import traceback
from typing import Any, Dict, List
from copy import deepcopy

from graphrag_toolkit.lexical_graph import set_logging_config

logger = logging.getLogger(__name__)


class IntegrationTestHandler():
    def __init__(self, test_run:int, index:int, s3_results_bucket:str, s3_results_prefix:str, results:List[Dict[str, Any]]):
        self.test_run = test_run
        self.index = index
        self.s3_results_bucket = s3_results_bucket
        self.s3_results_prefix = s3_results_prefix
        self.results = results
        self.skipped = False
        self.props = {
            'name': None,
            'description': None,
            'init_time': 0,
            'start_time': 0,
            'end_time': 0,
            'duration_seconds': None,
            'result': 'FAIL',
            'input_params': {},
            'exceptions': [],
            'test_results': {},
            'output': []           
        }
        
    def init_with_test_details(self, name:str, description:str, params:Dict[str, Any]):
        self.props['name'] = name
        self.props['description'] = description
        self.props['input_params'] = deepcopy(params)
        
        file_path = f"test-logs/{self.index:02d}-{self.props['name']}.log"
        if os.path.exists(file_path):
            os.remove(file_path)
        
        set_logging_config(
            'DEBUG', 
            ['graphrag_toolkit'],
            filename=file_path
        )
        
        self.props['init_time'] = int(time.time())
    
    def start_test(self):
        self.props['start_time'] = int(time.time())
        
    def add_output(self, key:str, value:Any):
        self.props['output'].append({
            'name': key,
            'timestamp': int(time.time()),
            'value': value
        })
        
    def run_assertions(self, assertions:unittest.TestCase):
        
        def format_test_name(s):
            return s.split(' ')[0]
        
        def format_test_case(f):
            test_case = f[0]
            msg = f[1]
            return {
                'name': format_test_name(str(test_case)),
                'description': test_case.shortDescription(),
                'message': msg
            }
        
        def format_param(s):
            if len(s) > 100:
                return f'{s[:100]} ... <{len(s) - 100} more chars>'
            else:
                return s
        
        runner = unittest.TextTestRunner()
        suite = unittest.makeSuite(assertions)
        
        
        
        all_tests = {
            str(test_case) : {
                'name': format_test_name(str(test_case)),
                'description': test_case.shortDescription()
            }
            for test_case in suite
        }   
        
        
        
        print(f'all_tests: {all_tests}') 
        
        r = runner.run(suite)
        
        suite_param_keys = [
            k 
            for k in assertions.__dict__.keys()
            if k.startswith('_') and k not in [
                '__module__', 
                'setUpClass', 
                '__doc__', 
                '_classSetupFailed', 
                '_class_cleanups', 
                'tearDown_exceptions'
            ]
        ]
        
        suite_params = {
            k:format_param(str(v)) for k, v in assertions.__dict__.items() if k in suite_param_keys
        }
        
        test_results = {
            'passed': r.wasSuccessful(),
            'num_tests': r.testsRun,
            'num_failures': len(r.failures),
            'num_errors': len(r.errors),
            'successes': [],
            'failures': [format_test_case(f) for f in r.failures],
            'errors': [format_test_case(e) for e in r.errors],
            'suite_params': suite_params
        }
        
        for f in r.failures:
            failure_key = str(f[0])
            print(f'failure_key: {failure_key}')
            if failure_key in all_tests:
                del all_tests[failure_key]
            
        for e in r.errors:
            error_key = str(e[0])
            print(f'error_key: {error_key}')
            if error_key in all_tests:
                del all_tests[error_key]
            
        test_results['successes'] = list(all_tests.values())   
        
        self.props['test_results'] = test_results
        
    def skip(self):
        self.skipped = True
    
    def succeeded(self):
        result = 'PASS' if self.props['test_results'].get('passed', False) else 'FAIL'
        result = 'SKIPPED' if self.skipped else result
        self.props['result'] = result
        
    def add_exception(self, ex):
        self.props['exceptions'].append(''.join(traceback.TracebackException.from_exception(ex).format()))
        logger.exception(ex, stack_info=True)        
        
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, exception_traceback):
        
        self.props['end_time'] = int(time.time())
        if self.props['start_time'] > 0:
            self.props['duration_seconds'] = self.props['end_time'] - self.props['start_time']
        else:
            self.props['duration_seconds'] = self.props['end_time'] - self.props['init_time']
        
        results_file_name = f"{self.index:02d}-{self.props['name']}.{self.props['result']}.json"
        results_file_path = f'test-results/{results_file_name}'
        s3_results_key = f'{self.s3_results_prefix}/test-runs/{self.test_run}/test-results/{results_file_name}'
        
        log_file_name = f"{self.index:02d}-{self.props['name']}.log"
        log_file_path = f'test-logs/{log_file_name}'
        s3_log_key = f'{self.s3_results_prefix}/test-runs/{self.test_run}/test-logs/{log_file_name}'
        
        with open(results_file_path, 'w') as out_file:
            json.dump(self.props, out_file, indent=2, ensure_ascii=False)
            
        client = boto3.client('s3')
        
        client.upload_file(
            results_file_path, 
            self.s3_results_bucket, 
            s3_results_key, 
            {
                'ServerSideEncryption':'AES256',
                'ContentType': 'application/json'
            }
        )
        
        result = {
            'name': self.props['name'],
            'result': self.props['result'],
            'duration_seconds': self.props['duration_seconds'],
            'details': f's3://{self.s3_results_bucket}/{s3_results_key}'
        }
        
        if os.path.exists(log_file_path):
            
            result['logs'] = f's3://{self.s3_results_bucket}/{s3_log_key}'

            client.upload_file(
                log_file_path, 
                self.s3_results_bucket, 
                s3_log_key,
                {
                    'ServerSideEncryption':'AES256',
                    'ContentType': 'text/plain'
                }
            )
            
        self.results.append(result)

        