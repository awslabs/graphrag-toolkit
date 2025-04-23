# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import logging
import smart_open

logger = logging.getLogger(__name__)

def read_text(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()
            
def write_text(path,text):      
    os.makedirs(os.path.dirname(os.path.realpath(path)), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)
        
def write_json(path, j):
    os.makedirs(os.path.dirname(os.path.realpath(path)), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(j, f, ensure_ascii=False)

def read_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.loads(f.read())

def s3_read_data(s3_path):
    try:
        with smart_open.smart_open(s3_path, 'r') as f:
            return f.read()
    except OSError as e:
        print(f'Failed to read {s3_path}')
        raise e
        
def s3_write_data(s3_path, data):
    try:
        with smart_open.smart_open(s3_path, 'w') as f:
            f.write(data)
    except OSError as e:
        print(f'Failed to write {s3_path}')
        raise e