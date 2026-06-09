# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
import logging
import smart_open

logger = logging.getLogger(__name__)

def read_text(path):
    """
    Reads the contents of a text file specified by the given file path and returns
    the content as a string.

    Args:
        path (str): The path to the text file to be read.

    Returns:
        str: The contents of the file as a string.
    """
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()
            
def write_text(path,text):
    """
    Writes the specified text to a file at the given path. Creates any necessary
    directories in the path if they do not already exist.

    Args:
        path: A string representing the file path where the text will be written.
        text: A string representing the text content to write to the file.
    """
    os.makedirs(os.path.dirname(os.path.realpath(path)), exist_ok=True)
    with open(path, "w", encoding='utf-8') as f:
        f.write(text)
        
def write_json(path, j):
    """
    Writes a given JSON object to a file at a specified path. Creates any necessary
    parent directories if they do not exist prior to writing the file. The content is
    dumped in a way that ensures non-ASCII characters are preserved.

    Args:
        path (str): The file path where the JSON should be written.
        j (dict): The JSON object to be written to the specified file.
    """
    os.makedirs(os.path.dirname(os.path.realpath(path)), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(j, f, ensure_ascii=False, indent=2)

def read_json(path):
    """
    Reads a JSON file from the specified file path, parses its content, and returns the
    corresponding Python dictionary or list.

    This function is designed to read JSON files with UTF-8 encoding. It opens the file
    in read mode, reads its content, and converts the JSON content into a Python object
    using the `json.loads` function.

    Args:
        path (str): The file path to the JSON file to be read.

    Returns:
        Union[dict, list]: A Python dictionary or list extracted from the JSON file.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    with open(path, 'r', encoding='utf-8') as f:
        return json.loads(f.read())

def s3_read_data(s3_path):
    """
    Reads data from the given S3 path using the `smart_open` library and returns
    the data as a string. This function provides an interface for downloading
    and reading content from an S3 bucket. If the file does not exist or another
    OS-level error occurs during the read operation, the error is printed, and it
    is re-raised.

    Args:
        s3_path (str): The path to the file stored in the S3 bucket. This should
            include the necessary protocol (e.g., 's3://') and the file location.

    Returns:
        str: The content of the file located at the specified S3 path as a string.

    Raises:
        OSError: If there is an error accessing or reading the file at the given
            S3 path.
    """
    try:
        with smart_open.smart_open(s3_path, 'r') as f:
            return f.read()
    except OSError as e:
        print(f'Failed to read {s3_path}')
        raise e
        
def s3_write_data(s3_path, data):
    """
    Writes data to a specified S3 path using smart_open.

    This function utilizes the smart_open library to write the provided data
    to an S3 path. It handles any file-related errors during the write
    operation and raises them for further handling.

    Args:
        s3_path: The S3 path where data will be written.
        data: The content to be written to the S3 path.

    Raises:
        OSError: If the write operation to the specified S3 path fails.
    """
    try:
        with smart_open.smart_open(s3_path, 'w') as f:
            f.write(data)
    except OSError as e:
        print(f'Failed to write {s3_path}')
        raise e