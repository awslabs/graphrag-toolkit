# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from typing import Dict, List, Optional, Protocol, Any

from graphrag_toolkit.core.types import Document

class TextExtractorFunction(Protocol):
    """Defines a protocol for a callable that extracts text from a dictionary.

    This protocol specifies the blueprint for any callable object or function
    that takes a dictionary with string keys and values of any type as input
    and returns a string output after performing the intended text extraction.

    Attributes:
        None
    """
    def __call__(self, data:Dict[str,Any]) -> str:
        """
        Processes the input data and provides a string output as per the implementation.

        This method is invoked to process a dictionary containing data and returns
        a string result based on the operation defined within the method logic.

        Args:
            data: A dictionary with keys as strings and values of any type that represents
                the data to process.

        Returns:
            A string that represents the outcome of the processing operation applied
            on the input data.
        """
        pass

class MetadataExtractorFunction(Protocol):
    """Defines a protocol for metadata extraction functions.

    This class serves as a protocol for defining functions that extract metadata
    from a given data input. It enforces the structure of such functions to ensure
    that they accept a dictionary of arbitrary key-value pairs as input and return
    a dictionary as the output. This can be used as a type for enforcing
    consistency in functions performing metadata extraction.

    Attributes:
        None
    """
    def __call__(self, data:Dict[str,Any]) -> Dict[str,Any]:
        """
        Invokes an instance of the class as a callable to process input dictionary data
        and return a processed dictionary.

        Args:
            data: A dictionary containing keys and values to be processed.

        Returns:
            A dictionary containing the processed output from the input data.
        """
        pass

class JSONArrayReader:
    """
    A reader class for loading and processing JSON array data.

    This class is designed to read JSON-encoded data from a file, process the
    data to extract text and metadata, and return it as a list of `Document`
    instances. It supports customization for text and metadata extraction through
    user-defined callback functions.

    Attributes:
        ensure_ascii (bool): Determines whether non-ASCII characters get escaped
            in the JSON output when text extraction is not provided. Defaults to False.
        text_fn (Optional[TextExtractorFunction]): A function to extract text from
            individual JSON objects. If None, the full JSON object is serialized
            as a string.
        metadata_fn (Optional[MetadataExtractorFunction]): A function to extract
            metadata from individual JSON objects. If None, metadata must come
            from extra_info or defaults to an empty dictionary.
    """
    def __init__(self, 
                 ensure_ascii:bool=False, 
                 text_fn:Optional[TextExtractorFunction]=None, 
                 metadata_fn:Optional[MetadataExtractorFunction]=None,
                 is_jsonl:Optional[bool]=False):
        """
        Initializes an instance of the class with options to configure ASCII handling,
        text extraction, and metadata extraction.

        Args:
            ensure_ascii (bool): Determines whether ASCII-only encoding is enforced.
                If set to True, non-ASCII characters will be escaped in the output.
            text_fn (Optional[TextExtractorFunction]): A callable function for extracting
                text content, used to customize or override the default behavior.
            metadata_fn (Optional[MetadataExtractorFunction]): A callable function for
                extracting metadata information, used to customize or override the
                default behavior.
        """
        self.ensure_ascii = ensure_ascii
        self.text_fn = text_fn
        self.metadata_fn = metadata_fn
        self.is_jsonl = is_jsonl
        
    def _get_metadata(self, data:Dict, extra_info:Dict):
        """
        Retrieves and constructs metadata for the given data. Metadata is combined from
        any extra information provided and the result of a metadata function, if defined.

        Args:
            data (Dict): The main data for which metadata is being generated.
            extra_info (Dict): Additional metadata information to be included.

        Returns:
            Dict: A dictionary containing the generated metadata.
        """
        metadata = {}
        
        if extra_info:
            metadata.update(extra_info)
        if self.metadata_fn:
            metadata.update(self.metadata_fn(data))
        return metadata

    def load_data(self, input_file: str, extra_info: Optional[Dict] = {}) -> List[Document]:
        """
        Loads data from a specified input file and processes it into a list of `Document` objects. The input file
        should be in JSON format, and the function supports processing either a single JSON object or a list of JSON
        objects.

        The function optionally allows additional metadata to be added to each document via `extra_info` and uses
        an optional callable function `text_fn` to extract text content from each JSON object if provided. If `text_fn`
        is not provided, the function converts the JSON data into a string.

        Args:
            input_file: The path to the JSON file containing the data to be loaded.
            extra_info: Optional dictionary of additional metadata to include
                in each `Document` object.

        Returns:
            A list of `Document` objects, where each object contains text content and associated metadata.
        """

        filename = os.path.basename(input_file)
        if 'filename' not in extra_info:
            extra_info['filename'] = filename

        with open(input_file, encoding='utf-8') as f:

            if self.is_jsonl:
                json_data = []
            
                s = f.readline()
                while s:
                    json_data.append(json.loads(s))
                    s = f.readline()
            else:
                json_data = json.load(f)
                
                if not isinstance(json_data, list):
                    json_data = [json_data]

            documents = []

            for data in json_data:
                if self.text_fn:
                    text = self.text_fn(data)
                    metadata = self._get_metadata(data, extra_info)
                    documents.append(Document(text=text, metadata=metadata))
                else:
                    json_output = json.dumps(data, ensure_ascii=self.ensure_ascii)
                    documents.append(Document(text=json_output, metadata=self._get_metadata(data, extra_info)))
            
            return documents