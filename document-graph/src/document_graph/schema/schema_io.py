# Copyright (c) Evan Erwee. All rights reserved.

"""Schema IO Module for Document Graph Operations.

This module provides utility functions for saving and loading ETL schemas to and from
various storage formats. It handles serialization and deserialization of ETL schema
objects, making it easy to persist schemas and share them between different components
of the document graph system.

The module includes functions for:
- Saving ETL schemas to JSON files or file-like objects
- Loading ETL schemas from JSON files or file-like objects

These functions handle the conversion between ETLSchema objects and their JSON
representations, ensuring that all schema components are properly serialized and
deserialized.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


import json
from pathlib import Path
from typing import Union, IO
from document_graph.schema.etl_schema_model import ETLSchema

def save_schema(schema: ETLSchema, output: Union[str, Path, IO], indent: int = 2) -> None:
    """
    Save an ETL schema to a file or file-like object in JSON format.
    
    This function serializes an ETLSchema object to JSON and writes it to the specified
    output. The output can be a file path (as a string or Path object) or a file-like
    object that supports the write method.
    
    Args:
        schema (ETLSchema): The ETL schema to save.
        output (Union[str, Path, IO]): The output destination, which can be a file path
                                      (as a string or Path object) or a file-like object.
        indent (int, optional): The indentation level for the JSON output. Defaults to 2.
    
    Raises:
        IOError: If there's an error writing to the output.
    
    Example:
        ```python
        from document_graph.schema.etl_schema_model import ETLSchema
        from document_graph.schema.schema_io import save_schema
        
        # Create a schema
        schema = ETLSchema(...)
        
        # Save to a file
        save_schema(schema, "path/to/schema.json")
        
        # Or save to a file-like object
        with open("path/to/schema.json", "w") as f:
            save_schema(schema, f)
        ```
    """
    try:
        if isinstance(output, (str, Path)):
            with Path(output).open("w", encoding="utf-8") as f:
                json.dump(schema.model_dump(mode="json", exclude_unset=True), f, indent=indent)
        else:
            json.dump(schema.model_dump(mode="json", exclude_unset=True), output, indent=indent)
    except Exception as e:
        raise IOError(f"IO error: {str(output), e}")


def load_schema(input_: Union[str, Path, IO]) -> ETLSchema:
    """
    Load an ETL schema from a file or file-like object.
    
    This function deserializes an ETLSchema object from JSON read from the specified
    input. The input can be a file path (as a string or Path object) or a file-like
    object that supports the read method.
    
    Args:
        input_ (Union[str, Path, IO]): The input source, which can be a file path
                                      (as a string or Path object) or a file-like object.
    
    Returns:
        ETLSchema: The deserialized ETL schema.
    
    Raises:
        IOError: If there's an error reading from the input.
        ValueError: If the input contains invalid JSON or the JSON doesn't represent a valid ETLSchema.
    
    Example:
        ```python
        from document_graph.schema.schema_io import pipeline_schema
        
        # Load from a file
        schema = pipeline_schema("path/to/schema.json")
        
        # Or load from a file-like object
        with open("path/to/schema.json", "r") as f:
            schema = pipeline_schema(f)
        ```
    """
    try:
        if isinstance(input_, (str, Path)):
            with Path(input_).open("r", encoding="utf-8") as f:
                return ETLSchema(**json.load(f))
        else:
            return ETLSchema(**json.load(input_))
    except Exception as e:
        raise IOError(f"IO error: {str(input_), e}")
