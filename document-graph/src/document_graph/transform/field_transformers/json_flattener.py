# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Flattener transformer for enriching records with JSON field data.

This transformer flattens JSON objects into individual columns without creating
new rows, allowing you to enrich existing records with structured data from
JSON blobs and then map those enriched fields to different graph nodes.
"""

import logging
import json
import pandas as pd
from typing import List, Dict, Any, Union
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)


class JSONFlattenerProvider(TransformerProvider):
    """Flattens JSON objects into columns to enrich records.
    
    This transformer takes JSON objects/arrays and flattens them into individual
    columns with a configurable prefix. It preserves the original row count while
    adding new columns for each JSON field.
    
    Args:
        field: The field containing JSON data (required)
        prefix: Prefix for flattened fields (default: field name without spaces)
        flatten_nested: Flatten nested objects with dot notation (default: True)
        max_depth: Maximum nesting depth (default: 3)
        preserve_original_field: Keep original JSON field (default: False)
        handle_arrays: How to handle arrays - "first", "join", "string" (default: "first")
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.field = self.args.get('field') or getattr(config, 'csv_property_name', None)
        if not self.field:
            raise ValueError("JSON Flattener requires 'field' argument or csv_property_name")
        
        # Use field name as default prefix (no underscore separator)
        if 'prefix' not in self.args:
            # Clean field name: remove spaces and special chars
            clean_field = ''.join(c if c.isalnum() else '' for c in self.field)
            self.prefix = clean_field
            logger.info(f"JSON Flattener using default prefix: '{self.prefix}' (from field '{self.field}')")
        else:
            self.prefix = self.args.get('prefix')
            logger.info(f"JSON Flattener using explicit prefix: '{self.prefix}'")
        self.flatten_nested = self.args.get('flatten_nested', True)
        self.max_depth = self.args.get('max_depth', 3)
        self.preserve_original_field = self.args.get('preserve_original_field', False)
        self.handle_arrays = self.args.get('handle_arrays', 'first')  # first, join, string

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by flattening JSON into columns."""
        enriched_records = []
        
        for record in records:
            try:
                json_data = self._extract_json_data(record)
                enriched_record = self._enrich_record(record, json_data)
                enriched_records.append(enriched_record)
            except Exception as e:
                logger.warning(f"Failed to process JSON in field '{self.field}': {e}")
                enriched_records.append(record.copy())
        
        return enriched_records
    
    def _extract_json_data(self, record: Dict[str, Any]) -> Union[Dict, List, None]:
        """Extract and parse JSON data from the specified field."""
        field_value = record.get(self.field)
        
        if field_value is None or field_value == '':
            return None
        
        if isinstance(field_value, str):
            if field_value.strip() == '':
                return None
            try:
                return json.loads(field_value)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in field '{self.field}': {e}")
                return None
        elif isinstance(field_value, (list, dict)):
            return field_value
        else:
            logger.warning(f"Unsupported data type in field '{self.field}': {type(field_value)}")
            return None
    
    def _enrich_record(self, original_record: Dict[str, Any], json_data: Any) -> Dict[str, Any]:
        """Enrich record with flattened JSON data."""
        # Start with original record
        if self.preserve_original_field:
            enriched_record = original_record.copy()
        else:
            enriched_record = {k: v for k, v in original_record.items() if k != self.field}
        
        if json_data is None:
            return enriched_record
        
        # Handle arrays by taking first item or converting to string
        if isinstance(json_data, list):
            if len(json_data) == 0:
                return enriched_record
            elif self.handle_arrays == 'first':
                json_data = json_data[0]  # Take first item
            elif self.handle_arrays == 'string':
                # Convert entire array to string
                flattened_data = {f"{self.prefix}array": str(json_data)}
                enriched_record.update(flattened_data)
                return enriched_record
            elif self.handle_arrays == 'numbered':
                # Create numbered columns like comma_flattener
                for i, item in enumerate(json_data, 1):
                    if i > 10:  # Limit to 10 items
                        break
                    formatted_num = str(i).zfill(2)
                    column_name = f"{self.prefix} {formatted_num}"
                    if isinstance(item, dict):
                        enriched_record[column_name] = json.dumps(item)
                    else:
                        enriched_record[column_name] = str(item)
                return enriched_record
            if self.handle_arrays == 'join' and all(isinstance(x, str) for x in json_data):
                # Join string arrays
                flattened_data = {f"{self.prefix}array": ', '.join(json_data)}
                enriched_record.update(flattened_data)
                return enriched_record
            else:
                json_data = json_data[0]  # Fallback to first
        
        # Flatten the JSON object
        if isinstance(json_data, dict):
            flattened_data = self._flatten_json_object(json_data)
            # Add prefix to all keys
            prefixed_data = {f"{self.prefix}{key}": value for key, value in flattened_data.items()}
            enriched_record.update(prefixed_data)
        else:
            # Handle primitive values
            enriched_record[f"{self.prefix}value"] = json_data
        
        return enriched_record
    
    def _flatten_json_object(self, json_obj: Dict[str, Any], parent_key: str = '', depth: int = 0) -> Dict[str, Any]:
        """Flatten nested JSON object using dot notation."""
        if not self.flatten_nested or depth >= self.max_depth:
            return json_obj
        
        flattened = {}
        
        for key, value in json_obj.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            
            if isinstance(value, dict) and depth < self.max_depth:
                # Recursively flatten nested objects
                nested_flattened = self._flatten_json_object(value, new_key, depth + 1)
                flattened.update(nested_flattened)
            elif isinstance(value, list):
                # Handle arrays based on configuration
                if self.handle_arrays == 'first' and len(value) > 0:
                    if isinstance(value[0], dict):
                        nested_flattened = self._flatten_json_object(value[0], new_key, depth + 1)
                        flattened.update(nested_flattened)
                    else:
                        flattened[new_key] = value[0]
                elif self.handle_arrays == 'join' and all(isinstance(x, str) for x in value):
                    flattened[new_key] = ', '.join(value)
                else:
                    flattened[new_key] = str(value)
            else:
                flattened[new_key] = value
        
        return flattened