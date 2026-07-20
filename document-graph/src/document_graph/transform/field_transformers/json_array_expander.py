# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Array Expander transformer for cybersecurity data processing.

This module provides a robust transformer specifically designed for handling
complex JSON arrays commonly found in cybersecurity vendor data. It expands
JSON arrays into multiple records while handling various edge cases like
empty arrays, duplicate fields, nested structures, and malformed data.

This transformer is essential for normalizing vendor security data that often
contains nested evidence, alerts, indicators, and other security artifacts
stored as JSON arrays within CSV fields.
"""

import logging

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

import json
import pandas as pd
from typing import List, Dict, Any, Union
from document_graph.transform.transformer_provider_base import TransformerProvider


class JSONArrayExpanderProvider(TransformerProvider):
    """Expands JSON arrays in fields into multiple records with advanced conflict resolution.
    
    This transformer is specifically designed for cybersecurity data where vendors often
    store complex nested data structures as JSON arrays. It handles common issues like:
    - Empty arrays that should be skipped
    - Duplicate field names between original record and JSON data
    - Nested objects within arrays
    - Malformed JSON data
    - Field name conflicts and collision resolution
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
            - field: The field containing the JSON array data (required)
            - skip_empty_arrays: Skip records with empty arrays [] (default: True)
            - conflict_resolution: How to handle field name conflicts (default: "prefix")
                - "prefix": Add prefix to JSON fields (json_fieldname)
                - "suffix": Add suffix to JSON fields (fieldname_json)  
                - "overwrite": JSON fields overwrite original fields
                - "skip": Skip JSON fields that conflict with original
            - prefix: Prefix for JSON fields when using prefix resolution (default: "json_")
            - suffix: Suffix for JSON fields when using suffix resolution (default: "_json")
            - flatten_nested: Flatten nested objects using dot notation (default: True)
            - max_depth: Maximum nesting depth to flatten (default: 3)
            - preserve_original_field: Keep the original JSON field in output (default: False)
            - add_array_index: Add array index as a field (default: True)
            - array_index_field: Field name for array index (default: "array_index")
            - log_unmapped_fields: Log unmapped fields to file (default: False)
            - log_file_path: Path to log file for unmapped fields (default: "unmapped_fields.json")
            - parent_node: Parent node name for unmapped field suggestions (required if log_unmapped_fields=True)
            
    Examples:
        >>> # Basic array expansion for security evidence
        >>> config = TransformerProviderConfig(
        ...     name="evidence_expander",
        ...     type="json_array_expander",
        ...     args={
        ...         "field": "Evidence",
        ...         "skip_empty_arrays": True,
        ...         "conflict_resolution": "prefix"
        ...     }
        ... )
        >>> transformer = JSONArrayExpanderProvider(config)
        >>> result = transformer.transform([
        ...     {"id": 1, "Evidence": '[{"type": "FILE", "path": "/etc/passwd"}]'}
        ... ])
        >>> result[0]
        {'id': 1, 'Evidence': '[{"type": "FILE", "path": "/etc/passwd"}]', 
         'json_type': 'FILE', 'json_path': '/etc/passwd', 'array_index': 0}
    """
    
    def __init__(self, config):
        """Initialize the JSON array expander with configuration.
        
        Args:
            config: Transformer configuration with name, type, and args
        """
        super().__init__(config)
        self.field = self.args.get('field')
        if not self.field:
            raise ValueError("JSON Array Expander requires 'field' argument")
        
        self.skip_empty_arrays = self.args.get('skip_empty_arrays', True)
        self.conflict_resolution = self.args.get('conflict_resolution', 'prefix')
        self.prefix = self.args.get('prefix', 'json_')
        self.suffix = self.args.get('suffix', '_json')
        self.flatten_nested = self.args.get('flatten_nested', True)
        self.max_depth = self.args.get('max_depth', 3)
        self.preserve_original_field = self.args.get('preserve_original_field', False)
        self.add_array_index = self.args.get('add_array_index', True)
        self.array_index_field = self.args.get('array_index_field', 'array_index')
        self.log_unmapped_fields = self.args.get('log_unmapped_fields', False)
        self.log_file_path = self.args.get('log_file_path', 'unmapped_fields.json')
        self.parent_node = self.args.get('parent_node')
        self.auto_add_discovered = self.args.get('auto_add_discovered', False)
        self.preserve_original_field = self.args.get('preserve_original_field', False)
        self.unmapped_fields = set()  # Track unique unmapped fields

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by expanding JSON arrays into multiple records.
        
        Args:
            records: List of record dictionaries to transform
            
        Returns:
            List of transformed record dictionaries with expanded JSON array data
            
        Note:
            Each input record may generate multiple output records, one for each
            item in the JSON array. Empty arrays are handled according to the
            skip_empty_arrays configuration.
        """
        expanded_records = []
        
        for record in records:
            try:
                json_data = self._extract_json_data(record)
                
                if json_data is None:
                    # No JSON data found, set field to NaN and keep original record
                    record_copy = record.copy()
                    record_copy[self.field] = pd.NA
                    expanded_records.append(record_copy)
                    continue
                
                if isinstance(json_data, list):
                    if len(json_data) == 0 and self.skip_empty_arrays:
                        # Skip empty arrays
                        logger.debug(f"Skipping record with empty array in field '{self.field}'")
                        continue
                    
                    # Expand array into multiple records
                    for index, item in enumerate(json_data):
                        new_record = self._create_expanded_record(record, item, index)
                        expanded_records.append(new_record)
                        
                else:
                    # Single object, treat as array with one item
                    new_record = self._create_expanded_record(record, json_data, 0)
                    expanded_records.append(new_record)
                    
            except Exception as e:
                logger.warning(f"Failed to process JSON in field '{self.field}': {e}")
                # Set field to NaN on error and keep original record
                record_copy = record.copy()
                record_copy[self.field] = pd.NA
                expanded_records.append(record_copy)
        
        return expanded_records
    
    def _extract_json_data(self, record: Dict[str, Any]) -> Union[List, Dict, None]:
        """Extract and parse JSON data from the specified field."""
        field_value = record.get(self.field)
        
        if field_value is None or field_value == '':
            return None
        
        if isinstance(field_value, str):
            # Handle empty strings
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
    
    def _create_expanded_record(self, original_record: Dict[str, Any], 
                              json_item: Any, index: int) -> Dict[str, Any]:
        """Create a new record by merging original record with JSON item data."""
        # Start with original record
        if self.preserve_original_field:
            new_record = original_record.copy()
        else:
            new_record = {k: v for k, v in original_record.items() if k != self.field}
        
        # Add array index if requested
        if self.add_array_index:
            new_record[self.array_index_field] = index
        
        # Process JSON item
        if isinstance(json_item, dict):
            flattened_data = self._flatten_json_object(json_item)
            resolved_data = self._resolve_field_conflicts(new_record, flattened_data)
            
            # Auto-add discovered fields or log unmapped fields
            if self.auto_add_discovered:
                # Add all discovered fields to the record
                new_record.update(resolved_data)
                # Log what was auto-mapped if requested
                if self.log_unmapped_fields and self.parent_node:
                    self._log_unmapped_fields(flattened_data)
            else:
                # Only log unmapped fields if requested
                if self.log_unmapped_fields and self.parent_node:
                    self._log_unmapped_fields(flattened_data)
        else:
            # Handle primitive values in array
            if self.auto_add_discovered:
                field_name = self._resolve_field_name('value', new_record)
                new_record[field_name] = json_item
            elif self.log_unmapped_fields:
                # Log that a primitive value was found but not mapped
                logger.info(f"Found primitive value in array but auto_add_discovered=False: {json_item}")
        
        return new_record
    
    def _flatten_json_object(self, json_obj: Dict[str, Any], 
                           parent_key: str = '', depth: int = 0) -> Dict[str, Any]:
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
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                # Handle arrays of objects by taking first item or concatenating
                if len(value) == 1:
                    nested_flattened = self._flatten_json_object(value[0], new_key, depth + 1)
                    flattened.update(nested_flattened)
                else:
                    # Multiple items - convert to string representation
                    flattened[new_key] = str(value)
            else:
                flattened[new_key] = value
        
        return flattened
    
    def _resolve_field_conflicts(self, original_record: Dict[str, Any], 
                                json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve field name conflicts between original record and JSON data."""
        resolved_data = {}
        
        for key, value in json_data.items():
            resolved_key = self._resolve_field_name(key, original_record)
            resolved_data[resolved_key] = value
        
        return resolved_data
    
    def _resolve_field_name(self, json_field: str, original_record: Dict[str, Any]) -> str:
        """Resolve a single field name conflict."""
        # Force prefix when auto_add_discovered is enabled
        if self.auto_add_discovered and self.conflict_resolution == 'prefix':
            return f"{self.prefix}{json_field}"
        
        if json_field not in original_record:
            return json_field
        
        # Field conflict detected
        if self.conflict_resolution == 'prefix':
            return f"{self.prefix}{json_field}"
        elif self.conflict_resolution == 'suffix':
            return f"{json_field}{self.suffix}"
        elif self.conflict_resolution == 'overwrite':
            return json_field
        elif self.conflict_resolution == 'skip':
            # Return None to indicate this field should be skipped
            return None
        else:
            # Default to prefix
            return f"{self.prefix}{json_field}"
    
    def _log_unmapped_fields(self, flattened_data: Dict[str, Any]) -> None:
        """Log unmapped fields for transform_and_load.json generation."""
        for field_name in flattened_data.keys():
            if field_name not in self.unmapped_fields:
                self.unmapped_fields.add(field_name)
                
                # Create mapping suggestion
                mapping_suggestion = {
                    "csv_property_name": field_name,
                    "type": "property",
                    "arrow_property_name": field_name.lower().replace(' ', '_').replace('-', '_'),
                    "parents": [self.parent_node]
                }
                
                # Append to log file
                try:
                    import os
                    # Create directory if it doesn't exist
                    log_dir = os.path.dirname(self.log_file_path)
                    if log_dir and not os.path.exists(log_dir):
                        os.makedirs(log_dir, exist_ok=True)
                    
                    existing_data = []
                    if os.path.exists(self.log_file_path):
                        try:
                            with open(self.log_file_path, 'r') as f:
                                existing_data = json.load(f)
                        except json.JSONDecodeError:
                            # File is corrupted, start fresh
                            logger.warning(f"Corrupted log file {self.log_file_path}, recreating")
                            existing_data = []
                    
                    existing_data.append(mapping_suggestion)
                    
                    with open(self.log_file_path, 'w') as f:
                        json.dump(existing_data, f, indent=2)
                        
                    if hasattr(self, 'auto_add_discovered') and self.auto_add_discovered:
                        logger.debug(f"Auto-mapped field '{field_name}' to {self.parent_node} node")
                    else:
                        logger.debug(f"Logged unmapped field '{field_name}' to {self.log_file_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to log unmapped field '{field_name}': {e}")
    
    def __del__(self):
        """Cleanup: Log summary of discovered fields."""
        if self.log_unmapped_fields and self.unmapped_fields:
            if hasattr(self, 'auto_add_discovered') and self.auto_add_discovered:
                logger.debug(f"Auto-mapped {len(self.unmapped_fields)} discovered fields in '{self.field}' to {self.parent_node} nodes: {sorted(self.unmapped_fields)}")
            else:
                logger.debug(f"Found {len(self.unmapped_fields)} unmapped fields in '{self.field}': {sorted(self.unmapped_fields)}")