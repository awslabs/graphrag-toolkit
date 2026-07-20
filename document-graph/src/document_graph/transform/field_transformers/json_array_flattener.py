# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Array Flattener transformer for enriching records with JSON array data.

This transformer flattens JSON arrays into individual numbered columns without creating
new rows, similar to comma_flattener but for JSON arrays.
"""

import logging
import json
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)


class JSONArrayFlattenerProvider(TransformerProvider):
    """Flattens JSON arrays into numbered columns to enrich records.
    
    This transformer takes JSON arrays and flattens them into individual
    columns with a configurable prefix and numbering, similar to comma_flattener.
    
    Args:
        field: The field containing JSON array data (required)
        prefix: Prefix for flattened fields (default: field name)
        suffix: Suffix pattern for numbering (default: " #00")
        preserve_original_field: Keep original field (default: False)
        max_items: Maximum number of items to extract (default: 10)
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.field = self.args.get('field') or config.csv_property_name
        if not self.field:
            raise ValueError("JSON Array Flattener requires 'field' argument or csv_property_name")
        
        if 'prefix' not in self.args:
            clean_field = ''.join(c if c.isalnum() else '' for c in self.field)
            self.prefix = clean_field
        else:
            self.prefix = self.args.get('prefix')
        
        self.suffix = self.args.get('suffix', ' #00')
        self.preserve_original_field = self.args.get('preserve_original_field', False)
        self.max_items = self.args.get('max_items', 10)

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by flattening JSON arrays into numbered columns."""
        enriched_records = []
        
        for record in records:
            try:
                enriched_record = self._enrich_record(record)
                enriched_records.append(enriched_record)
            except Exception as e:
                logger.warning(f"Failed to process JSON array field '{self.field}': {e}")
                enriched_records.append(record.copy())
        
        return enriched_records
    
    def _enrich_record(self, original_record: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich record with flattened JSON array data."""
        if self.preserve_original_field:
            enriched_record = original_record.copy()
        else:
            enriched_record = {k: v for k, v in original_record.items() if k != self.field}
        
        field_value = original_record.get(self.field)
        
        if field_value is None or field_value == '':
            return enriched_record
        
        # Parse JSON if string
        if isinstance(field_value, str):
            try:
                json_data = json.loads(field_value)
            except json.JSONDecodeError:
                return enriched_record
        else:
            json_data = field_value
        
        # Handle arrays
        if isinstance(json_data, list):
            for i, item in enumerate(json_data[:self.max_items], 1):
                if self.suffix and '#' in self.suffix:
                    hash_index = self.suffix.index('#')
                    suffix_prefix = self.suffix[:hash_index]
                    number_format = self.suffix[hash_index+1:]
                    num_zeros = len(number_format)
                    formatted_num = str(i).zfill(num_zeros)
                    column_name = f"{self.prefix}{suffix_prefix}{formatted_num}"
                else:
                    column_name = f"{self.prefix}{i}"
                
                # Convert item to string representation
                enriched_record[column_name] = json.dumps(item) if isinstance(item, (dict, list)) else str(item)
        
        return enriched_record