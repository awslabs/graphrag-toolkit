# Copyright (c) Evan Erwee. All rights reserved.

"""Comma Flattener transformer for enriching records with comma-separated field data.

This transformer flattens comma-separated values into individual columns without creating
new rows, allowing you to enrich existing records with structured data from
comma-separated fields and then map those enriched fields to different graph nodes.
"""

import logging
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)


class CommaFlattenerProvider(TransformerProvider):
    """Flattens comma-separated values into columns to enrich records.
    
    This transformer takes comma-separated values and flattens them into individual
    columns with a configurable prefix and numbering. It preserves the original row count while
    adding new columns for each comma-separated value.
    
    Args:
        field: The field containing comma-separated data (required)
        prefix: Prefix for flattened fields (default: field name + "_")
        separator: Separator character (default: ",")
        strip_whitespace: Strip whitespace from values (default: True)
        preserve_original_field: Keep original field (default: False)
        max_items: Maximum number of items to extract (default: 10)
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.field = self.args.get('field') or config.csv_property_name
        if not self.field:
            raise ValueError("Comma Flattener requires 'field' argument or csv_property_name")
        
        # Optional companion field for paired values
        self.companion_field = self.args.get('companion_field')
        self.companion_suffix = self.args.get('companion_suffix', '_name')
        
        # Generate prefix from field name if not provided
        if 'prefix' not in self.args:
            # Clean field name: remove spaces and special chars
            clean_field = ''.join(c if c.isalnum() else '' for c in self.field)
            self.prefix = clean_field
            logger.info(f"Comma Flattener using default prefix: '{self.prefix}' (from field '{self.field}')")
        else:
            self.prefix = self.args.get('prefix')
            logger.info(f"Comma Flattener using explicit prefix: '{self.prefix}'")
        
        if 'suffix' not in self.args:
            self.suffix = '#00'  # Default to 2-digit numbers: 01, 02, etc.
            logger.info(f"Comma Flattener using default suffix: '{self.suffix}' (auto-incremental numbers)")
        else:
            self.suffix = self.args.get('suffix')
            logger.info(f"Comma Flattener using explicit suffix: '{self.suffix}'")
        self.separator = self.args.get('separator', ',')
        self.strip_whitespace = self.args.get('strip_whitespace', True)
        self.preserve_original_field = self.args.get('preserve_original_field', False)
        self.max_items = self.args.get('max_items', 10)
        self.unique_nodes = self.args.get('unique_nodes', False)
        self._unique_values = set()  # Track unique values globally

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by flattening comma-separated values into columns."""
        
        # If unique_nodes is enabled, collect all unique values first
        if self.unique_nodes:
            self._collect_unique_values(records)
            logger.info(f"Found {len(self._unique_values)} unique values for field '{self.field}'")
        
        enriched_records = []
        
        for record in records:
            try:
                enriched_record = self._enrich_record(record)
                enriched_records.append(enriched_record)
            except Exception as e:
                logger.warning(f"Failed to process comma-separated field '{self.field}': {e}")
                enriched_records.append(record.copy())
        
        return enriched_records
    
    def _enrich_record(self, original_record: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich record with flattened comma-separated data."""
        # Start with original record
        if self.preserve_original_field:
            enriched_record = original_record.copy()
        else:
            enriched_record = {k: v for k, v in original_record.items() if k != self.field}
        
        field_value = original_record.get(self.field)
        
        # Handle empty/null values
        if field_value is None or field_value == '':
            return enriched_record
        
        # Convert to string if not already
        if not isinstance(field_value, str):
            field_value = str(field_value)
        
        # Split by separator
        items = field_value.split(self.separator)
        
        # Strip whitespace if requested
        if self.strip_whitespace:
            items = [item.strip() for item in items]
        
        # Filter out empty items
        items = [item for item in items if item]
        
        # Handle companion field if specified
        companion_items = []
        if self.companion_field:
            companion_value = original_record.get(self.companion_field, '')
            if companion_value:
                companion_items = companion_value.split(self.separator)
                if self.strip_whitespace:
                    companion_items = [item.strip() for item in companion_items]
                companion_items = [item for item in companion_items if item]
        
        # Add columns based on unique_nodes setting
        if self.unique_nodes:
            # Use consistent mapping for unique values
            value_mapping = self._get_unique_column_mapping()
            for item in items:
                if item in value_mapping:
                    column_name = value_mapping[item]
                    enriched_record[column_name] = item
        else:
            # Add numbered columns up to max_items (original behavior)
            for i, item in enumerate(items[:self.max_items], 1):
                if self.suffix and '#' in self.suffix:
                    # Format number according to suffix pattern (e.g., " #00" -> " 01", " 02")
                    # Split suffix into prefix part (before #) and number format (after #)
                    hash_index = self.suffix.index('#')
                    suffix_prefix = self.suffix[:hash_index]  # e.g., " " (space)
                    number_format = self.suffix[hash_index+1:]  # e.g., "00"
                    num_zeros = len(number_format)
                    formatted_num = str(i).zfill(num_zeros)
                    column_name = f"{self.prefix}{suffix_prefix}{formatted_num}"
                elif self.suffix:
                    column_name = f"{self.prefix}{self.suffix}{i}"
                else:
                    column_name = f"{self.prefix}{i}"
                enriched_record[column_name] = item
                
                # Add companion field if available
                if self.companion_field and i <= len(companion_items):
                    companion_column = column_name + self.companion_suffix
                    enriched_record[companion_column] = companion_items[i-1]
        
        return enriched_record
    
    def _collect_unique_values(self, records: List[Dict[str, Any]]) -> None:
        """Collect all unique values across all records."""
        for record in records:
            field_value = record.get(self.field)
            if field_value is None or field_value == '':
                continue
            
            if not isinstance(field_value, str):
                field_value = str(field_value)
            
            items = field_value.split(self.separator)
            if self.strip_whitespace:
                items = [item.strip() for item in items]
            
            items = [item for item in items if item]
            self._unique_values.update(items)
    
    def _get_unique_column_mapping(self) -> Dict[str, str]:
        """Get mapping of unique values to column names."""
        mapping = {}
        unique_list = sorted(list(self._unique_values))[:self.max_items]
        
        for i, value in enumerate(unique_list, 1):
            if self.suffix and '#' in self.suffix:
                hash_index = self.suffix.index('#')
                suffix_prefix = self.suffix[:hash_index]
                number_format = self.suffix[hash_index+1:]
                num_zeros = len(number_format)
                formatted_num = str(i).zfill(num_zeros)
                column_name = f"{self.prefix}{suffix_prefix}{formatted_num}"
            elif self.suffix:
                column_name = f"{self.prefix}{self.suffix}{i}"
            else:
                column_name = f"{self.prefix}{i}"
            mapping[value] = column_name
        
        return mapping