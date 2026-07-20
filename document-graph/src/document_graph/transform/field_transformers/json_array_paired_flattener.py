# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Array Paired Flattener for flattening two related JSON arrays."""

import logging
import json
from typing import List, Dict, Any, Union
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)


class JSONArrayPairedFlattenerProvider(TransformerProvider):
    """Flattens two related JSON arrays into paired columns.
    
    Handles JSON arrays like: ["id1", "id2"] and ["name1", "name2"]
    Creates columns like: project_01_id, project_01_name, project_02_id, project_02_name
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.id_field = self.args.get('id_field')
        self.name_field = self.args.get('name_field')
        self.prefix = self.args.get('prefix', 'project')
        self.max_items = self.args.get('max_items', 10)
        
        if not self.id_field or not self.name_field:
            raise ValueError("JSONArrayPairedFlattener requires both 'id_field' and 'name_field'")

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched_records = []
        
        for record in records:
            enriched_record = record.copy()
            
            ids = self._parse_json_array(record.get(self.id_field))
            names = self._parse_json_array(record.get(self.name_field))
            
            # Pair up IDs and names (pad shorter list with empty strings)
            max_len = min(max(len(ids), len(names)), self.max_items)
            ids.extend([''] * (max_len - len(ids)))
            names.extend([''] * (max_len - len(names)))
            
            for i in range(max_len):
                num = str(i + 1).zfill(2)
                enriched_record[f"{self.prefix}_{num}_id"] = ids[i]
                enriched_record[f"{self.prefix}_{num}_name"] = names[i]
            
            enriched_records.append(enriched_record)
        
        return enriched_records
    
    def _parse_json_array(self, value: Union[str, List, None]) -> List[str]:
        """Parse JSON array from string or return list directly."""
        if not value:
            return []
        
        if isinstance(value, list):
            return [str(item) for item in value]
        
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
                else:
                    return [str(parsed)]  # Single value
            except json.JSONDecodeError:
                # Fallback to comma-separated for compatibility
                return [item.strip() for item in value.split(',') if item.strip()]
        
        return [str(value)]  # Single value fallback