# Copyright (c) Evan Erwee. All rights reserved.

"""Paired Flattener transformer for flattening two related comma-separated fields."""

import logging
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)


class PairedFlattenerProvider(TransformerProvider):
    """Flattens two related comma-separated fields into paired columns.
    
    Creates columns like: project_01_id, project_01_name, project_02_id, project_02_name
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.id_field = self.args.get('id_field')
        self.name_field = self.args.get('name_field')
        self.prefix = self.args.get('prefix', 'project')
        self.suffix = self.args.get('suffix', '#00')
        self.separator = self.args.get('separator', ',')
        self.max_items = self.args.get('max_items', 10)
        
        if not self.id_field or not self.name_field:
            raise ValueError("PairedFlattener requires both 'id_field' and 'name_field'")

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched_records = []
        
        for record in records:
            enriched_record = record.copy()
            
            ids = self._split_field(record.get(self.id_field, ''))
            names = self._split_field(record.get(self.name_field, ''))
            
            # Pair up IDs and names (pad shorter list with empty strings)
            max_len = min(max(len(ids), len(names)), self.max_items)
            ids.extend([''] * (max_len - len(ids)))
            names.extend([''] * (max_len - len(names)))
            
            for i in range(max_len):
                num = str(i + 1).zfill(2) if '#00' in self.suffix else str(i + 1)
                enriched_record[f"{self.prefix}_{num}_id"] = ids[i]
                enriched_record[f"{self.prefix}_{num}_name"] = names[i]
            
            enriched_records.append(enriched_record)
        
        return enriched_records
    
    def _split_field(self, value: str) -> List[str]:
        if not value:
            return []
        return [item.strip() for item in str(value).split(self.separator) if item.strip()]