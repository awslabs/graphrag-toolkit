# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Value Flattener for creating JSON objects from paired fields."""

import json
import logging
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)


class JSONValueFlattenerProvider(TransformerProvider):
    """Creates JSON objects from paired comma-separated fields.
    
    Creates columns like: project_01: {"id": "123", "name": "Project A"}
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.id_field = self.args.get('id_field')
        self.name_field = self.args.get('name_field')
        self.prefix = self.args.get('prefix', 'project')
        self.suffix = self.args.get('suffix', '#00')
        self.max_items = self.args.get('max_items', 10)
        
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched_records = []
        
        for record in records:
            enriched_record = record.copy()
            
            ids = self._split_field(record.get(self.id_field, ''))
            names = self._split_field(record.get(self.name_field, ''))
            
            max_len = min(max(len(ids), len(names)), self.max_items)
            
            for i in range(max_len):
                num = str(i + 1).zfill(2)
                project_data = {
                    "id": ids[i] if i < len(ids) else "",
                    "name": names[i] if i < len(names) else ""
                }
                enriched_record[f"{self.prefix}_{num}"] = json.dumps(project_data)
            
            enriched_records.append(enriched_record)
        
        return enriched_records
    
    def _split_field(self, value: str) -> List[str]:
        if not value:
            return []
        return [item.strip() for item in str(value).split(',') if item.strip()]