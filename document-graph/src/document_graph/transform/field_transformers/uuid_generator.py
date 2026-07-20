# Copyright (c) Evan Erwee. All rights reserved.
"""UUID generator — generates UUID values for specified fields."""

import uuid
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider
from document_graph.transform.transformer_provider_config import TransformerProviderConfig


class UuidGeneratorTransformer(TransformerProvider):
    """Generates UUID values for specified fields"""
    
    def __init__(self, config: TransformerProviderConfig):
        super().__init__(config)
        self.target_field = config.args.get('target_field', 'uuid')
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self._log_transform_start(len(records))
        for record in records:
            record[self.target_field] = str(uuid.uuid4())
        self._log_transform_end(len(records), len(records))
        return records