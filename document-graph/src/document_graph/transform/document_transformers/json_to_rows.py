# Copyright (c) Evan Erwee. All rights reserved.

"""JSON to Rows transformer for flattening nested JSON structures.

This module provides functionality to transform nested JSON data into a flat
tabular structure, making it easier to process and analyze hierarchical data
in a row-based format. It uses pandas json_normalize to flatten nested JSON
structures into rows with dot-notation column names.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
import json
import pandas as pd
from document_graph.transform.transformer_provider_base import TransformerProvider

class JSONToRowsTransformer(TransformerProvider):
    """Transforms nested JSON fields into flat tabular rows.
    
    This transformer takes records containing nested JSON data and converts them
    into multiple flattened records, where each nested object becomes a separate row.
    The nested structure is flattened using dot notation for field names.
    
    Configuration:
        json_field (str): The field name containing the JSON data to flatten.
                         Defaults to 'content'.
    """

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records with JSON fields into flattened rows.
        
        This method processes each record by:
        1. Extracting the JSON data from the specified field
        2. Parsing the JSON if it's a string
        3. Normalizing the nested structure into a flat DataFrame
        4. Creating new records for each row in the DataFrame
        
        If JSON parsing fails, the original record is preserved.
        
        Args:
            records: A list of record dictionaries to transform
            
        Returns:
            A list of transformed records, where each nested JSON object
            has been converted to a separate record with flattened fields
            prefixed with 'json_'.
        """
        json_field = self.args.get('json_field', 'content')
        output_records = []

        for record in records:
            try:
                json_data = record.get(json_field)
                if isinstance(json_data, str):
                    json_data = json.loads(json_data)
                
                # Normalize JSON to flat structure
                df = pd.json_normalize(json_data)
                
                for idx, row in df.iterrows():
                    new_record = record.copy()
                    # Add flattened fields to record
                    for col, val in row.items():
                        new_record[f"json_{col}"] = val
                    new_record['row_index'] = idx
                    output_records.append(new_record)
                    
            except Exception:
                # Keep original record if JSON parsing fails
                output_records.append(record)

        return output_records