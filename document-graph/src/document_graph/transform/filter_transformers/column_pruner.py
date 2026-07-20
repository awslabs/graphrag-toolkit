# Copyright (c) Evan Erwee. All rights reserved.

"""Column Pruner module for document graph operations.

This module provides the ColumnPrunerProvider transformer, which removes specified
columns (fields) from records during document graph processing. This is useful for
data preparation and cleaning by allowing selective processing of fields based on
configurable criteria.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider

class ColumnPrunerProvider(TransformerProvider):
    """Transformer provider that removes specified columns from records.
    
    This transformer allows for selective field processing by removing specified
    columns (fields) from each record. It's useful for data preparation and cleaning
    when certain fields are not needed for downstream processing.
    
    Args:
        The provider expects the following arguments in the `args` dictionary:
            remove (List[str]): A list of column names to remove from each record.
                                If not provided, no columns will be removed.
    """
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove specified columns from each record.
        
        This method processes a list of records (dictionaries) and removes the
        specified columns from each record. The columns to remove are specified
        in the `remove` argument of the provider.
        
        Args:
            records (List[Dict[str, Any]]): A list of records to process, where each
                                           record is a dictionary with string keys.
        
        Returns:
            List[Dict[str, Any]]: A new list of records with the specified columns removed.
                                 The original records are not modified.
        """
        columns_to_remove: List[str] = self.args.get("remove", [])

        return [
            {k: v for k, v in record.items() if k not in columns_to_remove}
            for record in records
        ]
