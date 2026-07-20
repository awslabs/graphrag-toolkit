# Copyright (c) Evan Erwee. All rights reserved.

"""Row Filter module for document graph operations.

This module provides the RowFilterProvider transformer, which filters records based on
field conditions during document graph processing. It supports various comparison
operators such as equality, inequality, comparison, and null checks.

This transformer is useful for data preparation and cleaning by allowing selective
processing of records based on configurable criteria.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider


class RowFilterProvider(TransformerProvider):
    """Transformer provider that filters records based on field conditions.
    
    This transformer allows for selective record processing by filtering records
    based on field conditions. It supports various comparison operators such as
    equality, inequality, comparison, and null checks. Records are kept only if
    they satisfy all the specified conditions.
    
    Args:
        The provider expects the following arguments in the `args` dictionary:
            conditions (List[Dict[str, Any]]): A list of condition dictionaries.
                Each condition dictionary must have the following keys:
                - field (str): The name of the field to check.
                - operator (str): The comparison operator to use. Supported operators are:
                    - "eq": Equal to
                    - "ne": Not equal to
                    - "lt": Less than
                    - "gt": Greater than
                    - "null": Field is null
                    - "not_null": Field is not null
                - value (Any): The value to compare against. Required for "eq", "ne",
                  "lt", and "gt" operators. Not used for "null" and "not_null" operators.
    
    Raises:
        ValueError: If an unsupported operator is specified.
    """
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter records based on field conditions.
        
        This method processes a list of records (dictionaries) and returns only the
        records that satisfy all the specified conditions. The conditions are specified
        in the `conditions` argument of the provider.
        
        If no conditions are specified, all records are returned unchanged.
        
        Args:
            records (List[Dict[str, Any]]): A list of records to process, where each
                                           record is a dictionary with string keys.
        
        Returns:
            List[Dict[str, Any]]: A new list containing only the records that satisfy
                                 all the specified conditions.
        
        Raises:
            ValueError: If an unsupported operator is specified in a condition.
        """
        conditions: List[Dict[str, Any]] = self.args.get("conditions", [])
        if not conditions:
            return records

        filtered_records = []
        for record in records:
            keep_record = True
            
            for cond in conditions:
                field = cond["field"]
                op = cond["operator"]
                value = cond.get("value")
                field_value = record.get(field)

                if op == "eq" and field_value != value:
                    keep_record = False
                elif op == "ne" and field_value == value:
                    keep_record = False
                elif op == "lt" and not (field_value is not None and field_value < value):
                    keep_record = False
                elif op == "gt" and not (field_value is not None and field_value > value):
                    keep_record = False
                elif op == "null" and field_value is not None:
                    keep_record = False
                elif op == "not_null" and field_value is None:
                    keep_record = False
                else:
                    if op not in ["eq", "ne", "lt", "gt", "null", "not_null"]:
                        raise ValueError(f"Unsupported operator: {op}")
                
                if not keep_record:
                    break
            
            if keep_record:
                filtered_records.append(record)

        return filtered_records