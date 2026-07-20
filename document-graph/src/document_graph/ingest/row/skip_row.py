"""Skip row — skips DataFrame rows based on flexible conditions."""
import pandas as pd
from typing import List, Dict, Any, Union
from ..ingestors_provider_base import IngestorProvider

class SkipRowProvider(IngestorProvider):
    """Skip rows based on flexible conditions."""
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        conditions = self.args.get("conditions", [])
        if not conditions:
            return data
        
        # Start with all rows included
        mask = pd.Series([True] * len(data), index=data.index)
        
        for condition in conditions:
            field = condition["field"]
            operator = condition["operator"]
            value = condition.get("value")
            
            # Apply condition logic
            if operator == "eq":  # equals
                mask &= (data[field] == value)
            elif operator == "ne":  # not equals
                mask &= (data[field] != value)
            elif operator == "lt":  # less than
                mask &= (data[field] < value)
            elif operator == "le":  # less than or equal
                mask &= (data[field] <= value)
            elif operator == "gt":  # greater than
                mask &= (data[field] > value)
            elif operator == "ge":  # greater than or equal
                mask &= (data[field] >= value)
            elif operator == "in":  # in list
                mask &= data[field].isin(value)
            elif operator == "not_in":  # not in list
                mask &= ~data[field].isin(value)
            elif operator == "contains":  # string contains
                mask &= data[field].str.contains(str(value), na=False)
            elif operator == "not_contains":  # string doesn't contain
                mask &= ~data[field].str.contains(str(value), na=False)
            elif operator == "startswith":  # string starts with
                mask &= data[field].str.startswith(str(value), na=False)
            elif operator == "endswith":  # string ends with
                mask &= data[field].str.endswith(str(value), na=False)
            elif operator == "is_null":  # field is null/NaN
                mask &= data[field].isna()
            elif operator == "not_null":  # field is not null/NaN
                mask &= data[field].notna()
            elif operator == "is_empty":  # field is empty string
                mask &= (data[field] == "")
            elif operator == "not_empty":  # field is not empty string
                mask &= (data[field] != "")
            elif operator == "regex":  # regex match
                mask &= data[field].str.match(str(value), na=False)
            elif operator == "length_eq":  # string length equals
                mask &= (data[field].str.len() == value)
            elif operator == "length_gt":  # string length greater than
                mask &= (data[field].str.len() > value)
            elif operator == "length_lt":  # string length less than
                mask &= (data[field].str.len() < value)
            else:
                raise ValueError(f"Unsupported operator: {operator}")
        
        # Return rows that match the conditions (keep=True) or don't match (keep=False)
        keep_matching = self.args.get("keep_matching", True)
        if keep_matching:
            return data[mask]
        else:
            return data[~mask]