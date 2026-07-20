import pandas as pd
from document_graph.ingest.ingestors_provider_base import IngestorProvider

class ColumnTypeConverterProvider(IngestorProvider):
    """
    Provides functionality to convert dataframe column types based on a specified mapping.

    This class extends the base IngestorProvider and allows for dynamic conversion of column data
    types in a pandas DataFrame. It is particularly useful when ingesting data with columns that
    need to adhere to a specific type structure for further processing or analysis.

    Methods
    -------
    ingest(data: pd.DataFrame) -> pd.DataFrame
        Converts column types in the provided DataFrame according to the 'type_mapping' argument.

    """
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        type_mapping = self.args.get("type_mapping", {})
        
        if not type_mapping:
            return data
        
        result = data.copy()
        
        for column, target_type in type_mapping.items():
            if column not in result.columns:
                continue
                
            try:
                if target_type == "string":
                    result[column] = result[column].astype(str)
                elif target_type == "int":
                    result[column] = pd.to_numeric(result[column], errors='coerce').astype('Int64')
                elif target_type == "float":
                    result[column] = pd.to_numeric(result[column], errors='coerce')
                elif target_type == "datetime":
                    result[column] = pd.to_datetime(result[column], errors='coerce')
                elif target_type == "bool":
                    result[column] = result[column].astype(bool)
                else:
                    # Direct pandas dtype
                    result[column] = result[column].astype(target_type)
            except Exception as e:
                print(f"Warning: Could not convert column '{column}' to {target_type}: {e}")
        
        return result