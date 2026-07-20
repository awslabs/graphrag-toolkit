import pandas as pd
from document_graph.ingest.ingestors_provider_base import IngestorProvider

class ColumnRenamerProvider(IngestorProvider):
    """
    A provider class for column renaming in a DataFrame.

    The ColumnRenamerProvider allows ingestion of a DataFrame with column
    renaming applied based on a provided mapping. The mapping should be
    supplied via the arguments dictionary and contains the old column names
    as keys and the new column names as values. Only columns that exist in
    the DataFrame will be renamed.

    Methods:
    ingest(data: pd.DataFrame) -> pd.DataFrame
        Renames DataFrame columns based on the provided mapping.

    """
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Processes and renames columns in the given DataFrame based on a mapping
        configuration. If a mapping is provided, only the columns that exist
        in the DataFrame and are included in the mapping will be renamed.

        Parameters:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: A new DataFrame with renamed columns according to the
            provided mapping.
        """
        mapping = self.args.get("mapping", {})
        
        if not mapping:
            return data
        
        # Only rename columns that exist
        existing_mapping = {old: new for old, new in mapping.items() if old in data.columns}
        
        return data.rename(columns=existing_mapping)