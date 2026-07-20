"""Numeric ID cleanup ingestor — removes trailing decimals from numeric ID fields."""
import pandas as pd
from document_graph.ingest.ingestors_provider_base import IngestorProvider


class NumericIdCleanupIngestor(IngestorProvider):
    """
    Ingestor provider for cleaning up numeric IDs with specific formatting.

    This class processes a pandas DataFrame to clean up numeric ID fields,
    specifically by converting them to string format and removing any `.0`
    suffixes. It ensures the field specified in the arguments is cleaned if
    it exists in the DataFrame. Intended for use in data ingestion workflows
    where numeric IDs might be improperly formatted or need standardization.

    Methods:
        ingest: Processes and cleans a DataFrame based on the specified field.

    """
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a DataFrame to modify a specified field by converting its values to strings
        and removing ".0" suffix if present. The operation is applied only when the field
        is specified and exists in the DataFrame columns.

        Parameters:
            data (pd.DataFrame): The input DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: The processed DataFrame with the specified field modified, if applicable.
        """
        field = self.args.get("field")
        if not field or field not in data.columns:
            return data
        
        # Convert to string and remove .0 suffix
        data[field] = data[field].astype(str).str.replace(r'\.0$', '', regex=True)
        
        return data