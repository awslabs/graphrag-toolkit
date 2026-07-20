import pandas as pd
from document_graph.ingest.ingestors_provider_base import IngestorProvider

class ColumnReorderProvider(IngestorProvider):
    """
    Provides functionality to reorder the columns of a given pandas DataFrame
    based on a specified column order.

    The class is designed to take a pandas DataFrame and rearrange its columns
    based on a predefined order specified in the `args` attribute. If no column
    order is specified, the original DataFrame is returned unchanged. The primary
    purpose of the class is to modify column arrangements while preserving columns
    that are not explicitly mentioned in the provided order. This ensures all
    columns in the DataFrame are retained, even if not reordered.

    Methods
    -------
    ingest(data: pd.DataFrame) -> pd.DataFrame
        Reorders the columns of the provided DataFrame as per the specified
        column order or retains the original order if no preference is provided.
    """
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Reorders the columns of a DataFrame based on the specified order provided in the arguments.

        If a specific column order is provided in the arguments (`column_order`), the method will
        rearrange the DataFrame's columns to match the specified order. Columns not present in the
        `column_order` list will retain their positions and be appended after the ordered columns.

        Parameters:
            data (pd.DataFrame): The DataFrame to be ingested and reordered. The input DataFrame
            should contain columns that may partially or fully match the `column_order` list in
            the instance's arguments.

        Returns:
            pd.DataFrame: A DataFrame with columns reordered based on the `column_order` provided
            in the arguments. If `column_order` is not specified or empty, the original DataFrame
            is returned without any modifications.
        """
        column_order = self.args.get("column_order", [])
        
        if not column_order:
            return data
        
        # Keep existing columns that aren't in the order list
        existing_columns = [col for col in column_order if col in data.columns]
        remaining_columns = [col for col in data.columns if col not in column_order]
        
        # Combine ordered columns with remaining columns
        new_order = existing_columns + remaining_columns
        
        return data[new_order]
