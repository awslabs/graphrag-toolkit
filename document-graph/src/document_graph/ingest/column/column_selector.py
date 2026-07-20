import pandas as pd
from document_graph.ingest.ingestors_provider_base import IngestorProvider

class ColumnSelectorProvider(IngestorProvider):
    """
    Provides functionality to either select specific columns or drop specific columns
    from a given pandas DataFrame.

    The ColumnSelectorProvider class processes incoming data based on the specified
    action ("select" or "drop") and a list of columns. It is useful when there is a
    need to streamline DataFrame operations by filtering or excluding certain
    columns dynamically during data ingestion.

    Methods
    -------
    ingest(data: pd.DataFrame) -> pd.DataFrame
        Processes the DataFrame according to the specified action and columns.
    """
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a DataFrame by either selecting or dropping specified columns based on
        the provided action. The method utilizes the arguments passed to dynamically
        filter columns of the DataFrame. This is useful for cleaning or transforming
        datasets by retaining or removing specific columns.

        Parameters:
        data (pd.DataFrame): The input DataFrame that needs to be processed.

        Returns:
        pd.DataFrame: The modified DataFrame with columns either selected or dropped,
        based on the specified action.

        Raises:
        ValueError: Raised when the provided action is unsupported. Supported values
        are 'select' or 'drop'.
        """
        action = self.args.get("action", "select")  # "select" or "drop"
        columns = self.args.get("columns", [])
        
        if not columns:
            return data
        
        if action == "select":
            # Keep only specified columns
            available_columns = [col for col in columns if col in data.columns]
            return data[available_columns]
        elif action == "drop":
            # Drop specified columns
            return data.drop(columns=[col for col in columns if col in data.columns])
        else:
            raise ValueError(f"Unsupported action: {action}. Use 'select' or 'drop'.")