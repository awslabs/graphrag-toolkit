"""Ingestors error handler — error handling and validation utilities for ingestion."""
import logging
import pandas as pd
from typing import List, Dict, Any

# Use document-graph logging
logger = logging.getLogger(__name__)

class IngestorErrorHandler:
    """
    Provides error handling and validation utilities for ingestor processes.

    IngestorErrorHandler contains static methods that validate the presence of required
    columns in a DataFrame, ensure configuration fields are complete, and handle failures
    by rolling back to a safe state. This class is designed to support robust error
    management in data ingestion pipelines, promoting reliability and easier debugging.
    """
    
    @staticmethod
    def validate_column_exists(df: pd.DataFrame, column: str, ingestor_name: str):
        """
        Validates the existence of a specified column in a DataFrame for the given ingestor.

        If the specified column does not exist in the DataFrame, an error message will be
        logged and a ValueError will be raised.

        Parameters:
        df (pd.DataFrame): The input DataFrame in which to check the availability of the column.
        column (str): The name of the column to verify existence.
        ingestor_name (str): The name of the ingestor to include in error logging.

        Raises:
        ValueError: If the specified column is not found in the DataFrame's columns.
        """
        if column not in df.columns:
            error_msg = f"{ingestor_name}: Column '{column}' not found. Available columns: {list(df.columns)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    @staticmethod
    def validate_columns_exist(df: pd.DataFrame, columns: List[str], ingestor_name: str):
        """
        Validates if the specified columns exist in the provided DataFrame. This method checks
        for missing columns in the DataFrame compared to the list of required columns. If any
        columns are missing, an error is logged and an exception is raised.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            columns (List[str]): A list of column names to check for existence in the DataFrame.
            ingestor_name (str): The name of the ingestor for logging purposes.

        Raises:
            ValueError: If any of the specified columns are missing in the DataFrame.
        """
        if missing := [col for col in columns if col not in df.columns]:
            error_msg = f"{ingestor_name}: Columns not found: {missing}. Available: {list(df.columns)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    @staticmethod
    def handle_ingestor_failure(ingestor_name: str, error: Exception, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handles the failure of an ingestor operation and reverts to the original DataFrame.

        The method logs an error and a warning message whenever an ingestor fails. It ensures
        that in case of any error during ingestion, the original DataFrame is retained and
        returned unchanged.

        Parameters:
            ingestor_name: str
                The name of the ingestor that encountered a failure.
            error: Exception
                The exception instance describing the failure.
            df: pd.DataFrame
                The original DataFrame prior to the failed ingestion attempt.

        Returns:
            pd.DataFrame
                The original DataFrame unchanged, ensuring data consistency.
        """
        logger.error(f"{ingestor_name} failed: {error}")
        logger.warning(f"Rolling back to original DataFrame with {len(df)} rows")
        return df
    
    @staticmethod
    def validate_config(config_dict: Dict[str, Any], required_fields: List[str], ingestor_name: str):
        """
        Validates a configuration dictionary against a list of required fields.

        This method checks that all required fields are present in the provided
        configuration dictionary. If any required fields are missing, an error
        is logged, and a ValueError is raised.

        Parameters:
        config_dict : Dict[str, Any]
            The configuration dictionary to validate.
        required_fields : List[str]
            A list of field names required to be in the configuration dictionary.
        ingestor_name : str
            The name of the ingestor, included in error messages for better
            context.

        Raises:
        ValueError
            Raised when one or more required fields are missing from the
            configuration dictionary.
        """
        missing = [field for field in required_fields if field not in config_dict]
        if missing:
            error_msg = f"{ingestor_name}: Missing required config fields: {missing}"
            logger.error(error_msg)
            raise ValueError(error_msg)