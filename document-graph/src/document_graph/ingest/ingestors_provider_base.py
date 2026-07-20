"""Ingestors provider base — abstract base class for ingestor providers."""
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd

# Use document-graph logging
logger = logging.getLogger(__name__)

class IngestorProvider(ABC):
    """
    Abstract base class that defines the interface and behavior for an ingestor provider.

    This class is intended to be inherited and implemented by subclasses to perform
    specific ingestion tasks. It provides the basic blueprint and utility methods for
    ingesting and validating data. Subclasses must implement the `ingest` method to
    define the ingestion logic. The class also includes a utility method for input
    validation.

    Attributes:
        config: Configuration object for the ingestor, which provides necessary
        parameters and settings.

    Methods must be overridden or used directly where applicable in the defined
    workflow for ingestion.
    """
    
    def __init__(self, config):
        """
        Initializes an instance of the class with the provided configuration.

        Attributes:
        config: The configuration object associated with the instance.
        args: Parsed arguments extracted from the provided configuration.

        Args:
        config: The configuration object used to initialize the instance.
        """
        self.config = config
        self.args = config.args
        logger.debug(f"Initialized {self.__class__.__name__} with config: {config.type}")
    
    @abstractmethod
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method that must be implemented by derived classes to process and
        transform the input data.

        Parameters:
            data (pd.DataFrame): Input data in the form of a Pandas DataFrame.

        Returns:
            pd.DataFrame: Processed and transformed data as a Pandas DataFrame.
        """
        pass
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """
        Validates the input DataFrame to ensure it is not None or empty.

        This method checks if the input DataFrame is valid by ensuring it is not
        None and contains data.

        Returns:
            bool: True if the input DataFrame is valid, otherwise False.
        """
        if data is None or data.empty:
            logger.warning(f"{self.__class__.__name__}: Input DataFrame is empty")
            return False
        return True