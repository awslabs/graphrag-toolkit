"""Ingestors plan — orchestrates execution of ingestor providers on DataFrames."""
import logging
from typing import List
import pandas as pd
from .ingestors_provider_config import IngestorProviderConfig
from .ingestors_provider_factory import IngestorProviderFactory
from .ingestors_validator import IngestorConfigValidator
from .ingestors_schema import IngestorSchemaTracker
from .ingestors_error_handler import IngestorErrorHandler

# Use document-graph logging
logger = logging.getLogger(__name__)

class IngestorPlan:
    """
    Represents a plan for executing a sequence of ingestor operations on a dataset.

    The IngestorPlan class is used to manage and execute a series of data ingestion steps
    defined by a set of configurations. Each ingestor performs a specific processing step,
    and the class ensures that errors are handled and schema changes are tracked throughout
    the execution. The class also provides functionalities for validating configurations
    upon initialization and tracking schema modifications to aid in debugging or pipeline analysis.
    """
    
    def __init__(self, configs: List[IngestorProviderConfig], validate: bool = True):
        """
        Initialize an IngestorPlan instance.

        The IngestorPlan class manages the initialization and validation of ingestor
        configurations. It tracks the state of ingestor schemas and ensures the
        provided configurations are valid based on the validation rules.

        Arguments:
            configs: List of IngestorProviderConfig objects containing the configurations
                for initializing ingestors.
            validate: Boolean flag to specify whether to validate the configurations.
                Default is True.

        Raises:
            ValueError: If validation is enabled and any configuration errors are detected.
        """
        self.configs = configs
        self.schema_tracker = None
        
        if validate:
            errors = IngestorConfigValidator.validate_configs(configs)
            if errors:
                error_msg = "Ingestor configuration errors:\n" + "\n".join(errors)
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        logger.info(f"IngestorPlan initialized with {len(configs)} ingestors")
    
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes a pipeline of ingestors on the input DataFrame.

        This method processes the provided DataFrame by sequentially applying
        configured ingestors in the pipeline. Each ingestor modifies the DataFrame
        and tracks schema changes using an internal schema tracker. If any ingestor
        fails, the failure is logged, and an error handler processes the issue.

        Parameters:
        data (pd.DataFrame): The input DataFrame to process.

        Returns:
        pd.DataFrame: The resulting DataFrame after applying the pipeline of ingestors.

        Raises:
        Exception: If an unexpected error occurs during the execution of the pipeline.
        """
        if data.empty:
            logger.warning("Input DataFrame is empty")
            return data
        
        # Initialize schema tracking
        self.schema_tracker = IngestorSchemaTracker(data)
        result = data.copy()
        
        logger.info(f"Starting ingestor pipeline: {len(result)} rows, {len(result.columns)} columns")
        
        for i, config in enumerate(self.configs):
            try:
                logger.info(f"Executing ingestor {i+1}/{len(self.configs)}: {config.name} ({config.type})")
                
                # Store state before ingestor
                before_df = result.copy()
                
                # Execute ingestor
                ingestor = IngestorProviderFactory.create(config)
                result = ingestor.ingest(result)
                
                # Track schema changes
                change_type = self._determine_change_type(config.type, before_df, result)
                self.schema_tracker.track_change(
                    ingestor_name=config.name,
                    change_type=change_type,
                    details=config.args,
                    df_before=before_df,
                    df_after=result
                )
                
                logger.info(f"  Completed: {len(before_df)} → {len(result)} rows")
                
            except Exception as e:
                logger.error(f"Ingestor {config.name} failed: {e}")
                result = IngestorErrorHandler.handle_ingestor_failure(config.name, e, result)
        
        # Print final summary
        self.schema_tracker.print_summary()
        
        logger.info(f"Ingestor pipeline completed: {len(result)} rows, {len(result.columns)} columns")
        return result
    
    def get_schema_changes(self):
        """
        Retrieves schema changes tracked by the schema tracker.

        This method checks if a schema tracker exists and, if so, returns the
        changes tracked by it. If no schema tracker is available, an empty list
        is returned.

        Returns:
            list: A list containing the tracked schema changes, or an empty list
            if no schema tracker is present.
        """
        if self.schema_tracker:
            return self.schema_tracker.changes
        return []
    
    def _determine_change_type(self, ingestor_type: str, before_df: pd.DataFrame, after_df: pd.DataFrame) -> str:
        """
        Determines the type of change between two dataframes given their structure and content.

        This method compares two dataframes to determine the nature of the changes
        between them, such as removal or addition of rows, modification of data, or
        alteration of column structure. It also includes logic to detect specific
        column-related changes like renaming, addition, or removal. The determination
        of the change type relies entirely on structural and content differences
        of the dataframes.

        Parameters:
            ingestor_type: str
                Identifier for the type of ingestor or process responsible for
                generating the dataframes.
            before_df: pd.DataFrame
                The initial dataframe before the change was applied.
            after_df: pd.DataFrame
                The modified dataframe after the change was applied.

        Returns:
            str: A string indicating the type of change detected. Possible values are:
                 - "rows_filtered": If the number of rows has changed.
                 - "columns_removed": If fewer columns exist after the change.
                 - "columns_added": If additional columns exist after the change.
                 - "columns_renamed": If the column names differ without a change
                   in the column count.
                 - "data_modified": If neither the rows nor columns changed, but
                   data content was altered.
        """
        if len(before_df) != len(after_df):
            return "rows_filtered"
        elif list(before_df.columns) != list(after_df.columns):
            if len(before_df.columns) > len(after_df.columns):
                return "columns_removed"
            elif len(before_df.columns) < len(after_df.columns):
                return "columns_added"
            else:
                return "columns_renamed"
        else:
            return "data_modified"