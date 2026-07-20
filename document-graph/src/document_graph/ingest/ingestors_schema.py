"""Ingestors schema — tracks schema changes during ingestion pipeline execution."""
import logging
import pandas as pd
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Use document-graph logging
logger = logging.getLogger(__name__)

@dataclass
class IngestorSchemaChange:
    """
    Represents a schema change event in an ingestor.

    This data structure is used to track schema-related changes occurring
    during data ingestion. It stores the details of the change, including
    the type of change, the affected rows, and the affected columns.

    Attributes:
        ingestor_name: Name of the ingestor where the schema change occurred.
        change_type: Type of schema change. Possible values include "column_added",
            "column_removed", "column_renamed", "rows_filtered".
        details: A dictionary containing additional information about the schema change.
        rows_before: Number of rows before the change occurred.
        rows_after: Number of rows after the change occurred.
        columns_before: List of column names present before the change occurred.
        columns_after: List of column names present after the change occurred.
    """
    ingestor_name: str
    change_type: str  # "column_added", "column_removed", "column_renamed", "rows_filtered"
    details: Dict[str, Any]
    rows_before: int
    rows_after: int
    columns_before: List[str]
    columns_after: List[str]

@dataclass
class SourceSchema:
    """
    Represents a source schema with information about dataset structure.

    This class provides metadata and details about the structure of a dataset,
    including its columns, data types, the number of rows, and sample data
    for initial validation or analysis.

    Attributes:
        columns: List of column names in the dataset.
        dtypes: Mapping of column names to their respective data types.
        row_count: Total number of rows in the dataset.
        sample_data: A sample set of data values from the dataset, useful
            for verification or inspection.
    """
    columns: List[str]
    dtypes: Dict[str, str]
    row_count: int
    sample_data: Dict[str, Any]

@dataclass
class IngestorSchema:
    """
    Represents the schema for an ingestor.

    The IngestorSchema class defines the necessary schema required for data ingestion
    operations. It encapsulates the source schema, final columns and their data types,
    final row count, and the list of changes applied during ingestion.

    Attributes:
        source_schema: SourceSchema
            The original schema from the source data being ingested.
        final_columns: List[str]
            The list of column names that will exist in the final ingested data.
        final_dtypes: Dict[str, str]
            A dictionary mapping each column name to its data type in the final data.
        final_row_count: int
            The total row count in the final ingested dataset.
        changes: List[IngestorSchemaChange]
            A list of changes or transformations made to the schema during the ingestion
            process.
    """
    source_schema: SourceSchema
    final_columns: List[str]
    final_dtypes: Dict[str, str]
    final_row_count: int
    changes: List[IngestorSchemaChange]

class IngestorSchemaTracker:
    """
    Tracks schema changes for a source DataFrame during data ingestion processes.

    This class is designed to monitor and document changes to a source DataFrame's schema
    caused by data ingestors. It keeps track of initial schema details, lists all changes made,
    and provides a mechanism to summarize these changes. The class also offers a way to
    retrieve the final schema after all transformations.

    Attributes:
        source_schema: An object representing the schema of the source DataFrame, including
                       column names, data types, row count, and an optional sample of data.
        changes: A list of schema change events recorded during data ingestion.

    Methods:
        __init__(source_df)
            Initializes the schema tracker with the structure of the source DataFrame.
        track_change(ingestor_name, change_type, details, df_before, df_after)
            Records a schema change caused by an ingestor.
        get_final_schema(final_df)
            Constructs and retrieves the final schema after all data ingestion operations.
        print_summary()
            Displays a logged summary of all schema changes.
    """
    
    def __init__(self, source_df: pd.DataFrame):
        """
        Initializes a schema tracker for a source DataFrame.

        Summary:
        The constructor method initializes the schema tracking by capturing details
        about the column names, data types, row count, and sample data from the provided
        DataFrame. It also sets up an empty list to track schema changes and logs an
        initialization message.

        Parameters:
        source_df (pd.DataFrame): The DataFrame for which schema tracking is to be
        initialized.

        Attributes:
        source_schema (SourceSchema): Contains information about the structure of the
        source DataFrame, including columns, data types, row count, and a sample record.
        changes (List[IngestorSchemaChange]): A list intended to track schema changes
        that occur during operations.
        """
        self.source_schema = SourceSchema(
            columns=list(source_df.columns),
            dtypes={col: str(dtype) for col, dtype in source_df.dtypes.items()},
            row_count=len(source_df),
            sample_data=source_df.head(1).to_dict('records')[0] if not source_df.empty else {}
        )
        self.changes: List[IngestorSchemaChange] = []
        logger.info(f"Schema tracker initialized: {len(self.source_schema.columns)} columns, {self.source_schema.row_count} rows")
    
    def track_change(self, ingestor_name: str, change_type: str, details: Dict[str, Any], 
                    df_before: pd.DataFrame, df_after: pd.DataFrame):
        """
        Tracks changes in the ingested data and logs the details of the changes, including the
        number of rows and columns before and after the change.

        Parameters:
        ingestor_name: str
            The name of the ingestor responsible for the dataset change.
        change_type: str
            A description of the type of change applied (e.g., "schema_update").
        details: Dict[str, Any]
            Additional information or metadata about the change.
        df_before: pd.DataFrame
            The dataframe representing the state of the data before the change.
        df_after: pd.DataFrame
            The dataframe representing the state of the data after the change.
        """
        change = IngestorSchemaChange(
            ingestor_name=ingestor_name,
            change_type=change_type,
            details=details,
            rows_before=len(df_before),
            rows_after=len(df_after),
            columns_before=list(df_before.columns),
            columns_after=list(df_after.columns)
        )
        self.changes.append(change)
        
        logger.info(f"{ingestor_name}: {change_type} - {change.rows_before}→{change.rows_after} rows, "
                   f"{len(change.columns_before)}→{len(change.columns_after)} columns")
    
    def get_final_schema(self, final_df: pd.DataFrame) -> IngestorSchema:
        """
        Generate the final schema for the given DataFrame.

        This method prepares an instance of IngestorSchema by collecting metadata
        from the provided DataFrame, including column names, data types, row count,
        and any registered changes. It helps to encapsulate the final schema details
        for use in data ingestion or transformation processes.

        Args:
            final_df (pd.DataFrame): A DataFrame which contains the finalized data structure.

        Returns:
            IngestorSchema: An object encapsulating the schema details including
            source schema, final column names, data types, row count, and applied changes.
        """
        return IngestorSchema(
            source_schema=self.source_schema,
            final_columns=list(final_df.columns),
            final_dtypes={col: str(dtype) for col, dtype in final_df.dtypes.items()},
            final_row_count=len(final_df),
            changes=self.changes
        )
    
    def print_summary(self):
        """
        Logs a summary of the schema changes during the ingestion process.

        The method provides a detailed summary of the source schema and all
        the changes applied during the ingestion. Each change is logged with
        its name, type of change, number of rows and columns before and after
        the change, and additional details if provided. The final state of the
        data after all changes is also logged.

        Raises:
            AttributeError: If required attributes like `source_schema` or
            `changes` are missing or improperly formatted.
        """
        logger.info("=== INGESTOR SCHEMA SUMMARY ===")
        logger.info(f"Source: {len(self.source_schema.columns)} columns, {self.source_schema.row_count} rows")
        
        for change in self.changes:
            logger.info(f"  {change.ingestor_name}: {change.change_type}")
            logger.info(f"    Rows: {change.rows_before} → {change.rows_after}")
            logger.info(f"    Columns: {len(change.columns_before)} → {len(change.columns_after)}")
            if change.details:
                logger.info(f"    Details: {change.details}")
        
        if self.changes:
            final_change = self.changes[-1]
            logger.info(f"Final: {len(final_change.columns_after)} columns, {final_change.rows_after} rows")
        logger.info("=== END SUMMARY ===")