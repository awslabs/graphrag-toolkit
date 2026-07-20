# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Load Result module for document graph operations."""

import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from pydantic import BaseModel


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401



class LoadResult(BaseModel):
    """
    Represents the outcome of a document load operation.

    This class is used to encapsulate the details and status of a load operation,
    including the document identifier, records loaded, load time, overall status,
    and additional metadata related to the load. It provides utility methods to
    check the success or emptiness of the load result and class methods to create
    specific types of results such as successful, empty, or failed load outcomes.

    Attributes:
        document_id (str): Identifier of the document being loaded.
        records_loaded (int): Number of records successfully loaded. Defaults to 0.
        load_time (str): Timestamp when the load operation occurred. If not provided
            during initialization, it will be automatically generated in UTC.
        status (str): Status of the load operation, defaulting to "success".
        transformations_applied (bool): Indicates whether transformations were applied
            during the load process. Defaults to False.
        output_files (Optional[Dict[str, str]]): Dictionary containing file paths for
            generated output files. Defaults to None if not provided.
        metadata (Optional[Dict[str, Any]]): Additional metadata related to the load
            operation. Defaults to None if not provided.
    """
    
    document_id: str
    records_loaded: int = 0
    load_time: str
    status: str = "success"
    transformations_applied: bool = False
    output_files: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None

    def __init__(self, **data):
        """
        Initializes a LoadResult object.

        This method sets the load_time for the object upon creation if it is not explicitly
        provided in the input data. It then invokes the parent class initializer with the
        given data. A debug log entry is generated for the initialization of a LoadResult
        instance.

        Attributes:
            load_time (str): A UTC timestamp indicating the object's creation time. If not
                             provided, it defaults to the current time.

        Parameters:
            data (dict): Arbitrary keyword arguments representing the properties of the
                         LoadResult object.

        Returns:
            None
        """
        if 'load_time' not in data:
            data['load_time'] = datetime.now(timezone.utc).isoformat()
        super().__init__(**data)
        logger.debug(f"Created LoadResult for document: {self.document_id}")

    def __str__(self) -> str:
        """
        Converts the object to its string representation.

        Returns
        -------
        str
            A string that includes the document ID, the number of records loaded, and
            the status of the operation.
        """
        return f"LoadResult(document_id={self.document_id}, records_loaded={self.records_loaded}, status={self.status})"

    def is_successful(self) -> bool:
        """
        Determines whether the operation was successful based on its status
        and the number of records loaded.

        Returns:
            bool: True if the status is "success" and at least one record was
            loaded; otherwise, False.
        """
        return self.status == "success" and self.records_loaded > 0

    def is_empty(self) -> bool:
        """
        Determines whether the collection is empty.

        This method evaluates whether the number of loaded records equals zero,
        indicating that the collection is currently empty.

        Returns:
            bool: True if the collection has no loaded records; otherwise, False.
        """
        return self.records_loaded == 0

    @classmethod
    def create_success(
        cls,
        document_id: str,
        records_loaded: int,
        output_files: Optional[Dict[str, str]] = None,
        transformations_applied: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "LoadResult":
        """Create a successful load result.
        
        Args:
            document_id: Document identifier
            records_loaded: Number of records loaded
            output_files: Dictionary of output file paths
            transformations_applied: Whether transformations were applied
            metadata: Additional metadata
            
        Returns:
            LoadResult: Successful load result
        """
        logger.info(f"Creating successful load result: {document_id} ({records_loaded} records)")
        return cls(
            document_id=document_id,
            records_loaded=records_loaded,
            status="success",
            transformations_applied=transformations_applied,
            output_files=output_files or {},
            metadata=metadata or {}
        )

    @classmethod
    def create_empty(
        cls,
        document_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "LoadResult":
        """Create an empty load result.
        
        Args:
            document_id: Document identifier
            metadata: Additional metadata
            
        Returns:
            LoadResult: Empty load result
        """
        logger.warning(f"Creating empty load result: {document_id}")
        return cls(
            document_id=document_id,
            records_loaded=0,
            status="empty_result",
            metadata=metadata or {}
        )

    @classmethod
    def create_failed(
        cls,
        document_id: str,
        error_message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "LoadResult":
        """Create a failed load result.
        
        Args:
            document_id: Document identifier
            error_message: Error message describing the failure
            metadata: Additional metadata
            
        Returns:
            LoadResult: Failed load result
        """
        logger.error(f"Creating failed load result: {document_id} - {error_message}")
        metadata = metadata or {}
        metadata["error_message"] = error_message
        return cls(
            document_id=document_id,
            records_loaded=0,
            status="failed",
            metadata=metadata
        )