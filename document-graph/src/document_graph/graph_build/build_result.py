# Copyright (c) Evan Erwee. All rights reserved.

"""Build Result module for document graph operations.

This module defines the BuildResult class, which represents the complete
output of a build operation. It includes information about the build status,
output files, graph statistics, and metadata. It also provides factory methods
for creating success, empty, and failed results.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from pydantic import BaseModel, ConfigDict, computed_field
from graphrag_toolkit.lexical_graph import TenantId, to_tenant_id


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


class BuildResult(BaseModel):
    """
    Represents the result of a build operation.

    The BuildResult class models the outcome of a build process, providing details
    such as the document identifier, tenant information, build mode, and status. It
    is designed to encapsulate various states including successful, empty, or
    failed build results. This class includes helper methods to simplify the
    creation of instances based on different build scenarios.

    Attributes:
        document_id (str): The unique identifier of the document associated with the
            build operation.
        tenant_id (TenantId): The tenant information to which this build result
            belongs.
        build_mode (str): The mode in which the build operation was performed. Can
            be one of: "graph", "code", "schema", or "sample".
        build_time (str): The timestamp indicating when the build was created.
        status (str): The status of the build operation, default is "success". Other
            valid statuses include "empty_input" and "failed".
        output_files (Optional[Dict[str, str]]): A dictionary containing the output
            files resulting from the build operation.
        graph_stats (Optional[Dict[str, Any]]): Optional metrics related to the
            generated graph during the build.
        metadata (Optional[Dict[str, Any]]): Additional metadata about the build
            operation.
    """

    #Add this to allow custom TenantId types
    model_config = ConfigDict(arbitrary_types_allowed=True)

    document_id: str
    tenant_id: TenantId
    build_mode: str  # graph, code, schema, sample
    build_time: str
    status: str = "success"
    output_files: Optional[Dict[str, str]] = None
    graph_stats: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    tenant_id: Optional[str] = None

    @computed_field
    @property
    def scope_id(self) -> Optional[str]:
        """Returns 'tenant_id.tenant_id' when both present, None otherwise."""
        if self.tenant_id is not None and self.tenant_id is not None:
            return f"{self.tenant_id}.{self.tenant_id}"
        return None

    def __init__(self, **data):
        """
        [Brief one-line description of the function's purpose]
        
        [Detailed explanation of what the function does and how it works]
        
        Args:
            [param_name] ([param_type]): [Description of parameter]
            [param_name] ([param_type], optional): [Description of parameter]. Defaults to [default_value].
        
        Returns:
            [return_type]: [Description of return value]
        
        Raises:
            [exception_type]: [Conditions under which exception is raised]
        """
        if 'build_time' not in data:
            data['build_time'] = datetime.now(timezone.utc).isoformat()
        if 'tenant_id' in data:
            data['tenant_id'] = to_tenant_id(data['tenant_id'])
        super().__init__(**data)
        logger.debug(f"Created BuildResult for document: {self.document_id} (mode: {self.build_mode})")

    def __str__(self) -> str:
        return f"BuildResult(document_id={self.document_id}, mode={self.build_mode}, status={self.status})"

    def is_successful(self) -> bool:
        """
        Determines whether the operation was successful based on the status attribute.

        Returns
        -------
        bool
            True if the status attribute equals "success", otherwise False.
        """
        return self.status == "success"

    def is_empty(self) -> bool:
        """
        Determines if the current instance represents an empty state.

        This method checks whether the instance is in an empty state based on its
        `status` attribute or the lack of output files.

        Returns:
            bool: True if the instance is in an empty state, False otherwise.
        """
        return self.status == "empty_input" or not self.output_files

    @classmethod
    def create_success(
        cls,
        document_id: str,
        build_mode: str,
        output_files: Dict[str, str],
        tenant_id: Optional[str] = None,
        graph_stats: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None
    ) -> "BuildResult":
        """
        Creates a successful build result object for a document build process.

        Summary:
        This class method is responsible for generating a `BuildResult` instance
        representing a successful document build operation. The method logs the process
        and initializes the `BuildResult` object with relevant details such as document
        ID, build mode, output files, and optional metadata or stats.

        Args:
            document_id (str): The unique identifier of the document being built.
            build_mode (str): The mode used during the build process (e.g., production,
                draft).
            output_files (Dict[str, str]): A mapping of output file types to their
                corresponding paths or locations.
            tenant_id (Optional[str]): The identifier of the tenant, if applicable.
            graph_stats (Optional[Dict[str, Any]]): Statistics about graph data, if
                available.
            metadata (Optional[Dict[str, Any]]): Additional metadata associated with
                the build.
            tenant_id (Optional[str]): The root identifier for scoped operation.

        Returns:
            BuildResult: A `BuildResult` instance initialized with the provided data.

        Raises:
            None
        """
        logger.info(f"Creating successful build result: {document_id} (mode: {build_mode})")
        return cls(
            document_id=document_id,
            tenant_id=to_tenant_id(tenant_id),
            build_mode=build_mode,
            status="success",
            output_files=output_files,
            graph_stats=graph_stats or {},
            metadata=metadata or {},
        )

    @classmethod
    def create_empty(
        cls,
        document_id: str,
        build_mode: str,
        tenant_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None
    ) -> "BuildResult":
        """
        Creates an empty BuildResult object with a predefined status and optional metadata.

        This class method serves as a utility to instantiate a BuildResult object in situations
        where no input is provided or where an empty initialization is required. It initializes
        the object with the provided `document_id`, `build_mode`, `tenant_id`, and optional
        metadata. The status of the created object is set to "empty_input" by default.

        Arguments:
            document_id: str
                Unique identifier of the document associated with the build result.
            build_mode: str
                Specifies the mode in which the build is executed.
            tenant_id: Optional[str]
                Identifier for the tenant, if applicable. If not provided,
                a default tenant ID will be used.
            metadata: Optional[Dict[str, Any]]
                Additional information related to the build result, provided
                as a dictionary. Defaults to an empty dictionary if not provided.
            tenant_id: Optional[str]
                The root identifier for scoped operation.

        Returns:
            BuildResult
                A new instance of BuildResult with the specified attributes and
                a predefined status "empty_input".

        Additional Notes:
            This method logs a warning message indicating that an empty build
            result is being created for the specified document.
        """
        logger.warning(f"Creating empty build result: {document_id}")
        return cls(
            document_id=document_id,
            tenant_id=to_tenant_id(tenant_id),
            build_mode=build_mode,
            status="empty_input",
            metadata=metadata or {},
        )

    @classmethod
    def create_failed(
        cls,
        document_id: str,
        build_mode: str,
        error_message: str,
        tenant_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None
    ) -> "BuildResult":
        """
        Creates a failed build result with the provided details.

        This method initializes a failed build result instance for the specified document,
        document creation/build mode, and error message.

        Parameters:
            document_id: str
                Identifier of the document for which the build has failed.
            build_mode: str
                Mode in which the build was attempted.
            error_message: str
                Detailed error message explaining the failure.
            tenant_id: Optional[str]
                Optional identifier of the tenant. Defaults to None.
            metadata: Optional[Dict[str, Any]]
                Optional additional metadata associated with the failure. Defaults to None.
            tenant_id: Optional[str]
                The root identifier for scoped operation.

        Returns:
            BuildResult
                An instance of the BuildResult class in a failed state with the provided information.
        """
        logger.error(f"Creating failed build result: {document_id} - {error_message}")
        metadata = metadata or {}
        metadata["error_message"] = error_message
        return cls(
            document_id=document_id,
            tenant_id=to_tenant_id(tenant_id),
            build_mode=build_mode,
            status="failed",
            metadata=metadata,
        )
