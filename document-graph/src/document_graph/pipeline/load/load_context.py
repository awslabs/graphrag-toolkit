# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Load Context module for document graph operations."""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401


from typing import Optional
from document_graph.pipeline.extract.extracted_data import ExtractedData
from document_graph.storage.graph.storage_provider_factory import StorageProviderFactory
from document_graph.storage.graph.validated_config import StorageProviderConfig


class LoadContext:
    """
    Represents a context for loading and managing extracted data.

    This class is responsible for handling the management of data extracted from a source.
    It provides utilities to access or create relevant storage mechanisms as well as
    to interact with the data source registration. The class ensures that the data needed
    for processing or analysis is properly handled and stored.

    Attributes:
        extracted_data (ExtractedData): The extracted data object that includes information
        about the data source and registration.
    """
    
    def __init__(self, extracted_data: ExtractedData):
        """
        Initializes a new instance of the class with provided extracted data.

        Attributes:
            extracted_data: ExtractedData
                The data extracted and provided for initialization.

        Arguments:
            extracted_data: ExtractedData
                The extracted data to initialize the instance.
        """
        self.extracted_data = extracted_data
        self._storage = None
    
    @property
    def storage(self):
        """
        Provides accessor for the `storage` property of the class. The property ensures
        that a storage provider instance is initialized and configured properly for
        the class instance. If the storage has already been initialized, the existing
        instance is returned. Otherwise, the storage configuration is built using the
        graph information and a new storage provider instance is created.

        Raises:
            ValueError: If the graph information is not found in the data registration.

        Returns:
            StorageProvider: An instance of a storage provider initialized with the
            corresponding configuration.
        """
        if self._storage is None:
            graph_info = self.extracted_data.source.get_graph_info()
            if not graph_info:
                raise ValueError(f"Graph {self.extracted_data.graph_id} not found in registration")
            
            # Create storage config from registration
            # This would be configured based on the data plane setup
            storage_config = StorageProviderConfig(
                provider_type="graphjson",  # Default, could be from registration
                connection_config={"path": f"/tmp/{self.extracted_data.graph_id}.json"},
                tenant_id=self.extracted_data.tenant_id
            )
            
            self._storage = StorageProviderFactory.for_writer(storage_config)
        
        return self._storage
    
    @property
    def registration(self):
        """
        Provides access to the 'registration' property.

        This property retrieves and returns the value of the 'registration' attribute
        from the 'extracted_data' object.

        Returns:
            The registration value fetched from the 'extracted_data' object.
        """
        return self.extracted_data.registration