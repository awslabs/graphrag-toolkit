# Copyright (c) Evan Erwee. All rights reserved.

"""Base class for transformer providers.

This module defines the abstract base class for all transformer providers in the
document graph processing system. Transformer providers implement data transformation
operations that can be used in ETL pipelines, document processing, and graph building.
The module provides a common interface for all transformers, regardless of their
specific transformation logic.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Dict
from document_graph.transform.transformer_provider_config import TransformerProviderConfig

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


class TransformerProvider(ABC):
    """Abstract base class for all transformers providers.
    
    Provides the interface for data transformation operations
    used by plugins and transformation pipelines. All concrete transformer
    implementations must inherit from this class and implement the transform method.
    
    Attributes:
        config: Configuration object for the transformer
        name: Name of the transformer (from config)
        args: Arguments for the transformer (from config)
        
    Examples:
        >>> # Define a custom transformer
        >>> class LowercaseTransformer(TransformerProvider):
        ...     def transform(self, records):
        ...         for record in records:
        ...             if 'text' in record:
        ...                 record['text'] = record['text'].lower()
        ...         return records
        >>> 
        >>> # Create and use the transformer
        >>> config = TransformerProviderConfig(name="lowercase", args={"fields": ["text"]})
        >>> transformer = LowercaseTransformer(config)
        >>> result = transformer.transform([{"text": "HELLO WORLD"}])
        >>> result[0]["text"]
        'hello world'
    """

    def __init__(self, config: TransformerProviderConfig):
        """Initialize transformers with configuration.
        
        Args:
            config: Transformer configuration with name, type, and args
        """
        self.config = config
        self.name = config.name
        self.args = config.args

    @abstractmethod
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply transformation logic to records.

        Args:
            records: List of record dictionaries to transform

        Returns:
            List of transformed record dictionaries
        """
        pass
    
    def _log_transform_start(self, record_count: int):
        """Log the start of a transformation operation.
        
        This helper method logs a debug message indicating that a transformation
        operation is starting, including the number of records to be processed.
        
        Args:
            record_count: Number of records to be transformed
        """
        import logging
        logger = logging.getLogger(self.__class__.__module__)
        logger.debug(f"[{self.__class__.__name__}] Starting transformation of {record_count} records")
    
    def _log_transform_end(self, input_count: int, output_count: int):
        """Log the completion of a transformation operation.
        
        This helper method logs a debug message indicating that a transformation
        operation has completed, including the number of input and output records.
        
        Args:
            input_count: Number of input records processed
            output_count: Number of output records produced
        """
        import logging
        logger = logging.getLogger(self.__class__.__module__)
        logger.debug(f"[{self.__class__.__name__}] Completed: {input_count} -> {output_count} records")

    def __repr__(self):
        """Return a string representation of the transformer provider.
        
        Returns:
            String representation including the transformer name and type
            
        Examples:
            >>> config = TransformerProviderConfig(name="example", type="test")
            >>> transformer = TransformerProvider(config)
            >>> repr(transformer)
            '<TransformerProvider(name=example, type=test)>'
        """
        return f"<TransformerProvider(name={self.config.name}, type={self.config.type})>"

# Alias for backward compatibility
TransformerConfig = TransformerProviderConfig
