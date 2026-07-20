# Copyright (c) Evan Erwee. All rights reserved.

"""Document Graph Exception Classes and Error Handling.

This module defines a comprehensive set of custom exceptions for document graph operations,
providing specific error types for different failure scenarios. These exceptions enable
precise error handling and debugging throughout the document graph toolkit.

Exception Hierarchy:
    - ModelError: Data model validation and processing failures
    - BatchJobError: Batch processing and ETL pipeline failures
    - IndexError: Graph indexing and search index failures
    - DatabaseConnectionError: Database connection and communication failures
    - ConfigurationError: Configuration validation and parameter failures
    - QueryExecutionError: Graph query execution and syntax failures

Error Handling Patterns:
    All exceptions follow consistent patterns for error reporting and debugging:
    - Clear error messages with context information
    - Preservation of original exception details when wrapping errors
    - Integration with logging system for error tracking
    - Support for error recovery and retry mechanisms

Usage:
    from document_graph.errors import (
        ModelError, DatabaseConnectionError, QueryExecutionError
    )
    
    try:
        result = execute_graph_operation()
    except DatabaseConnectionError as e:
        logger.error(f"Database connection failed: {e}")
        # Implement retry logic
    except QueryExecutionError as e:
        logger.error(f"Query failed: {e}")
        # Handle query errors
    except ModelError as e:
        logger.error(f"Model validation failed: {e}")
        # Handle data model issues
"""

import logging

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


class ModelError(Exception):
    """Raised when model validation or processing fails.
    
    This exception indicates issues with data model validation,
    schema mismatches, or model processing errors.
    
    Examples:
        >>> try:
        ...     validate_document_model(invalid_data)
        ... except ModelError as e:
        ...     print(f"Model validation failed: {e}")
    """
    pass

class BatchJobError(Exception):
    """Raised when batch job operations fail.
    
    This exception indicates failures in batch processing operations,
    such as ETL pipeline errors or bulk data processing issues.
    
    Examples:
        >>> try:
        ...     run_batch_job(job_config)
        ... except BatchJobError as e:
        ...     print(f"Batch job failed: {e}")
    """
    pass

class IndexError(Exception):
    """Raised when graph indexing operations fail.
    
    This exception indicates failures in graph database indexing,
    search index creation, or index maintenance operations.
    
    Examples:
        >>> try:
        ...     create_graph_index(index_config)
        ... except IndexError as e:
        ...     print(f"Index creation failed: {e}")
    """
    pass

class DatabaseConnectionError(Exception):
    """Raised when database connection fails.
    
    This exception indicates failures in establishing or maintaining
    connections to graph databases ( Neptune, Neo4j).
    
    Examples:
        >>> try:
        ...     connect_to_database(connection_config)
        ... except DatabaseConnectionError as e:
        ...     print(f"Database connection failed: {e}")
    """
    pass

class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing.
    
    This exception indicates issues with provider configuration,
    missing required parameters, or invalid configuration values.
    
    Examples:
        >>> try:
        ...     validate_config(user_config)
        ... except ConfigurationError as e:
        ...     print(f"Configuration error: {e}")
    """
    pass

class QueryExecutionError(Exception):
    """Raised when graph query execution fails.
    
    This exception indicates failures in executing Cypher or Gremlin
    queries against graph databases.
    
    Examples:
        >>> try:
        ...     execute_query("MATCH (n) RETURN n LIMIT 10")
        ... except QueryExecutionError as e:
        ...     print(f"Query execution failed: {e}")
    """
    pass
