# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import abc  
import uuid
from dataclasses import dataclass
from tenacity import Retrying, stop_after_attempt, wait_random
from tenacity import RetryCallState
from typing import Callable, List, Dict, Any, Optional

from graphrag_toolkit.lexical_graph import TenantId

from llama_index.core.bridge.pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

REDACTED = '**REDACTED**'
NUM_CHARS_IN_DEBUG_RESULTS = 256

def get_log_formatting(args):
    """
    Retrieves the log formatting configuration from the given arguments. If the
    'log_formatting' key is not present in the `args` dictionary, a default
    `RedactedGraphQueryLogFormatting` instance is used. Ensures the log formatting
    object is an instance of `GraphQueryLogFormatting`.

    Args:
        args (dict): A dictionary containing configuration parameters, including
            an optional 'log_formatting' key.

    Returns:
        GraphQueryLogFormatting: The log formatting object retrieved from the
        input dictionary, or the default `RedactedGraphQueryLogFormatting` if
        not provided.

    Raises:
        ValueError: If the value associated with the 'log_formatting' key in the
        input `args` dictionary is not an instance of `GraphQueryLogFormatting`.
    """
    log_formatting = args.pop('log_formatting', RedactedGraphQueryLogFormatting())
    if not isinstance(log_formatting, GraphQueryLogFormatting):
        raise ValueError('log_formatting must be of type GraphQueryLogFormatting')
    return log_formatting

@dataclass
class NodeId:
    """
    Represents an identifier node with key-value pair attributes.

    This class encapsulates a node identifier consisting of a key, value, and an
    optional flag to determine if the identifier is based on properties. It can
    be used to represent nodes with unique identifiers in various contexts.

    Attributes:
        key (str): The key associated with the node identifier.
        value (str): The value corresponding to the key for the node identifier.
        is_property_based (bool): Indicates whether the node is based on properties.
            Defaults to True.
    """
    key:str
    value:str
    is_property_based:bool = True

    def __str__(self):
        return self.value
    
def format_id(id_name:str):
    """
    Parses and formats the given ID string into a NodeId object.

    The function takes a string representing an ID and splits it by the delimiter '.'.
    If the string does not contain the delimiter, it assumes the ID is standalone and
    constructs a NodeId object where both the identifier and full ID are the same.
    If the string contains the delimiter, it uses the second part of the split string
    as the identifier and the entire string as the full ID.

    Args:
        id_name: A string representing a potentially delimited ID.

    Returns:
        NodeId: An instance of NodeId containing the formatted identifier and full ID.
    """
    parts = id_name.split('.')
    if len(parts) == 1:
        return NodeId(parts[0], parts[0])
    else:
        return NodeId(parts[1], id_name)

@dataclass
class GraphQueryLogEntryParameters:
    """
    Represents the parameters of a log entry for a graph query.

    This class is a container for details related to a graph query log entry,
    such as the reference identifier, the query string, and optional associated
    parameters or results. It is designed to provide structured access to these
    elements, making it easier to manage and manipulate data regarding graph
    queries.

    Attributes:
        query_ref (str): A reference identifier for the graph query.
        query (str): The graph query string.
        parameters (str): Parameters associated with the graph query.
        results (Optional[str]): Optional results of the graph query, if available.
    """
    query_ref:str
    query:str
    parameters:str
    results:Optional[str]=None

    def format_query_with_query_ref(self, q):
        """
        Formats the provided query by appending the query reference as a prefix.

        Args:
            q: The SQL query to be formatted.

        Returns:
            A formatted string containing the query reference prefixed to the
            original query.
        """
        return f'//query_ref: {self.query_ref}\n{q}'

class GraphQueryLogFormatting(BaseModel):
    """
    Abstract base class for formatting graph query log entries.

    This class provides a blueprint for implementing custom formats for
    logging graph query details. The purpose of this class is to ensure
    a consistent structure for logging information about graph queries,
    including the query reference, query string, associated parameters,
    and potential results.

    Attributes:
        None
    """
    @abc.abstractmethod
    def format_log_entry(self, query_ref:str, query:str, parameters:Dict[str,Any]={}, results:Optional[List[Any]]=None) -> GraphQueryLogEntryParameters:
        """
        Formats a log entry for a graph query execution.

        This method must be implemented by a subclass and is responsible for
        formatting the details of a graph query, including its reference, query
        string, parameters, and execution results, into a specific structure
        that conforms to `GraphQueryLogEntryParameters`. This allows for
        consistent logging of query execution metadata.

        Args:
            query_ref: The reference identifier for the executed query.
            query: The query string executed against the graph database.
            parameters: A dictionary of parameters used in the query. Defaults
                to an empty dictionary if no parameters are provided.
            results: An optional list of results returned by the query
                execution. Defaults to None if no results are returned.

        Returns:
            GraphQueryLogEntryParameters: The formatted log entry object
            containing details about the query execution.

        Raises:
            NotImplementedError: If this method is not implemented by the
            subclass.
        """
        raise NotImplementedError
    
class RedactedGraphQueryLogFormatting(GraphQueryLogFormatting):
    def format_log_entry(self, query_ref:str, query:str, parameters:Dict[str,Any]={}, results:Optional[List[Any]]=None) -> GraphQueryLogEntryParameters:
        """
        Formats and creates a new `GraphQueryLogEntryParameters` instance with
        the given query reference, query, parameters, and results, while redacting
        specified information.

        Args:
            query_ref (str): A reference identifier for the query.
            query (str): The query string to be logged.
            parameters (Dict[str, Any], optional): A dictionary of parameters used
                in the query. Defaults to an empty dictionary.
            results (Optional[List[Any]], optional): A list of results generated by
                the query. Defaults to None.

        Returns:
            GraphQueryLogEntryParameters: An instance containing the formatted and
            redacted log entry details.
        """
        lines = query.split('\n')
        redacted_query = '\n'.join(line for line in lines if line.startswith('//')) 
        return GraphQueryLogEntryParameters(query_ref=query_ref, query=redacted_query or REDACTED, parameters=REDACTED, results=REDACTED)

class NonRedactedGraphQueryLogFormatting(GraphQueryLogFormatting):
    def format_log_entry(self, query_ref:str, query:str, parameters:Dict[str,Any]={}, results:Optional[List[Any]]=None) -> GraphQueryLogEntryParameters:
        """
        Formats a log entry for a graph query execution, including the query reference,
        query string, parameters used, and the results. Truncates the results string if
        it exceeds a predefined character limit, appending a note of the truncated length.

        Args:
            query_ref: A string representing the unique reference or identifier for the query.
            query: A string representing the graph query executed.
            parameters: A dictionary containing key-value pairs representing query
                parameters. Defaults to an empty dictionary.
            results: An optional list containing the results of the query. Defaults to
                None.

        Returns:
            GraphQueryLogEntryParameters: A dataclass representing the formatted log entry
                with the provided query details and truncated results if applicable.
        """
        results_str = str(results)
        if len(results_str) > NUM_CHARS_IN_DEBUG_RESULTS:
            results_str = f'{results_str[:NUM_CHARS_IN_DEBUG_RESULTS]}... <{len(results_str) - NUM_CHARS_IN_DEBUG_RESULTS} more chars>'
        return GraphQueryLogEntryParameters(query_ref=query_ref, query=query, parameters=str(parameters), results=results_str)

def on_retry_query(
    logger:'logging.Logger',
    log_level:int,
    log_entry_parameters:GraphQueryLogEntryParameters,
    exc_info:bool=False    
) -> Callable[[RetryCallState], None]:
    """
    Creates a logging function to log retry attempts for a query.

    This function is intended to generate a reusable logger callback to be used
    with retry mechanisms like tenacity's RetryCallState. It logs the outcome
    and decision for retrying, based on the query execution results or exceptions.

    Args:
        logger (logging.Logger): The logger instance used to log the retry information.
        log_level (int): The logging level at which the retry attempt will be logged.
        log_entry_parameters (GraphQueryLogEntryParameters): The metadata that identifies
            the query, including query reference, parameters, and additional context.
        exc_info (bool, optional): Indicates whether exception information should be
            included in the logs if an exception is raised. Defaults to False.

    Returns:
        Callable[[RetryCallState], None]: A logging function that logs retry attempts
        using the provided logger parameters and retry state.
    """
    def log_it(retry_state: 'RetryCallState') -> None:
        """
        Logs information about a query being retried using the provided logger. This function is intended to be used
        in conjunction with a retry mechanism, logging relevant details about the retry attempt, such as the amount
        of time until the next retry, the outcome of the prior attempt, and additional query-related metadata.

        Args:
            logger: The logger instance used for writing the log message. It is expected to support the standard
                logging methods like `log(level, message, ...)`.
            log_level: The logging level used when invoking the logger to output the retry information.
            log_entry_parameters: An instance of `GraphQueryLogEntryParameters`, containing metadata about
                the query being retried, such as reference identifiers, the query itself, and its associated parameters.
            exc_info: A boolean indicating whether exception information should be included in the log entry if
                the query execution raised an exception. Defaults to `False`.

        Returns:
            A callable function that takes a `RetryCallState` object and logs the retry information.

        Raises:
            RuntimeError: If `retry_state.outcome` or `retry_state.next_action` attributes are not set before
                invoking the inner logging function.
        """
        local_exc_info: BaseException | bool | None

        if retry_state.outcome is None:
            raise RuntimeError('log_it() called before outcome was set')

        if retry_state.next_action is None:
            raise RuntimeError('log_it() called before next_action was set')

        if retry_state.outcome.failed:
            ex = retry_state.outcome.exception()
            verb, value = 'raised', f'{ex.__class__.__name__}: {ex}'

            if exc_info:
                local_exc_info = retry_state.outcome.exception()
            else:
                local_exc_info = False
        else:
            verb, value = 'returned', retry_state.outcome.result()
            local_exc_info = False  # exc_info does not apply when no exception

        logger.log(
            log_level,
            f'[{log_entry_parameters.query_ref}] Retrying query in {retry_state.next_action.sleep} seconds because it {verb} {value} [attempt: {retry_state.attempt_number}, query: {log_entry_parameters.query}, parameters: {log_entry_parameters.parameters}]',
            exc_info=local_exc_info
        )

    return log_it

def on_query_failed(
    logger:'logging.Logger',
    log_level:int,
    max_attempts:int,
    log_entry_parameters:GraphQueryLogEntryParameters,
) -> Callable[['RetryCallState'], None]:
    """
    Handles logging for query failure during retries.

    Logs information about a failed query attempt if the maximum number of retry
    attempts has been reached. If the query raises an exception, details about
    the exception are also logged. The log entry includes the query reference,
    query string, associated parameters, and the reason for the final failure.

    Args:
        logger (logging.Logger): Logger instance to use for logging failure
            information.
        log_level (int): Log level used for logging the failure message.
        max_attempts (int): Maximum number of retry attempts before failure is
            logged.
        log_entry_parameters (GraphQueryLogEntryParameters): Parameters used to
            construct the log entry, including query reference, query string,
            and related parameters.

    Returns:
        Callable[[RetryCallState], None]: A function to be called during each
            retry attempt to check if logging is necessary for query failure.
    """
    def log_it(retry_state: 'RetryCallState') -> None:
        """
        Handles logging of query retries and failures, specifically after reaching the
        maximum allowed attempts, along with the associated exception details and query
        parameters.

        Args:
            logger: The logger instance to use for logging query failure information.
            log_level: The logging level as an integer (e.g., logging.INFO, logging.ERROR).
            max_attempts: The maximum number of retry attempts before considering a query
                as failed.
            log_entry_parameters: A `GraphQueryLogEntryParameters` object containing details
                about the query, query reference, and associated parameters.

        Returns:
            Callable[['RetryCallState'], None]: A callable function that logs the details
            of a failed query, processing the given retry state for contextual information.
        """
        if retry_state.attempt_number == max_attempts:
            ex: BaseException | bool | None
            if retry_state.outcome.failed:
                ex = retry_state.outcome.exception()
                verb, value = 'raised', f'{ex.__class__.__name__}: {ex}'       
            logger.log(
                log_level,
                f'[{log_entry_parameters.query_ref}] Query failed after {retry_state.attempt_number} retries because it {verb} {value} [query: {log_entry_parameters.query}, parameters: {log_entry_parameters.parameters}]',
                exc_info=ex
            )
        
    return log_it

class GraphStore(BaseModel):
    """
    Manages the storage and execution of graph queries with configurable retry logic
    and logging functionality.

    Detailed description:
    This class provides methods to execute graph queries with retry mechanisms, log
    formatting features, and utilities for handling node IDs and property assignments.
    It is designed to interact with a graph database while ensuring resilience through
    retry attempts and structured error logging.

    Attributes:
        log_formatting (GraphQueryLogFormatting): Handles the formatting of log entries
            for executed queries.
        tenant_id (TenantId): Represents the unique identifier of the tenant.
    """
    log_formatting:GraphQueryLogFormatting = Field(default_factory=lambda: RedactedGraphQueryLogFormatting())
    tenant_id:TenantId = Field(default_factory=lambda: TenantId())

    def execute_query_with_retry(self, query:str, parameters:Dict[str, Any], max_attempts=3, max_wait=5, **kwargs) -> Dict[str, Any]:
        """
        Executes a database query with a retry mechanism, allowing multiple attempts with delays between them.

        This method provides a mechanism to execute a query on a database reliably, allowing retries in the event
        of transient failures or network issues. Each retry includes logging related to the attempt, along with a
        unique correlation ID for tracing purposes.

        Args:
            query:
                The SQL query string to be executed.
            parameters:
                A dictionary of query parameters to bind to the SQL query.
            max_attempts:
                The maximum number of retry attempts allowed, including the initial attempt. Default is 3.
            max_wait:
                The maximum wait time in seconds between retry attempts. The exact wait is randomized between 0
                and this value. Default is 5.
            **kwargs:
                Additional parameters that may include a pre-specified 'correlation_id'. Any such value will be
                included in the logging and supplemented with a unique suffix.

        """
        correlation_id = uuid.uuid4().hex[:5]
        if 'correlation_id' in kwargs:
            correlation_id = f'{kwargs["correlation_id"]}/{correlation_id}'
        kwargs['correlation_id'] = correlation_id

        log_entry_parameters = self.log_formatting.format_log_entry(f'{correlation_id}/*', query, parameters)

        attempt_number = 0
        for attempt in Retrying(
            stop=stop_after_attempt(max_attempts), 
            wait=wait_random(min=0, max=max_wait),
            before_sleep=on_retry_query(logger, logging.WARNING, log_entry_parameters), 
            after=on_query_failed(logger, logging.WARNING, max_attempts, log_entry_parameters),
            reraise=True
        ):
            with attempt:
                attempt_number += 1
                attempt.retry_state.attempt_number
                return self._execute_query(query, parameters, **kwargs)

    def _logging_prefix(self, query_id:str, correlation_id:Optional[str]=None):
        """
        Generates a logging prefix by combining the query ID with an optional correlation ID.

        The method returns a string that concatenates the correlation ID and query ID, separated by
        a forward slash, if a correlation ID is provided. Otherwise, it only returns the query ID.

        Args:
            query_id: The unique identifier for the query.
            correlation_id: An optional unique identifier for correlating logs or transactions.

        Returns:
            str: The constructed logging prefix based on the available identifiers.
        """
        return f'{correlation_id}/{query_id}' if correlation_id else f'{query_id}'
    
    def node_id(self, id_name:str) -> NodeId:
        """
        Formats a given identifier name into a NodeId object.

        This method takes an identifier name, processes it, and returns a NodeId
        object. The output ensures the identifier is in the correct format
        for consistent usage within the application.

        Args:
            id_name (str): The identifier name to be formatted.

        Returns:
            NodeId: The formatted identifier.
        """
        return format_id(id_name)
    
    def property_assigment_fn(self, key:str, value:Any) -> Callable[[str], str]:
        """
        Assigns a value to a key and returns a callable that processes a string.

        This function is used to create a lambda function that captures the input
        parameters and processes an additional string based on the implementation of
        the returned function.

        Args:
            key: The key to which the value is assigned.
            value: The value to be assigned to the specified key.

        Returns:
            Callable[[str], str]: A callable that takes a string as input and returns
            a string after processing. The exact processing implemented in the
            returned callable depends on the specific implementation.
        """
        return lambda x: x
    
    def execute_query(self, cypher, parameters={}, correlation_id=None) -> Dict[str, Any]:
        if correlation_id:
            return self.execute_query_with_retry(
                query=cypher, 
                parameters=parameters,
                max_attempts=1, 
                max_wait=0, 
                correlation_id=correlation_id
            )
        else:
            return self.execute_query_with_retry(
                query=cypher, 
                parameters=parameters,
                max_attempts=1, 
                max_wait=0
            )

    
    @abc.abstractmethod
    def _execute_query(self, cypher, parameters={}, correlation_id=None) -> Dict[str, Any]:
        """
        Executes a Cypher query on a connected database and returns the result as a dictionary.

        Args:
            cypher: The Cypher query string to be executed.
            parameters: Optional dictionary of parameters to be passed into the Cypher query.
            correlation_id: Optional identifier for correlating logs or tracing execution flows.

        Returns:
            A dictionary containing the results of the executed Cypher query.
        """
        raise NotImplementedError



    