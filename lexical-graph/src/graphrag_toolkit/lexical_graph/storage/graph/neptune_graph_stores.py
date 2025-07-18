# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import logging
import time
import uuid
from botocore.config import Config
from typing import Optional, Any, Callable
from importlib.metadata import version, PackageNotFoundError
from dateutil.parser import parse

from graphrag_toolkit.lexical_graph.storage.graph import GraphStoreFactoryMethod, GraphStore, NodeId, get_log_formatting
from graphrag_toolkit.lexical_graph.metadata import format_datetime, is_datetime_key
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from llama_index.core.bridge.pydantic import PrivateAttr

NEPTUNE_ANALYTICS = 'neptune-graph://'
NEPTUNE_DATABASE = 'neptune-db://'
NEPTUNE_DB_DNS = 'neptune.amazonaws.com'

logger = logging.getLogger(__name__)

def format_id_for_neptune(id_name:str):
    """
    Formats an identifier string into a structured format suitable for Neptune.

    This function accepts an identifier string, splits it into parts based on the
    presence of a dot ('.'), and returns a `NodeId` object with appropriately
    formatted values based on these parts. If the identifier does not contain a
    dot, it uses the entire string as the identifier's base name and assigns a
    fixed format for the resulting instance.

    Args:
        id_name (str): The identifier string to format, which can contain a dot
            separating different parts.

    Returns:
        NodeId: An object that encapsulates the formatted identifier, including its
        base name, formatted string, and a fixed boolean flag.
    """
    parts = id_name.split('.')
    if len(parts) == 1:
        return NodeId(parts[0], '`~id`', False)
    else:
        return NodeId(parts[1], f'id({parts[0]})', False)
        
def create_config(config:Optional[str]=None):
    """
    Creates a configuration object for the application, including retries, timeouts, and
    optional user-specified arguments in JSON format.

    This function initializes a configuration with default settings, such as retry policies,
    read timeouts, and user agent identifiers. If a configuration string in JSON format is
    provided, it will be parsed and applied to augment or override default settings. The
    toolkit version is dynamically fetched if available.

    Args:
        config: A JSON-formatted string containing additional configuration settings. If not
            provided, only default settings will be used.

    Returns:
        Config: An initialized configuration object containing properties such as retry
        settings, timeouts, and user agent details.
    """
    toolkit_version = 'unknown'

    try:
        toolkit_version = version('graphrag-toolkit-lexical-graph')
    except PackageNotFoundError:
        pass

    config_args = {}
    if config:
        config_args = json.loads(config)
    return Config(
        retries={
            'total_max_attempts': 1, 
            'mode': 'standard'
        }, 
        read_timeout=600,
        user_agent_appid=f'graphrag-toolkit-lexical-graph-{toolkit_version}',
        **config_args
    )

def create_property_assigment_fn_for_neptune(key:str, value:Any) -> Callable[[str], str]:
    """
    Creates a property assignment function for Neptune keys and values, enabling specialized handling for
    datetime keys to format their associated values while leaving other keys untouched.

    Args:
        key (str): The key associated with the property, which can affect the behavior based on its type.
        value (Any): The value to be assigned, specifically checked and formatted if the key indicates
            datetime.

    Returns:
        Callable[[str], str]: A function that takes a single string argument and returns a string. This
            function formats the input value depending on whether the key is determined to represent
            datetime.
    """
    if is_datetime_key(key):
        try:
            format_datetime(value)
            return lambda x: f'datetime({x})'
        except ValueError as e:
            return lambda x: x
    else:
        return lambda x: x

class NeptuneAnalyticsGraphStoreFactory(GraphStoreFactoryMethod):
    """
    Factory class to create instances of NeptuneAnalyticsClient.

    This class serves as a factory for creating instances of a NeptuneAnalyticsClient
    using the factory method pattern. It provides a mechanism to conditionally create
    the graph store depending on the provided identifier and configuration parameters.

    Attributes:
        None
    """
    def try_create(self, graph_info:str, **kwargs) -> GraphStore:
        """
        Attempts to create and return an instance of `NeptuneAnalyticsClient` if the provided
        graph information matches the expected format. If the graph information does not match,
        `None` is returned. The function is used for initializing a graph connection based
        on specific criteria.

        Args:
            graph_info (str): Information about the graph, expected to start with the
                `NEPTUNE_ANALYTICS` prefix to successfully create a client instance.
            **kwargs: Additional optional parameters, such as configuration settings.
                The `config` parameter, if provided, will be used to customize
                the initialization of the `NeptuneAnalyticsClient`.

        Returns:
            GraphStore: An instance of `NeptuneAnalyticsClient` if the graph info matches the
                expected conditions. Otherwise, `None` is returned.
        """
        if graph_info.startswith(NEPTUNE_ANALYTICS):

            graph_id = graph_info[len(NEPTUNE_ANALYTICS):]
            config = kwargs.pop('config', {})

            logger.debug(f'Opening Neptune Analytics graph [graph_id: {graph_id}]')
            return NeptuneAnalyticsClient(graph_id=graph_id, log_formatting=get_log_formatting(kwargs), config=json.dumps(config))
        else:
            return None
            
class NeptuneDatabaseGraphStoreFactory(GraphStoreFactoryMethod):
    """Factory for creating Neptune database graph store instances.

    Provides the implementation for creating instances of NeptuneDatabaseClient based
    on the provided graph information and additional configurations. This factory parses
    the graph endpoint from the given graph information and constructs the client accordingly.

    Attributes:
        None
    """
    def try_create(self, graph_info:str, **kwargs) -> GraphStore:
        """
        Attempts to create a GraphStore instance for a Neptune database based on the provided
        graph information. It extracts or processes the graph endpoint from the given
        `graph_info` and optionally leverages additional configurations provided through
        keyword arguments.

        Args:
            graph_info (str): Information or identifier for the graph database. This could
                be a URL or a string containing elements allowing identification of a
                Neptune database endpoint.
            **kwargs: Additional arguments to customize the connection and configuration
                for the generated Neptune database client. Supported arguments include:
                - endpoint_url: Explicit endpoint URL to use for the database connection.
                - port: Port number to use for the connection if not embedded in the
                  `graph_endpoint`. Defaults to 8182.
                - config: A dictionary of configuration key-value pairs to control specifics
                  of the database client's behavior.

        Returns:
            GraphStore: A NeptuneDatabaseClient instance configured for the provided
            `graph_info` and `**kwargs`, or `None` if the `graph_info` does not lead to a
            valid endpoint.
        """
        graph_endpoint = None

        if graph_info.startswith(NEPTUNE_DATABASE):
            graph_endpoint = graph_info[len(NEPTUNE_DATABASE):]
        elif graph_info.endswith(NEPTUNE_DB_DNS):
            graph_endpoint = graph_info
        elif NEPTUNE_DB_DNS in graph_info:
            graph_endpoint = graph_info.replace('https://', '')

        if graph_endpoint:
            logger.debug(f'Opening Neptune database [endpoint: {graph_endpoint}]')
            endpoint_url = kwargs.pop('endpoint_url', None)
            port = kwargs.pop('port', 8182)
            if not endpoint_url:
                endpoint_url = f'https://{graph_endpoint}' if ':' in graph_endpoint else f'https://{graph_endpoint}:{port}'
            config = kwargs.pop('config', {})
            return NeptuneDatabaseClient(endpoint_url=endpoint_url, log_formatting=get_log_formatting(kwargs), config=json.dumps(config))
        else:
            return None
            
class NeptuneAnalyticsClient(GraphStore):
    """
    Represents a client for interacting with an Amazon Neptune graph database.

    Provides functionality to execute queries and handle operations specific to
    Neptune's OpenCypher API. It also offers utility methods for node ID formatting
    and property assignment customization.

    Attributes:
        graph_id (str): The identifier for the graph being accessed.
        config (Optional[str]): Optional configuration details for the client.
        _client (Optional[Any]): Internal attribute to manage the Neptune client
            instance. Initialized lazily when accessed via the `client` property.
    """
    graph_id: str
    config : Optional[str] = None
    _client: Optional[Any] = PrivateAttr(default=None)
        
    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    @property
    def client(self):
        """
        The `client` property provides access to the Neptune Graph API client. It initializes
        the client lazily upon the first request, ensuring that the session and configuration
        are properly set up. This encapsulation allows deferred initialization and prevents
        unnecessary setup unless the client is required.

        Attributes:
            _client (Optional[Any]): Stores the initialized Neptune Graph API client.

        Returns:
            Any: The initialized Neptune Graph API client object.
        """
        if self._client is None:
            session = GraphRAGConfig.session
            self._client = session.client('neptune-graph', config=create_config(self.config))
        return self._client
    
    def node_id(self, id_name:str) -> NodeId:
        """
        Formats an identifier for compatibility with Neptune.

        This function processes the provided identifier into the format required
        by Neptune, enabling smooth handling and identification of nodes.

        Args:
            id_name: The identifier string to be formatted.

        Returns:
            NodeId: The formatted identifier as a NodeId type, ready for use with
            Neptune.
        """
        return format_id_for_neptune(id_name)
    
    def property_assigment_fn(self, key:str, value:Any) -> Callable[[str], str]:
        """
        Assigns a property value for a Neptune property system using the provided key and value.

        This function creates a callable function that assigns a property value in a Neptune-based
        system by leveraging the given key and value. The result of the function is intended to
        help simplify interactions with Neptune’s property system by wrapping the implementation
        details into a callable template.

        Args:
            key (str): The property key to assign the value to. Must be provided as a string.
            value (Any): The value to be assigned to the key, where the type depends on the specific
                requirements of the Neptune system.

        Returns:
            Callable[[str], str]: A callable function that implements the specified property
            assignment operation based on the provided key and value.
        """
        return create_property_assigment_fn_for_neptune(key, value)
 
    def _execute_query(self, cypher, parameters={}, correlation_id=None):
        """
        Executes a Cypher query against the database, with the ability to log the request and response,
        including the execution time. This method is a wrapper over the database client execution,
        providing additional formatting and debug logging for traceability.

        Args:
            cypher (str): The Cypher query string to be executed.
            parameters (dict, optional): The parameters to pass along with the Cypher query. Defaults to an empty dictionary.
            correlation_id (str, optional): An identifier to associate the request with a specific operation or context. Defaults to None.

        Returns:
            list: A list of results obtained by executing the query. Each result is parsed as a JSON object.

        """
        query_id = uuid.uuid4().hex[:5]

        request_log_entry_parameters = self.log_formatting.format_log_entry(
            self._logging_prefix(query_id, correlation_id), 
            cypher, 
            parameters
        )

        logger.debug(f'[{request_log_entry_parameters.query_ref}] Query: [query: {request_log_entry_parameters.query}, parameters: {request_log_entry_parameters.parameters}]')

        start = time.time()
        
        response =  self.client.execute_query(
            graphIdentifier=self.graph_id,
            queryString=request_log_entry_parameters.format_query_with_query_ref(cypher),
            parameters=parameters,
            language='OPEN_CYPHER',
            planCache='DISABLED'
        )

        end = time.time()

        results = json.loads(response['payload'].read())['results']

        if logger.isEnabledFor(logging.DEBUG):
            response_log_entry_parameters = self.log_formatting.format_log_entry(
                self._logging_prefix(query_id, correlation_id), 
                cypher, 
                parameters, 
                results
            )
            logger.debug(f'[{response_log_entry_parameters.query_ref}] {int((end-start) * 1000)}ms Results: [{response_log_entry_parameters.results}]')
    
        return results
    
class NeptuneDatabaseClient(GraphStore):
    """
    A client implementation for interacting with an Amazon Neptune graph database.

    This class provides functionalities to interact with an Amazon Neptune graph
    database in a structured way. It allows executing queries using OpenCypher,
    managing nodes and their properties, and more. The client handles setting up
    connections and formatting requests, making it easier to perform graph database
    operations. It also includes functionality for detailed logging of queries and
    their responses.

    Attributes:
        endpoint_url (str): The endpoint URL of the Neptune database to connect to.
        config (Optional[str]): Configuration settings for the Neptune client
            session, if needed.
        _client (Optional[Any]): Internal attribute, managed privately, used to
            hold the initialized Neptune client session.
    """
    endpoint_url: str
    config : Optional[str] = None
    _client: Optional[Any] = PrivateAttr(default=None)
        
    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    @property
    def client(self):
        """
        Provides a property to access the `_client` attribute, initializing it if
        necessary. The `_client` is created using the `neptunedata` service, where
        the `endpoint_url` and configuration are supplied dynamically.

        Attributes:
            _client (Any): The underlying client instance for interacting with the
                `neptunedata` service. It is initialized lazily when the property
                is accessed.
            endpoint_url (str): The endpoint URL for the `neptunedata` service.
            config (dict): Configuration options required for creating the client.

        Returns:
            Any: A client object set up to interact with the Neptune Data service.
        """
        if self._client is None:
            session = GraphRAGConfig.session
            self._client = session.client(
                'neptunedata',
                endpoint_url=self.endpoint_url,
                config=create_config(self.config)
            )
        return self._client

    def node_id(self, id_name:str) -> NodeId:
        """
        Formats the given identifier into a NodeId compatible format for Neptune.

        Args:
            id_name (str): The identifier string to format.

        Returns:
            NodeId: The formatted NodeId object.
        """
        return format_id_for_neptune(id_name)
    
    def property_assigment_fn(self, key:str, value:Any) -> Callable[[str], str]:
        """
        Creates and returns a property assignment function which assigns a given value
        to a specific key. The returned function will perform the assignment when
        invoked.

        Args:
            key (str): The key to which the value would be assigned.
            value (Any): The value to assign to the provided key.

        Returns:
            Callable[[str], str]: A function that takes a single string argument and
            performs the assignment with respect to the specified key and value.
        """
        return create_property_assigment_fn_for_neptune(key, value)

    def _execute_query(self, cypher, parameters={}, correlation_id=None):
        """
        Executes an openCypher query using the provided query string and parameters,
        logs the query and its results for debugging purposes, and measures the
        execution time in milliseconds.

        Args:
            cypher (str): The openCypher query string to be executed.
            parameters (dict, optional): A dictionary of parameters to bind to the
                query. Defaults to an empty dictionary.
            correlation_id (str, optional): An optional correlation ID for tracking
                queries across services. Defaults to None.

        Returns:
            list: The results of the executed query as returned by the query response.
        """
        query_id = uuid.uuid4().hex[:5]
        
        params = json.dumps(parameters)

        request_log_entry_parameters = self.log_formatting.format_log_entry(
            self._logging_prefix(query_id, correlation_id), 
            cypher, 
            params
        )

        logger.debug(f'[{request_log_entry_parameters.query_ref}] Query: [query: {request_log_entry_parameters.query}, parameters: {request_log_entry_parameters.parameters}]')

        start = time.time()

        response =  self.client.execute_open_cypher_query(
            openCypherQuery=request_log_entry_parameters.format_query_with_query_ref(cypher),
            parameters=params
        )

        end = time.time()

        results = response['results']

        if logger.isEnabledFor(logging.DEBUG):
            response_log_entry_parameters = self.log_formatting.format_log_entry(
                self._logging_prefix(query_id, correlation_id), 
                cypher, 
                parameters, 
                results
            )
            logger.debug(f'[{response_log_entry_parameters.query_ref}] {int((end-start) * 1000)}ms Results: [{response_log_entry_parameters.results}]')
        
        return results