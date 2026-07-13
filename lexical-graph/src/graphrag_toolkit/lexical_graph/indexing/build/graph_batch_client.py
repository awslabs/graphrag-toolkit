# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Dict, Any, List, Callable
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, Query, QueryTree

BATCH_MAX_ATTEMPTS = 10
BATCH_MAX_WAIT = 7

logger = logging.getLogger(__name__)

class GraphBatchClient():
    """
    Handles batched operations with a graph store client.

    GraphBatchClient is designed for handling operations in batches to optimize performance
    when interacting with a GraphStore. It enables batching of writes and efficient processing
    of graph queries, with support for retrying queries.

    Attributes:
        graph_client (GraphStore): The underlying graph store client used for executing
            queries and node operations.
        batch_writes_enabled (bool): Flag indicating whether batch writes are enabled.
        batch_write_size (int): The maximum number of entries in a batch.
        batches (dict): A mapping of queries to their associated batched parameters.
        all_nodes (list): A collection of all nodes processed for yielding.
    """
    def __init__(self, graph_client:GraphStore, batch_writes_enabled:bool, batch_write_size:int):
        """
        Initializes an instance of the class that manages the graph client and facilitates
        batch processing of operations, such as writes. This class maintains a reference
        to a graph client, enables or disables batch writes, and configures batch write
        sizes. Additionally, it holds data structures for managing batched operations
        and tracking all nodes processed.

        Args:
            graph_client: The graph client instance used for interacting with the graph
                store.
            batch_writes_enabled: A boolean flag indicating whether batch writing is
                enabled or not.
            batch_write_size: The number of items to include in a single batch when
                performing batch operations.
        """
        self.graph_client = graph_client
        self.batch_writes_enabled = batch_writes_enabled
        self.batch_write_size = batch_write_size
        self.batches:Dict[str, List] = {}
        self.batch_operations:Dict[str, Any] = {}
        self.query_trees:Dict[str, QueryTree] = {}
        self.all_nodes = []
        self.parameterless_queries:Dict[str, str] = {}

    @property
    def tenant_id(self):
        """
        Getter method for retrieving the tenant ID associated with the graph client.

        The tenant ID identifies the tenant to which the current graph client is bound.

        Returns:
            str: The tenant ID of the associated graph client.
        """
        return self.graph_client.tenant_id

    def node_id(self, id_name:str):
        """
        Fetches the node ID by a given name using the graph client.

        This function utilizes the `graph_client` to retrieve the unique node ID
        associated with the provided name. It acts as a proxy to the `node_id()`
        method of the `graph_client`.

        Args:
            id_name (str): The name of the node for which the ID is requested.

        Returns:
            Any: The ID of the node as returned by the `graph_client`.
        """
        return self.graph_client.node_id(id_name)
    
    def property_assigment_fn(self, key:str, value:Any) -> Callable[[str], str]:
        """
        Assigns a property to a specified key and returns a function to retrieve the property.

        Args:
            key (str): The key to which the property will be assigned.
            value (Any): The value of the property to be assigned.

        Returns:
            Callable[[str], str]: A function that takes a key and retrieves the assigned property
            value as a string.
        """
        return self.graph_client.property_assigment_fn(key, value)
    
    def _add_parameterless_query(self, query):
        parts = query.split(' // awsqid:')
        if len(parts) < 2:
            raise ValueError(f'Error in parameterless query - expected query id: {query}')
        query_str = parts[0]
        q_id = parts[1]
        if q_id not in self.parameterless_queries:
            self.parameterless_queries[q_id] = query_str

    
    def execute_query_with_retry(self, query:QueryTree, properties:Dict[str, Any], **kwargs):
        """
        Executes a query with retry logic. Supports batch processing of queries if
        batch writes are enabled. When batch writes are enabled, properties are grouped
        and stored in a batch for the given query. Otherwise, the query is executed
        immediately with the provided properties and additional arguments.

        Args:
            query: The query string to be executed against the database.
            properties: A dictionary containing parameters or other properties required
                for executing the query. Must include 'params' if batching is enabled.
            **kwargs: Arbitrary keyword arguments that may affect query execution.
        """
        if not self.batch_writes_enabled:
            self.graph_client.execute_query_with_retry(query, properties, **kwargs)
        else:
            if isinstance(query, str):
                if properties:
                    if query not in self.batches:
                        self.batches[query] = []
                        self.batch_operations[query] = kwargs.get('operation')
                    self.batches[query].extend(properties['params'])
                elif kwargs.get('operation') is None:
                    self._add_parameterless_query(query)
            elif isinstance(query, QueryTree):
                properties = properties or {'params':[]}
                if query.id not in self.batches:
                    self.batches[query.id] = []
                    self.query_trees[query.id] = query
                self.batches[query.id].extend(properties['params'])
            else:
                raise ValueError(f'Invalid query type. Expected string or Query Tree but received {type(query).__name__}.')

    def allow_yield(self, node):
        """
        Determines whether the given node should be processed immediately or added to a batch
        if batch writes are enabled.

        This function evaluates the system's current mode of handling nodes and either appends
        the node to a pending batch queue when batch writes are enabled or permits immediate
        processing. If batch writes are enabled, the `all_nodes` list is updated with the
        given node. The function determines and returns whether yielding the node is allowed.

        Args:
            node: The node to process, either by immediate evaluation or batching, depending
                on the `batch_writes_enabled` state.

        Returns:
            bool: False if the node is added to the batch queue (batch write mode is enabled),
            True otherwise, allowing the node to be processed immediately.
        """
        if self.batch_writes_enabled:
            self.all_nodes.append(node)
            return False
        else:
            return True
        
    def _apply_parameterless_queries(self):

        parameterless_queries = list(self.parameterless_queries.values())
        parameterless_batch_write_size = min(25, self.batch_write_size)

        parameterless_query_batches = [
            parameterless_queries[x:x+parameterless_batch_write_size] 
            for x in range(0, len(parameterless_queries), parameterless_batch_write_size)
        ]

        for parameterless_query_batch in parameterless_query_batches:
            parameterless_query_batch = ['// parameterless queries'] + parameterless_query_batch
            query = '\n'.join(parameterless_query_batch)
            self.graph_client.execute_query_with_retry(query, {}, max_attempts=BATCH_MAX_ATTEMPTS, max_wait=BATCH_MAX_WAIT)

    def _apply_batch_query(self, query, parameters):

        retry_batches = []

        deduped_parameters = self._dedup(parameters)
        parameter_chunks = [
            deduped_parameters[x:x+self.batch_write_size] 
            for x in range(0, len(deduped_parameters), self.batch_write_size)
        ]

        for p in parameter_chunks:
            params = {
                'params': p
            }
            try:
                self._execute_batch(query, params)
            except Exception as e:
                logger.debug(f'Batch failed - queuing for retry: [query: {query}, params: {params}]')
                retry_batches.append((query, params))

        failed_batches = []

        for (query, params) in retry_batches:
            try:
                self._execute_batch(query, params)
            except Exception as e:
                logger.debug(f'Retry batch failed - queuing for return: [query: {query}, params: {params}]')
                failed_batches.append((query, params))

        return failed_batches

    def _execute_batch(self, query, parameters):
        return self.graph_client.execute_query_with_retry(
            query,
            parameters,
            max_attempts=BATCH_MAX_ATTEMPTS,
            max_wait=BATCH_MAX_WAIT,
            operation=self.batch_operations.get(query),
        )
    
    def _retry_failed_batches(self, failed_batches):

        last_chance_batches = []

        for (query, params) in failed_batches:
            try:
                self._execute_batch(query, params)
            except Exception as e:
                logger.debug(f'Retry failed batch failed - queuing for individual writes retry: [query: {query}, params: {params}]')
                last_chance_batches.append((query, params))

        for (query, params) in last_chance_batches:
            logger.debug('Splitting failed batch into individual writes retrying')
            failed_params = params['params']
            for p in failed_params:
                single_params = {
                    'params': [p]
                }
                try:
                    self._execute_batch(query, single_params)
                except Exception as e:
                    logger.error(f'Failed single write: [query: {query}, params: {params}, error: {str(e)}]') 
                    raise e    


    def _apply_batch_query_tree(self, query_tree_id, parameters):

        query_tree = self.query_trees[query_tree_id]

        def graph_store_op(q, p, **kwargs):

            all_params = p['params']

            parameter_chunks = [
                all_params[x:x+self.batch_write_size] 
                for x in range(0, len(all_params), self.batch_write_size)
            ]

            for chunk in parameter_chunks:
                
                params = {
                    'params': chunk
                }

                results = self.graph_client.execute_query_with_retry(q, params, max_attempts=BATCH_MAX_ATTEMPTS, max_wait=BATCH_MAX_WAIT, **kwargs)

                for r in results:
                    yield r

        for r in query_tree.run(parameters, graph_store_op):
            continue
        
    def apply_batch_operations(self):
        """
        Executes batch operations by processing stored queries and parameters, deduplicating
        them, and executing the queries in chunks according to the defined batch size.

        Executes queries in retries to handle transient errors, ensuring robust and reliable
        execution. Returns the resulting nodes from the performed operations.

        Raises:
            Any exceptions raised during the execution of queries are managed internally
            and retried up to the specified maximum attempts.

        Returns:
            list: A list of all nodes resulting from the operations.
        """

        failed_batches = []

        for query, parameters in self.batches.items():

            if query.startswith('query-tree-'):
                self._apply_batch_query_tree(query, parameters)
            else:
                failed_batches.extend(self._apply_batch_query(query, parameters))

        self._apply_parameterless_queries()

        self._retry_failed_batches(failed_batches)

        return self.all_nodes
  
    def _dedup(self, parameters:List):
        """
        Removes duplicate entries from the input list based on case-insensitive string
        representation and maintains the last occurrence of each unique entry.

        Args:
            parameters (List): A list of elements which may contain duplicates.

        Returns:
            List: A list containing unique elements from the input, preserving the last
            occurrence order.
        """
        params_map = {}
        for p in parameters:
            params_map[str(p).lower()] = p
        return list(params_map.values())
    
    def __enter__(self):
        """
        Handles the setup operations for a context manager. This method is invoked
        automatically when the context manager is entered using the `with` statement,
        and it ensures that any necessary initialization logic for the context is
        executed.

        Returns:
            self: Returns the context manager instance to enable usage within the
            `with` statement.
        """
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Handles the exit of a context manager by suppressing exceptions or performing cleanup
        operations when the context is exited.

        Args:
            exception_type: The type of exception class that was raised, if any. If no
                exception was raised, this value will be None.
            exception_value: The value of the exception that was raised, if any. If no
                exception was raised, this value will be None.
            exception_traceback: The traceback object associated with the raised
                exception, if any. If no exception was raised, this value will be None.
        """
        pass

    