# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any, List, Callable
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

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
        self.batches = {}
        self.all_nodes = []
        self.parameterless_queries = {}

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

    
    def execute_query_with_retry(self, query:str, properties:Dict[str, Any], **kwargs):
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
            if properties:
                if query not in self.batches:
                    self.batches[query] = []
                self.batches[query].extend(properties['params'])
            else:
                self._add_parameterless_query(query)

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
        for query, parameters in self.batches.items():

            deduped_parameters = self._dedup(parameters)
            parameter_chunks = [
                deduped_parameters[x:x+self.batch_write_size] 
                for x in range(0, len(deduped_parameters), self.batch_write_size)
            ]

            for p in parameter_chunks:
                params = {
                    'params': p
                }
                self.graph_client.execute_query_with_retry(query, params, max_attempts=5, max_wait=7)

        parameterless_queries = list(self.parameterless_queries.values())

        parameterless_query_batches = [
            parameterless_queries[x:x+self.batch_write_size] 
            for x in range(0, len(parameterless_queries), self.batch_write_size)
        ]

        for parameterless_query_batch in parameterless_query_batches:
            parameterless_query_batch = ['// parameterless queries'] + parameterless_query_batch
            query = '\n'.join(parameterless_query_batch)
            self.graph_client.execute_query_with_retry(query, {}, max_attempts=5, max_wait=7)

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

    