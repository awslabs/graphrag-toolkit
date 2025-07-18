# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Dict, Any, Optional, Callable

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.storage.constants import LEXICAL_GRAPH_LABELS
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, NodeId

class MultiTenantGraphStore(GraphStore):
    """
    Represents a multi-tenant graph store.

    This class serves as a wrapper for a GraphStore to support multi-tenancy. It modifies
    queries and operations to be specific to the tenant identified by a tenant ID. The
    class ensures that graph operations are isolated and specific to the tenant, providing
    effective multi-tenant handling for graph data.

    Attributes:
        inner (GraphStore): The underlying graph store being wrapped to enable multi-tenancy.
        labels (List[str]): A list of graph labels that might be rewritten for the tenant.
    """
    @classmethod
    def wrap(cls, graph_store:GraphStore, tenant_id:TenantId, labels:List[str]=LEXICAL_GRAPH_LABELS):
        """
        Creates and returns a GraphStore instance, which can appropriately handle multi-tenancy
        based on the tenant information provided. If the tenant is a default tenant, the original
        graph store instance is returned as is. Otherwise, ensures that a MultiTenantGraphStore
        instance is created and returned.

        Args:
            graph_store (GraphStore): The graph store instance to be wrapped for multi-tenancy.
            tenant_id (TenantId): The tenant identifier used to determine if multi-tenancy is
                required.
            labels (List[str]): A list of labels associated with the graph store. Defaults to
                `LEXICAL_GRAPH_LABELS`.

        Returns:
            GraphStore: The resulting graph store instance, either as-is or wrapped as a
                MultiTenantGraphStore depending on tenant information.
        """
        
        if isinstance(graph_store, MultiTenantGraphStore):
            return graph_store
        return MultiTenantGraphStore(inner=graph_store, tenant_id=tenant_id, labels=labels)
    
    inner:GraphStore
    labels:List[str]=[]

    def execute_query_with_retry(self, query:str, parameters:Dict[str, Any], max_attempts=3, max_wait=5, **kwargs):
        """
        Executes a query with retry logic, ensuring the query is attempted multiple
        times in case of failure. This method uses the `inner` object's
        `execute_query_with_retry` method to perform the actual query, after rewriting
        the provided query string if necessary.

        Args:
            query: A string representing the SQL query to be executed.
            parameters: A dictionary containing the parameters to be bound to the query.
            max_attempts: An optional integer specifying the maximum number of retry
                attempts. Defaults to 3.
            max_wait: An optional integer specifying the maximum wait time in seconds
                between retries. Defaults to 5.
            **kwargs: Additional optional keyword arguments to be passed to the
                `execute_query_with_retry` method of the `inner` object.
        """
        return self.inner.execute_query_with_retry(query=self._rewrite_query(query), parameters=parameters, max_attempts=max_attempts, max_wait=max_wait)

    def _logging_prefix(self, query_id:str, correlation_id:Optional[str]=None):
        """
        Generates a logging prefix based on given `query_id` and optional
        `correlation_id`.

        This method delegates the functionality to the `inner` attribute's
        `_logging_prefix` method. The prefix is typically used for logging
        or debugging purposes to include contextual information.

        Args:
            query_id (str): A unique identifier for the query to include in
                the logging prefix.
            correlation_id (Optional[str]): An optional identifier that links
                related queries or requests for additional context in log
                messages.

        Returns:
            str: The generated logging prefix containing the provided identifiers.
        """
        return self.inner._logging_prefix(query_id=query_id, correlation_id=correlation_id)
    
    def property_assigment_fn(self, key:str, value:Any) -> Callable[[str], str]:
        """
        Assigns a value to a property of an inner object and returns a callable function
        to retrieve a property value as a string. This method delegates the assignment
        to an inner object's function.

        Args:
            key: The name of the property to assign in the inner object.
            value: The value to assign to the specified property.

        Returns:
            A callable that takes a string parameter and returns a string representation
            of the requested property's value.
        """
        return self.inner.property_assigment_fn(key, value)
    
    def property_assigment_fn(self, key:str, value:Any) -> Callable[[str], str]:
        return self.inner.property_assigment_fn(key, value)
    
    def node_id(self, id_name:str) -> NodeId:
        """
        Retrieves a unique node identifier (NodeId) based on the provided node name.

        This function interacts with an internal structure to map the given node name
        to its corresponding unique identifier.

        Args:
            id_name (str): The name of the node for which the identifier is being
                retrieved.

        Returns:
            NodeId: The unique identifier associated with the specified node name.
        """
        return self.inner.node_id(id_name=id_name)
    
    def _execute_query(self, cypher:str, parameters={}, correlation_id=None) -> Dict[str, Any]:
        """
        Executes a database query with the given cypher query string, parameters,
        and an optional correlation ID for tracking purposes.

        Args:
            cypher: The cypher query string to execute.
            parameters: A dictionary containing the parameters for the query. Default
                is an empty dictionary.
            correlation_id: An optional identifier used for correlating and tracking
                the query execution. Default is None.

        Returns:
            A dictionary containing the query results or metadata.

        """
        return self.inner._execute_query(cypher=self._rewrite_query(cypher), parameters=parameters, correlation_id=correlation_id)
    
    def _rewrite_query(self, cypher:str):
        """
        Rewrites the given Cypher query to replace instance-specific labels with their tenant-specific
        versions based on the tenant ID. The method processes all labels and updates the query accordingly.

        Args:
            cypher (str): The original Cypher query to be rewritten.

        Returns:
            str: The rewritten Cypher query with tenant-specific labels.
        """
        if self.tenant_id.is_default_tenant():
            return cypher
        for label in self.labels:
            original_label = f'`{label}`'
            new_label = self.tenant_id.format_label(label)
            cypher = cypher.replace(original_label, new_label)
        return cypher
    