# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List
from graphrag_toolkit.lexical_graph.indexing.extract.scoped_value_provider import ScopedValueStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

logger = logging.getLogger(__name__)

class GraphScopedValueStore(ScopedValueStore):
    """Manages and stores values in a graph database with scope-based organization.

    This class allows for storing and retrieving scoped values using a graph database.
    Scoped values are organized by a label and associated with a defined scope for easy
    management and retrieval.

    Attributes:
        graph_store (GraphStore): The graph database store used for executing queries.
    """
    graph_store: GraphStore

    def get_scoped_values(self, label:str, scope:str) -> List[str]:
        """
        Fetches distinct values associated with a specific label and scope from the graph database.

        This function performs a Cypher query to retrieve distinct values from nodes that match
        the specified label and scope in the graph database. The results are then extracted and
        returned as a list.

        Args:
            label (str): The label used to identify the nodes in the graph.
            scope (str): The scope value to filter nodes in the graph.

        Returns:
            List[str]: A list of distinct values associated with the input label and scope from
            the graph database.

        Raises:
            Any exceptions raised by the `execute_query` method within `self.graph_store` or
            any database-related issues will propagate to the caller.
        """
        cypher = f'''
        MATCH (n:`__SYS_SV__{label}__`)
        WHERE n.scope=$scope
        RETURN DISTINCT n.value AS value
        '''

        params = {
            'scope': scope
        }

        results = self.graph_store.execute_query_with_retry(cypher, params)

        return [result['value'] for result in results]

    def save_scoped_values(self, label:str, scope:str, values:List[str]) -> None:
        """
        Saves a list of values associated with a specific label and scope to the graph store. Each value is
        processed within the provided scope, and the method ensures a unique combination of scope and value
        through the `MERGE` operation in the query. The execution handles retries in case of query failure.

        Args:
            label (str): The label used to dynamically define the node label in the query. This allows for
                compartmentalization of values within the graph store.
            scope (str): A string defining the specific scope in which values will be stored. Used as an
                attribute to uniquely identify nodes and group values accordingly.
            values (List[str]): A list of string values to be stored in association with the provided scope
                and label.

        Returns:
            None
        """
        cypher = f'''
        UNWIND $values AS value
        MERGE (:`__SYS_SV__{label}__`{{scope:$scope, value:value}})
        '''

        params = {
            'scope': scope,
            'values': values
        }

        self.graph_store.execute_query_with_retry(cypher, params)

   

    

