# Copyright FalkorDB.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from graphrag_toolkit.lexical_graph.storage.graph import GraphStoreFactoryMethod, GraphStore, get_log_formatting

DUMMY = 'dummy://'

logger = logging.getLogger(__name__)


class DummyGraphStoreFactory(GraphStoreFactoryMethod):
    """
    Factory class for creating instances of DummyGraphStore if applicable.

    This class implements a factory method pattern to create a DummyGraphStore
    object based on provided graph information. It attempts to determine whether
    the provided information corresponds to a dummy graph store, and if so,
    returns a new instance of DummyGraphStore. Otherwise, it returns None.

    Attributes:
        No additional class attributes are explicitly defined beyond inherited attributes.
    """
    def try_create(self, graph_info: str, **kwargs) -> GraphStore:
        """
        Attempts to create a `GraphStore` instance based on the provided `graph_info`.
        If `graph_info` starts with the constant `DUMMY`, a `DummyGraphStore` instance
        is initialized and returned. Otherwise, the method returns `None`.

        Args:
            graph_info (str): Information specifying the type of the graph store to
                create. If the value starts with `DUMMY`, a dummy graph store is opened.
            **kwargs: Additional keyword arguments used for configuring the graph store,
                such as formatting for logs.

        Returns:
            GraphStore: A `DummyGraphStore` instance if `graph_info` starts with
            `DUMMY`. Otherwise, returns `None`.
        """
        if graph_info.startswith(DUMMY):
            logger.debug('Opening dummy graph store')
            return DummyGraphStore(log_formatting=get_log_formatting(kwargs))
        else:
            return None


class DummyGraphStore(GraphStore):
    """
    Represents a specialized graph store that extends the base functionality of GraphStore.

    This class is designed to execute Cypher queries on a graph database and log the query
    information for debugging purposes. It provides an implementation for executing queries with
    optional parameters and correlation IDs. The main use case for this class is to interact with
    graph databases, primarily for logging and debugging scenarios.

    Attributes:
        log_formatting (LogFormatter): An instance of LogFormatter used for formatting log entries.
        _logging_prefix (callable): A callable function or method responsible for generating the
            logging prefix based on the provided correlation ID.
    """
    def _execute_query(self, cypher, parameters={}, correlation_id=None):
        """
        Executes the given Cypher query with specified parameters and logs the operation.

        The function logs a formatted version of the Cypher query and its parameters with
        a correlation identifier for tracking. It provides an empty result as a placeholder.

        Args:
            cypher: The Cypher query to be executed.
            parameters: A dictionary representing the parameters for the Cypher query.
                Defaults to an empty dictionary.
            correlation_id: An optional identifier for correlating log entries. Defaults
                to None.

        Returns:
            A list as a placeholder for query execution results. Currently, it does
            not retrieve any actual results.
        """
        log_entry_parameters = self.log_formatting.format_log_entry(self._logging_prefix(correlation_id), cypher,
                                                                    parameters)
        logger.debug(
            f'[{log_entry_parameters.query_ref}] query: {log_entry_parameters.query}, parameters: {log_entry_parameters.parameters}')
        return []
