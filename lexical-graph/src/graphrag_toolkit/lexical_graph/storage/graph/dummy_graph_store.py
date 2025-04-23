# Copyright FalkorDB.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from graphrag_toolkit.lexical_graph.storage.graph import GraphStoreFactoryMethod, GraphStore, get_log_formatting


DUMMY = 'dummy://'

logger = logging.getLogger(__name__)

class DummyGraphStoreFactory(GraphStoreFactoryMethod):

    def try_create(self, graph_info:str, **kwargs) -> GraphStore:

        if graph_info.startswith(DUMMY):
            logger.debug('Opening dummy graph store')
            return DummyGraphStore(log_formatting=get_log_formatting(kwargs))
        else:
            return None
        

class DummyGraphStore(GraphStore):
    def execute_query(self, cypher, parameters={}, correlation_id=None):  
        log_entry_parameters = self.log_formatting.format_log_entry(self._logging_prefix(correlation_id), cypher, parameters)
        logger.debug(f'[{log_entry_parameters.query_ref}] query: {log_entry_parameters.query}, parameters: {log_entry_parameters.parameters}')
        return []

