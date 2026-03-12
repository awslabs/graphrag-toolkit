# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from graphrag_toolkit.lexical_graph.storage.graph import GraphStoreFactoryMethod, GraphStore, get_log_formatting
from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient

logger = logging.getLogger(__name__)

NEO4J_SCHEMES = ['bolt', 'bolt+ssc', 'bolt+s', 'neo4j', 'neo4j+ssc', 'neo4j+s'] 

class Neo4jGraphStoreFactory(GraphStoreFactoryMethod):

    def try_create(self, graph_info:str, **kwargs) -> GraphStore:  # pragma: no cover
        endpoint_url = None
        for scheme in NEO4J_SCHEMES:
            if graph_info.startswith(f'{scheme}://'):
                endpoint_url = graph_info
                break    
        if endpoint_url:
            logger.debug(f'Opening Neo4j database [endpoint: {endpoint_url}]')
            return Neo4jDatabaseClient(
                endpoint_url=endpoint_url,
                log_formatting=get_log_formatting(kwargs), 
                **kwargs
            )
        else:
            return None