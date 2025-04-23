import logging
from typing import List, Union
from falkordb.node import Node
from falkordb.edge import Edge
from falkordb.path import Path

from graphrag_toolkit.lexical_graph.storage.graph import GraphStoreFactoryMethod, GraphStore, get_log_formatting

logger = logging.getLogger(__name__)

FALKORDB = 'falkordb://'
FALKORDB_DNS = 'falkordb.com'
DEFAULT_DATABASE_NAME = 'graphrag'
QUERY_RESULT_TYPE = Union[List[List[Node]], List[List[List[Path]]], List[List[Edge]]]

class FalkorDBGraphStoreFactory(GraphStoreFactoryMethod):

    def try_create(self, graph_info:str, **kwargs) -> GraphStore:
        endpoint_url = None
        if graph_info.startswith(FALKORDB):
            endpoint_url = graph_info[len(FALKORDB):]
        elif graph_info.endswith(FALKORDB_DNS):
            endpoint_url = graph_info
        if endpoint_url:
            try:
                from graphrag_toolkit.lexical_graph.storage.graph.falkordb_graph_store import FalkorDBDatabaseClient
                logger.debug(f'Opening FalkorDB database [endpoint: {endpoint_url}]')
                return FalkorDBDatabaseClient(
                    endpoint_url=endpoint_url,
                    log_formatting=get_log_formatting(kwargs), 
                    **kwargs
                )
            except ImportError as e:
                raise e
            
        else:
            return None