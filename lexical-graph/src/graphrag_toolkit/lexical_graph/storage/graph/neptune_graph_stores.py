# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import logging
import time
import uuid
from botocore.config import Config
from typing import Optional, Any

from graphrag_toolkit.lexical_graph.storage.graph import GraphStoreFactoryMethod, GraphStore, NodeId, get_log_formatting
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from llama_index.core.bridge.pydantic import PrivateAttr

NEPTUNE_ANALYTICS = 'neptune-graph://'
NEPTUNE_DATABASE = 'neptune-db://'
NEPTUNE_DB_DNS = 'neptune.amazonaws.com'

logger = logging.getLogger(__name__)

def format_id_for_neptune(id_name:str):
        parts = id_name.split('.')
        if len(parts) == 1:
            return NodeId(parts[0], '`~id`', False)           
        else:
            return NodeId(parts[1], f'id({parts[0]})', False)
        
def create_config(config:Optional[str]=None):
    config_args = {}
    if config:
        config_args = json.loads(config)
    return Config(
        retries={
            'total_max_attempts': 1, 
            'mode': 'standard'
        }, 
        read_timeout=600,
        **config_args
    )

class NeptuneAnalyticsGraphStoreFactory(GraphStoreFactoryMethod):

    def try_create(self, graph_info:str, **kwargs) -> GraphStore:

        if graph_info.startswith(NEPTUNE_ANALYTICS):

            graph_id = graph_info[len(NEPTUNE_ANALYTICS):]
            config = kwargs.pop('config', {})

            logger.debug(f'Opening Neptune Analytics graph [graph_id: {graph_id}]')
            return NeptuneAnalyticsClient(graph_id=graph_id, log_formatting=get_log_formatting(kwargs), config=json.dumps(config))
        else:
            return None
            
class NeptuneDatabaseGraphStoreFactory(GraphStoreFactoryMethod):

    def try_create(self, graph_info:str, **kwargs) -> GraphStore:
         
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
    
    graph_id: str
    config : Optional[str] = None
    _client: Optional[Any] = PrivateAttr(default=None)
        
    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    @property
    def client(self):
        if self._client is None:
            session = GraphRAGConfig.session
            self._client = session.client('neptune-graph', config=create_config(self.config))
        return self._client
    
    def node_id(self, id_name:str) -> NodeId:
        return format_id_for_neptune(id_name)
 
    def execute_query(self, cypher, parameters={}, correlation_id=None):

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
            
    endpoint_url: str
    config : Optional[str] = None
    _client: Optional[Any] = PrivateAttr(default=None)
        
    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    @property
    def client(self):
        if self._client is None:
            session = GraphRAGConfig.session
            self._client = session.client(
                'neptunedata',
                endpoint_url=self.endpoint_url,
                config=create_config(self.config)
            )
        return self._client

    def node_id(self, id_name:str) -> NodeId:
        return format_id_for_neptune(id_name)

    def execute_query(self, cypher, parameters={}, correlation_id=None):

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