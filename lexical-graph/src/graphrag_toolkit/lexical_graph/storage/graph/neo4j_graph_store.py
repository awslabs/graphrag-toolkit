# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import uuid
from typing import Optional, Any
from urllib.parse import urlparse

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, NodeId, format_id

from llama_index.core.bridge.pydantic import PrivateAttr

logger = logging.getLogger(__name__)

class Neo4jDatabaseClient(GraphStore):
    
    connection_string:str
    database:Optional[str]=None
    username:Optional[str]=None
    password:Optional[str]=None
    notifications_min_severity:str
        
    _client: Optional[Any] = PrivateAttr(default=None)

    
    def __init__(self,
                 endpoint_url:str =None,
                 host:str=None,
                 port:int=None,
                 database:str=None,
                 username:str=None,
                 password:str =None,
                 **kwargs
                 ) -> None:
        
        parsed = urlparse(endpoint_url)

        scheme = parsed.scheme
        database = parsed.path[1:] if parsed.path else database
        host = parsed.hostname or host
        port = parsed.port or port
        username = parsed.username or username
        password = parsed.password or password
        query = parsed.query

        if not host:
            raise ValueError("Host missing")
        
        if not port:
            raise ValueError("Port missing")

        if username and not password:
            raise ValueError("Password is required when username is provided")
        
        if database is not None and not database.isalnum():
            raise ValueError("Database name must be alphanumeric and non-empty")
        
        connection_string = f'{scheme}://{host}:{port}?{query}' if query else f'{scheme}://{host}:{port}'
                
        notifications_min_severity = kwargs.pop('notifications_min_severity', 'WARNING')

        super().__init__(
            connection_string=connection_string,
            database=database,
            username=username,
            password=password,
            notifications_min_severity=notifications_min_severity,
            **kwargs
        )

    def __getstate__(self):
        if self._client:
            self._client.close()
        self._client = None
        return super().__getstate__()
    
    @property
    def client(self):

        try:
            from neo4j import GraphDatabase
        except ImportError as e:
            raise ImportError(
                "Neo4j package not found, install with 'pip install neo4j'"
            ) from e

        if self._client is None:
            try:
                self._client = GraphDatabase.driver(
                    self.connection_string,
                    auth=(self.username, self.password),
                    connection_timeout=10.0,
                    notifications_min_severity=self.notifications_min_severity
                )
            except Exception as e:
                logger.error(f"Unexpected error while connecting to Neo4j: {e}")
                raise ConnectionError(f"Unexpected error while connecting to Neo4j: {e}") from e
        return self._client
        
    
    def node_id(self, id_name: str) -> NodeId:
        return format_id(id_name)
    
    def __exit__(self, exception_type, exception_value, traceback):
        if self._client:
            self._client.close()
        return super().__exit__(exception_type, exception_value, traceback)

    def _execute_query(self, 
                      cypher: str, 
                      parameters: Optional[dict] = None, 
                      correlation_id: Any = None):
 
        if parameters is None:
            parameters = {}

        query_id = uuid.uuid4().hex[:5]

        request_log_entry_parameters = self.log_formatting.format_log_entry(
            self._logging_prefix(query_id, correlation_id), 
            cypher, 
            parameters
        )

        logger.debug(f'[{request_log_entry_parameters.query_ref}] Query: [query: {request_log_entry_parameters.query}, parameters: {request_log_entry_parameters.parameters}]')

        start = time.time()

        with self.client.session() as session:
            if self.database:
                self.client.execute_query(
                    cypher, 
                    parameters,
                    database_=self.database
                )
            else:
                self.client.execute_query(
                    cypher, 
                    parameters
                )
            result = session.run(cypher, parameters)
            results = [record.data() for record in result]

        end = time.time()

        if logger.isEnabledFor(logging.DEBUG):
            response_log_entry_parameters = self.log_formatting.format_log_entry(
                self._logging_prefix(query_id, correlation_id), 
                cypher, 
                parameters, 
                results
            )
            logger.debug(f'[{response_log_entry_parameters.query_ref}] {int((end-start) * 1000)}ms Results: [{response_log_entry_parameters.results}]')
        
        return results
    
    def init(self, graph_store=None):
    
        graph_store = graph_store or self
        
        #search_str_constraint = f'CREATE CONSTRAINT entity_search_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Entity__`) REQUIRE n.search_str IS :: STRING'
        search_str_index = f'CREATE TEXT INDEX entity_text_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Entity__`) ON (n.search_str)'
        create_entity_index = f'CREATE INDEX entity_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Entity__`) ON (n.entityId)'
        create_fact_index = f'CREATE INDEX fact_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Fact__`) ON (n.factId)'
        create_statement_index = f'CREATE INDEX statement_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Statement__`) ON (n.statementId)'
        create_topic_index = f'CREATE INDEX topic_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Topic__`) ON (n.topicId)'
        create_chunk_index = f'CREATE INDEX chunk_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Chunk__`) ON (n.chunkId)'
        create_source_index = f'CREATE INDEX source_{graph_store.tenant_id} IF NOT EXISTS FOR (n:`__Source__`) ON (n.sourceId)'
       
        ops = [
            #search_str_constraint,
            search_str_index,
            create_entity_index,
            create_fact_index,
            create_statement_index,
            create_topic_index,
            create_chunk_index,
            create_source_index
        ]
        
        for op in ops:
            graph_store.execute_query_with_retry(op, {})
