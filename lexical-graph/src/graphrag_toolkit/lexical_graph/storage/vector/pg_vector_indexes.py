# Copyright FalkorDB.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import numpy as np

from pgvector.psycopg2 import register_vector
from typing import List, Sequence, Dict, Any, Optional
from urllib.parse import urlparse

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig, EmbeddingType
from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, to_embedded_query
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

from llama_index.core.schema import BaseNode, QueryBundle
from llama_index.core.indices.utils import embed_nodes

logger = logging.getLogger(__name__)

try:
    import psycopg2
    from pgvector.psycopg2 import register_vector
    from psycopg2.errors import UniqueViolation, UndefinedTable
except ImportError as e:
    raise ImportError(
        "psycopg2 and/or pgvector packages not found, install with 'pip install psycopg2-binary pgvector'"
    ) from e

class PGIndex(VectorIndex):

    @staticmethod
    def for_index(index_name:str,
                  connection_string:str,
                  database='postgres',
                  schema_name='graphrag',
                  host:str='localhost',
                  port:int=5432,
                  username:str=None,
                  password:str=None,
                  embed_model:EmbeddingType=None,
                  dimensions:int=None,
                  enable_iam_db_auth=False):
        
        def compute_enable_iam_db_auth(s, default):
            if 'enable_iam_db_auth' in s.lower():
                return 'enable_iam_db_auth=true' in s.lower()
            else:
                return default
        
        parsed = urlparse(connection_string)

        database = parsed.path[1:] if parsed.path else database
        host = parsed.hostname or host
        port = parsed.port or port
        username = parsed.username or username
        password = parsed.password or password
        enable_iam_db_auth = compute_enable_iam_db_auth(parsed.query, enable_iam_db_auth)
        
        embed_model = embed_model or GraphRAGConfig.embed_model
        dimensions = dimensions or GraphRAGConfig.embed_dimensions

        return PGIndex(index_name=index_name, 
                       database=database, 
                       schema_name=schema_name,
                       host=host, 
                       port=port, 
                       username=username, 
                       password=password, 
                       dimensions=dimensions, 
                       embed_model=embed_model, 
                       enable_iam_db_auth=enable_iam_db_auth)

    index_name:str
    database:str
    schema_name:str
    host:str
    port:int
    username:str
    password:Optional[str]
    dimensions:int
    embed_model:EmbeddingType
    enable_iam_db_auth:bool=False
    initialized:bool=False

    def _get_connection(self):

        token = None

        if self.enable_iam_db_auth:
            client = GraphRAGConfig.rds  # via __getattr__
            region = GraphRAGConfig.aws_region
            token = client.generate_db_auth_token(
                DBHostname=self.host,
                Port=self.port,
                DBUsername=self.username,
                Region=region
            )

        password = token or self.password

        dbconn = psycopg2.connect(
            host=self.host,
            user=self.username, 
            password=password,
            port=self.port, 
            database=self.database,
            connect_timeout=30
        )

        dbconn.set_session(autocommit=True)

        if not self.initialized:

            cur = dbconn.cursor()

            try:

                register_vector(dbconn)

                if self.writeable:

                    try:
                        cur.execute(f'''CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.underlying_index_name()}(
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            {self.index_name}Id VARCHAR(255) unique,
                            value text,
                            metadata text,
                            embedding vector({self.dimensions})
                            );'''
                        )
                    except UniqueViolation:
                        # For alt approaches, see: https://stackoverflow.com/questions/29900845/create-schema-if-not-exists-raises-duplicate-key-error
                        logger.warning(f"UniqueViolation while trying to create table '{self.underlying_index_name()}'")
                        pass

                    index_name = f'{self.underlying_index_name()}_{self.index_name}Id_idx'
                    try:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {self.schema_name}.{self.underlying_index_name()} USING hash ({self.index_name}Id);')
                    except UniqueViolation:
                        logger.warning(f"UniqueViolation while trying to create index '{index_name}'")
                        pass

                    index_name = f'{self.underlying_index_name()}_{self.index_name}Id_idx'
                    try:
                        cur.execute(f'CREATE INDEX IF NOT EXISTS {self.underlying_index_name()}_embedding_idx ON {self.schema_name}.{self.underlying_index_name()} USING hnsw (embedding vector_l2_ops)')
                    except UniqueViolation:
                        logger.warning(f"UniqueViolation while trying to create index '{index_name}'")
                        pass
            finally:
                cur.close()

            self.initialized = True

        return dbconn


    def add_embeddings(self, nodes:Sequence[BaseNode]) -> Sequence[BaseNode]:

        if not self.writeable:
            raise IndexError(f'Index {self.index_name()} is read-only')

        dbconn = self._get_connection()
        cur = dbconn.cursor()

        try:

            id_to_embed_map = embed_nodes(
                nodes, self.embed_model
            )
            for node in nodes:
                cur.execute(
                    f'INSERT INTO {self.schema_name}.{self.underlying_index_name()} ({self.index_name}Id, value, metadata, embedding) SELECT %s, %s, %s, %s WHERE NOT EXISTS (SELECT * FROM {self.schema_name}.{self.underlying_index_name()} c WHERE c.{self.index_name}Id = %s);',
                    (node.id_, node.text,  json.dumps(node.metadata), id_to_embed_map[node.id_], node.id_)
                )

        except UndefinedTable as e:
            if self.tenant_id.is_default_tenant():
                raise e
            else:
                logger.warning(f'Multi-tenant index {self.underlying_index_name()} does not exist')

        finally:

            cur.close()
            dbconn.close()

        return nodes
    
    def _to_top_k_result(self, r):
        
        result = {
            'score': round(r[2], 7)
        }

        metadata = json.loads(r[1])

        if INDEX_KEY in metadata:
            index_name = metadata[INDEX_KEY]['index']
            result[index_name] = metadata[index_name]
            if 'source' in metadata:
                result['source'] = metadata['source']
        else:
            for k,v in metadata.items():
                result[k] = v
            
        return result
    
    def _to_get_embedding_result(self, r):
        
        id = r[0]
        value = r[1]
        metadata = json.loads(r[2])
        embedding = np.array(r[3]).tolist()

        result = {
            'id': id,
            'value': value,
            'embedding': embedding
        }

        for k,v in metadata.items():
            if k != INDEX_KEY:
                result[k] = v
            
        return result
    
    def top_k(self, query_bundle:QueryBundle, top_k:int=5) -> Sequence[Dict[str, Any]]:

        dbconn = self._get_connection()
        cur = dbconn.cursor()

        top_k_results = []

        try:

            query_bundle = to_embedded_query(query_bundle, self.embed_model)
        
            cur.execute(f'''SELECT {self.index_name}Id, metadata, embedding <-> %s AS score
                FROM {self.schema_name}.{self.underlying_index_name()}
                ORDER BY score ASC LIMIT %s;''',
                (np.array(query_bundle.embedding), top_k)
            )

            results = cur.fetchall()

            top_k_results.extend(
                [self._to_top_k_result(result) for result in results]
            )

        except UndefinedTable as e:
            logger.warning(f'Index {self.underlying_index_name()} does not exist')

        finally:
            cur.close()
            dbconn.close()

        return top_k_results

    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Dict[str, Any]]:
        
        dbconn = self._get_connection()
        cur = dbconn.cursor()

        def format_ids(ids):
            return ','.join([f"'{id}'" for id in ids])
        
        get_embeddings_results = []

        try:

            cur.execute(f'''SELECT {self.index_name}Id, value, metadata, embedding
                FROM {self.schema_name}.{self.underlying_index_name()}
                WHERE {self.index_name}Id IN ({format_ids(ids)});'''
            )

            results = cur.fetchall()

            get_embeddings_results.extend(
                [self._to_get_embedding_result(result) for result in results]
            )

        except UndefinedTable as e:
            logger.warning(f'Index {self.underlying_index_name()} does not exist')

        finally:
            cur.close()
            dbconn.close()

        return get_embeddings_results