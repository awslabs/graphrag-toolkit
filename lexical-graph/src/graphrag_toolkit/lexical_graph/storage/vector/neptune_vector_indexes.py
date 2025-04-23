# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import string
import logging
from typing import Any, List

from graphrag_toolkit.lexical_graph import IndexError
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result
from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneAnalyticsClient
from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, VectorIndexFactoryMethod, to_embedded_query

from llama_index.core.indices.utils import embed_nodes
from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

NEPTUNE_ANALYTICS = 'neptune-graph://'

class NeptuneAnalyticsVectorIndexFactory(VectorIndexFactoryMethod):
    def try_create(self, index_names:List[str], vector_index_info:str, **kwargs) -> List[VectorIndex]:
        graph_id = None
        if vector_index_info.startswith(NEPTUNE_ANALYTICS):
            graph_id = vector_index_info[len(NEPTUNE_ANALYTICS):]
            logger.debug(f'Opening Neptune Analytics vector indexes [index_names: {index_names}, graph_id: {graph_id}]')
            return [NeptuneIndex.for_index(index_name, vector_index_info, **kwargs) for index_name in index_names]
        else:
            return None

class NeptuneIndex(VectorIndex):
    
    @staticmethod
    def for_index(index_name, graph_id, embed_model=None, dimensions=None):

        index_name = index_name.lower()
        neptune_client:GraphStore = GraphStoreFactory.for_graph_store(graph_id)
        embed_model = embed_model or GraphRAGConfig.embed_model
        dimensions = dimensions or GraphRAGConfig.embed_dimensions
        id_name = f'{index_name}Id'
        label = f'__{string.capwords(index_name)}__' 
        path = f'({index_name})'
        return_fields = node_result(index_name, neptune_client.node_id(f'{index_name}.{id_name}'))

        if index_name == 'chunk':
            path = '(chunk)-[:`__EXTRACTED_FROM__`]->(source:`__Source__`)'
            return_fields = f"source:{{sourceId: {neptune_client.node_id('source.sourceId')}, {node_result('source', key_name='metadata')}}},\n{node_result('chunk', neptune_client.node_id('chunk.chunkId'), [])}"
            
        return NeptuneIndex(
            index_name=index_name,
            neptune_client=neptune_client,
            embed_model=embed_model,
            dimensions=dimensions,
            id_name=id_name,
            label=label,
            path=path,
            return_fields=return_fields
        ) 


    neptune_client: NeptuneAnalyticsClient
    embed_model: Any
    dimensions: int
    id_name: str
    label: str
    path: str
    return_fields: str

    
    def add_embeddings(self, nodes):

        if not self.tenant_id.is_default_tenant():
            raise IndexError('NeptuneIndex does not support multi-tenant indexes')
                
        id_to_embed_map = embed_nodes(
            nodes, self.embed_model
        )
        
        for node in nodes:
        
            statement = f"MATCH (n:`{self.label}`) WHERE {self.neptune_client.node_id('n.{self.id_name}')} = $nodeId"
            
            embedding = id_to_embed_map[node.node_id]
            
            query = '\n'.join([
                statement,
                f'WITH n CALL neptune.algo.vectors.upsert(n, {embedding}) YIELD success RETURN success'
            ])
            
            properties = {
                'nodeId': node.node_id,
                'embedding': embedding
            }

            self.neptune_client.execute_query(query, properties)
        
        return nodes
    
    def add_embeddings_using_unwind(self, nodes):

        # Currently too slow
                
        id_to_embed_map = embed_nodes(
            nodes, self.embed_model
        )

        statements = [
            '// insert embeddings',
            'UNWIND $params AS params'
        ]

        statements.extend([
             f"MATCH (n:`{self.label}`) WHERE {self.neptune_client.node_id('n.{self.id_name}')} = params.nodeId",
             'WITH n, params.embedding AS embedding',
             'WITH n CALL neptune.algo.vectors.upsert(n, embedding) YIELD success RETURN success'
        ])

        params = []

        for node in nodes:
            params.append({
                'nodeId': node.node_id,
                'embedding': id_to_embed_map[node.node_id]
            })

        query = '\n'.join(statements)

        self.neptune_client.execute_query_with_retry(query, {'params': params})

        return nodes
    
    def top_k(self, query_bundle:QueryBundle, top_k:int=5):

        if not self.tenant_id.is_default_tenant():
            raise IndexError('NeptuneIndex does not support multi-tenant indexes')

        query_bundle = to_embedded_query(query_bundle, self.embed_model)

        cypher = f'''
        CALL neptune.algo.vectors.topKByEmbedding(
            {query_bundle.embedding},
            {{   
                topK: 10000,
                concurrency: 4
            }}
        )
        YIELD node, score       
        WITH node as {self.index_name}, score WHERE '{self.label}' in labels({self.index_name}) 
        WITH {self.index_name}, score ORDER BY score ASC LIMIT {top_k}
        MATCH {self.path}
        RETURN {{
            score: score,
            {self.return_fields}
        }} AS result ORDER BY result.score ASC LIMIT {top_k}
        '''
        results = self.neptune_client.execute_query(cypher)
        
        return [result['result'] for result in results]

    def get_embeddings(self, ids:List[str]=[]):

        if not self.tenant_id.is_default_tenant():
            raise IndexError('NeptuneIndex does not support multi-tenant indexes')
        
        all_results = []
        
        for i in ids:

            cypher = f'''
            MATCH (n:`{self.label}`)  WHERE {self.neptune_client.node_id('n.{self.id_name}')} = $elementId
            CALL neptune.algo.vectors.get(
                n
            )
            YIELD node, embedding       
            WITH node as {self.index_name}, embedding WHERE '{self.label}' in labels({self.index_name}) 
            MATCH {self.path}
            RETURN {{
                embedding: embedding,
                {self.return_fields}
            }} AS result
            '''
            
            params = {
                'elementId': i
            }
            
            results = self.neptune_client.execute_query(cypher, params)
            
            for result in results:
                all_results.append(result['result'])
        
        return all_results
