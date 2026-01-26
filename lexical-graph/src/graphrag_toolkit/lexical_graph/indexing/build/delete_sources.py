# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Dict, Any, Callable, Generator
import concurrent.futures
import logging
import json

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.indexing import NodeHandler

from llama_index.core.schema import BaseComponent, BaseNode

logger = logging.getLogger(__name__)

class DeletePrevVersions(NodeHandler):

    lexical_graph:Any
    filter_fn:Callable[[Dict[str, Any]], bool]=lambda d: True

    def accept(self, nodes, **kwargs):

        for node in nodes:

            j = json.loads(node.to_json())
            metadata = j['metadata']

            if 'aws::graph::index' in metadata:
                node_type = metadata['aws::graph::index']['index']

                if node_type == 'source':
                    prev_version_ids = metadata.get('source', {}).get('versioning', {}).get('prev_versions', [])
                    prev_versions = self.lexical_graph.get_sources(source_ids=prev_version_ids)
                    deletable_prev_versions = [prev_version for prev_version in prev_versions if self.filter_fn(prev_version['metadata'])]
                    
                    if deletable_prev_versions:
                        logger.debug(f'Deleting previous versions for source [source_id: {node.id_}, prev_versions: {json.dumps(deletable_prev_versions)}]')
                        deletable_prev_version_ids = [d['sourceId'] for d in deletable_prev_versions]
                        self.lexical_graph.delete_sources(source_ids=deletable_prev_version_ids)
                
            yield node

class DeleteSources(BaseComponent):

    graph_store:GraphStore
    vector_store:VectorStore
    num_workers:int=10
    batch_size:int=1000

    def get_chunk_ids(self, source_id:str):

        cypher =  f'''// get chunk ids (delete source)                                  
        MATCH (s)<-[:`__EXTRACTED_FROM__`]-(c)
        WHERE {self.graph_store.node_id("s.sourceId")} = $sourceId
        RETURN DISTINCT {self.graph_store.node_id("c.chunkId")} AS chunkId LIMIT $batchSize
        '''

        parameters = {
            'sourceId': source_id,
            'batchSize': self.batch_size
        }

        results = self.graph_store.execute_query(cypher, parameters)

        chunk_ids = [r['chunkId'] for r in results]

        return chunk_ids
    
    def get_topic_ids(self, source_id:str):

        cypher =  f'''// get topic ids (delete source)                                  
        MATCH (s)<-[:`__EXTRACTED_FROM__`]-()<-[:`__MENTIONED_IN__`]-(t:`__Topic__`)
        WHERE {self.graph_store.node_id("s.sourceId")} = $sourceId
        RETURN DISTINCT {self.graph_store.node_id("t.topicId")} AS topicId LIMIT $batchSize
        '''

        parameters = {
            'sourceId': source_id,
            'batchSize': self.batch_size
        }

        results = self.graph_store.execute_query(cypher, parameters)

        topic_ids = [r['topicId'] for r in results]

        return topic_ids

    def get_statements_ids(self, source_id:str):

        cypher =  f'''// get statement ids (delete source)                                  
        MATCH (s)<-[:`__EXTRACTED_FROM__`]-()<-[:`__MENTIONED_IN__`]-(l:`__Statement__`)
        WHERE {self.graph_store.node_id("s.sourceId")} = $sourceId
        RETURN DISTINCT {self.graph_store.node_id("l.statementId")} AS statementId LIMIT $batchSize
        '''

        parameters = {
            'sourceId': source_id,
            'batchSize': self.batch_size
        }

        results = self.graph_store.execute_query(cypher, parameters)

        statement_ids = [r['statementId'] for r in results]

        return statement_ids
    
    def get_facts_ids(self, statement_ids:List[str]):

        cypher =  f'''// get fact ids (delete source)                                 
        MATCH (l)<-[:`__SUPPORTS__`]-(f)
        WHERE {self.graph_store.node_id("l.statementId")} IN $statementIds
        RETURN DISTINCT {self.graph_store.node_id("f.factId")} AS factId
        '''

        parameters = {
            'statementIds': statement_ids
        }

        results = self.graph_store.execute_query(cypher, parameters)

        fact_ids = [r['factId'] for r in results]

        return fact_ids
    
    def get_entity_ids(self, fact_ids:List[str]):

        cypher =  f'''// get entity ids (delete source)                                 
        MATCH (f)<-[:`__SUBJECT__`|`__OBJECT__`]-(e)
        WHERE {self.graph_store.node_id("f.factId")} IN $factIds
        RETURN DISTINCT {self.graph_store.node_id("e.entityId")} AS entityId
        '''

        parameters = {
            'factIds': fact_ids
        }

        results = self.graph_store.execute_query(cypher, parameters)

        entity_ids = [r['entityId'] for r in results]

        return entity_ids
    
    def get_orphaned_fact_ids(self, fact_ids:List[str]):

        cypher =  f'''// get orphaned fact ids (delete source)                                 
        MATCH (f)
        WHERE {self.graph_store.node_id("f.statementId")} IN $factIds
        AND NOT (f)-[:`__SUPPORTS__`]->()
        RETURN DISTINCT {self.graph_store.node_id("f.factId")} AS factId
        '''

        parameters = {
            'factIds': fact_ids
        }

        results = self.graph_store.execute_query(cypher, parameters)

        fact_ids = [r['factId'] for r in results]

        return fact_ids
    
    def get_orphaned_entity_ids(self, entity_ids:List[str]):

        cypher =  f'''// get orphaned entity ids (delete source)                                 
        MATCH (e)
        WHERE {self.graph_store.node_id("e.entityId")} IN $entityIds
        AND NOT (e)-[:`__SUBJECT__`|`__OBJECXT__`]->()
        RETURN DISTINCT {self.graph_store.node_id("e.entityId")} AS entityId
        '''

        parameters = {
            'entityIds': entity_ids
        }

        results = self.graph_store.execute_query(cypher, parameters)

        entity_ids = [r['entityId'] for r in results]

        return entity_ids
    
    def delete_entities(self, entity_ids:List[str]):

        cypher =  f'''// delete entity relationships (delete source)                                 
        MATCH (e)-[r]-()
        WHERE {self.graph_store.node_id("e.entityId")} IN $entityIds
        DELETE r
        '''

        parameters = {
            'entityIds': entity_ids
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)

        cypher =  f'''// delete entities (delete source)                                 
        MATCH (e)
        WHERE {self.graph_store.node_id("e.entityId")} IN $entityIds
        DELETE e
        '''

        self.graph_store.execute_query_with_retry(cypher, parameters)

    def delete_facts(self, fact_ids:List[str]):

        cypher =  f'''// delete fact relationships (delete source)                                 
        MATCH (f)-[r]-()
        WHERE {self.graph_store.node_id("f.factId")} IN $factIds
        DELETE r
        '''

        parameters = {
            'factIds': fact_ids
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)

        cypher =  f'''// delete facts (delete source)                                 
        MATCH (f)
        WHERE {self.graph_store.node_id("f.factId")} IN $factIds
        DELETE f
        '''

        self.graph_store.execute_query_with_retry(cypher, parameters)

    def delete_statements(self, statement_ids:List[str]):

        cypher =  f'''// delete statement relationships (delete source)                                 
        MATCH (l)-[r]-()
        WHERE {self.graph_store.node_id("l.statementId")} IN $statementIds
        DELETE r
        '''

        parameters = {
            'statementIds': statement_ids
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)

        cypher =  f'''// delete statements (delete source)                                 
        MATCH (l)
        WHERE {self.graph_store.node_id("l.statementId")} IN $statementIds
        DELETE l
        '''

        self.graph_store.execute_query_with_retry(cypher, parameters)

    def delete_topics(self, topic_ids:List[str]):

        cypher =  f'''// delete topic relationships (delete source)                                 
        MATCH (t)-[r]-()
        WHERE {self.graph_store.node_id("t.topicId")} IN $topicIds
        DELETE r
        '''

        parameters = {
            'topicIds': topic_ids
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)

        cypher =  f'''// delete topics (delete source)                                 
        MATCH (t)
        WHERE {self.graph_store.node_id("t.topicId")} IN $topicIds
        DELETE t
        '''

        self.graph_store.execute_query_with_retry(cypher, parameters)

    def delete_chunks(self, chunk_ids:List[str]):

        cypher =  f'''// delete chunk relationships (delete source)                                 
        MATCH (c)-[r]-()
        WHERE {self.graph_store.node_id("c.chunkIds")} IN $chunkIds
        DELETE r
        '''

        parameters = {
            'chunkIds': chunk_ids
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)

        cypher =  f'''// delete chunks (delete source)                                 
        MATCH (c)
        WHERE {self.graph_store.node_id("c.chunkIds")} IN $chunkIds
        DELETE c
        '''

        self.graph_store.execute_query_with_retry(cypher, parameters)

    def delete_source(self, source_id:str):

        cypher =  f'''// delete source relationships (delete source)                                 
        MATCH (s)-[r]-()
        WHERE {self.graph_store.node_id("s.sourceId")} = $sourceId
        DELETE r
        '''

        parameters = {
            'sourceId': source_id
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)

        cypher =  f'''// delete source (delete source)                                 
        MATCH (s)
        WHERE {self.graph_store.node_id("s.sourceId")} = $sourceId
        DELETE s
        '''

        self.graph_store.execute_query_with_retry(cypher, parameters)

    def delete_source_document(self, source_id:str) -> Dict[str, Any]:

        chunk_count = 0
        topic_count = 0
        statement_count = 0
        fact_count = 0
        entity_count = 0

        statement_ids = self.get_statements_ids(source_id)
        
        while statement_ids:
            
            fact_ids = self.get_facts_ids(statement_ids)
            self.delete_statements(statement_ids)
            orphaned_fact_ids = self.get_orphaned_fact_ids(fact_ids)
            if orphaned_fact_ids:
                entity_ids = self.get_entity_ids(orphaned_fact_ids)
                self.delete_facts(orphaned_fact_ids)
                fact_count += len(orphaned_fact_ids)
                orphaned_entity_ids = self.get_orphaned_entity_ids(entity_ids)
                if orphaned_entity_ids:
                    self.delete_entities(orphaned_entity_ids)
                    entity_count += len(orphaned_entity_ids)
            self.vector_store.get_index('statement').delete_embeddings(statement_ids)
            statement_count += len(statement_ids)
            statement_ids = self.get_statements_ids(source_id)

        topic_ids = self.get_topic_ids(source_id)
        
        while topic_ids:
            self.delete_topics(topic_ids)
            self.vector_store.get_index('topic').delete_embeddings(topic_ids)
            topic_count += len(topic_ids)
            topic_ids = self.get_topic_ids(source_id)

        chunk_ids = self.get_chunk_ids(source_id)

        while chunk_ids:
            self.delete_chunks(chunk_ids)
            self.vector_store.get_index('chunk').delete_embeddings(chunk_ids)
            chunk_count += len(chunk_ids)
            chunk_ids = self.get_chunk_ids(source_id)

        self.delete_source(source_id)

        logger.debug(f'Deleted source [source_id: {source_id}, chunks: {chunk_count}, topics: {topic_count}, statements: {statement_count}, facts: {fact_count}, entities: {entity_count}]')

        return {
            'sourceId': source_id,
            'chunks': chunk_count,
            'topics': topic_count,
            'statements': statement_count,
            'facts': fact_count,
            'entities': entity_count
        }
    

    def delete_source_documents(self, source_ids:List[str]) -> List[Dict[str, Any]]:

        source_id_batches = [
            source_ids[x:x+self.num_workers] 
            for x in range(0, len(source_ids), self.num_workers)
        ]

        deleted_sources = []

        for source_id_batch in source_id_batches:

             with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:

                futures = [
                    executor.submit(self.delete_source_document, source_id)
                    for source_id in source_id_batch
                ]
                
                executor.shutdown()

                for future in futures:
                    deleted_sources.append(future.result())

        return deleted_sources


    