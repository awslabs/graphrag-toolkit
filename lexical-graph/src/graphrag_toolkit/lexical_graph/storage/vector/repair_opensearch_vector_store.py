# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
import logging

from typing import List, Dict

from graphrag_toolkit.lexical_graph import TenantId, TenantIdType, to_tenant_id, DEFAULT_TENANT_ID
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore, MultiTenantVectorStore, ReadOnlyVectorStore
from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes import OpenSearchIndex
from graphrag_toolkit.lexical_graph.storage.constants import ALL_EMBEDDING_INDEXES

logger = logging.getLogger(__name__)

def get_tenant_ids(graph_store:GraphStore):
    
    cypher = '''MATCH (n)
    WITH DISTINCT labels(n) as lbls
    WITH split(lbls[0], '__') AS lbl_parts WHERE size(lbl_parts) > 2
    WITH lbl_parts WHERE lbl_parts[1] = 'SYS_Class' AND lbl_parts[2] <> ''
    RETURN DISTINCT lbl_parts[2] AS tenant_id
    '''

    results = graph_store.execute_query_with_retry(cypher, {})

    return [to_tenant_id(result['tenant_id']) for result in results]

def index_exists(tenant_id:TenantId, index_name:str, vector_store:VectorStore):
    
    read_only_vector_store = ReadOnlyVectorStore.wrap(
        MultiTenantVectorStore.wrap(
            vector_store,
            tenant_id
        )
    )

    index = read_only_vector_store.get_index(index_name)

    if isinstance(index, OpenSearchIndex):
        return index.index_exists()
    else:
        return False

def get_node_ids(tenant_id:TenantId, index_name:str, graph_store:GraphStore):
    
    label_name = f'__{index_name.capitalize()}__'
    id_name = f'n.{index_name}Id'
    id_selector = graph_store.node_id(id_name)
    
    cypher = f'MATCH (n:`{label_name}`) RETURN {id_selector} AS node_id'

    multi_tenant_graph_store = MultiTenantGraphStore.wrap(
        graph_store,
        tenant_id
    )

    results = multi_tenant_graph_store.execute_query_with_retry(cypher, {})

    return [result['node_id'] for result in results]

def get_existing_doc_ids_for_node_ids(tenant_id:TenantId, index_name:str, vector_store:VectorStore, node_ids:List[str]):
    
    multi_tenant_vector_store = MultiTenantVectorStore.wrap(
        vector_store,
        tenant_id
    )

    return multi_tenant_vector_store.get_index(index_name)._get_existing_doc_ids_for_ids(node_ids)

def delete_duplicate_docs(tenant_id:TenantId, index_name:str, vector_store:VectorStore, doc_id_map:Dict[str, List[str]], dry_run:bool=False):
        
        doc_ids_to_delete = [
            d
            for doc_ids in list(doc_id_map.values())
            for d in doc_ids[1:]
        ]

        if not dry_run:
            
            multi_tenant_vector_store = MultiTenantVectorStore.wrap(
                vector_store,
                tenant_id
            )

            index = multi_tenant_vector_store.get_index(index_name)
            client = index.client

            for doc_id in doc_ids_to_delete:
                client._os_client.delete(client._index, doc_id)
       
        
        return len(doc_ids_to_delete)

def number_doc_ids(doc_id_map:Dict[str, List[str]]):
    count = 0
    for doc_ids in doc_id_map.values():
        count += len(doc_ids)
    return count

def unindexed_nodes(node_ids:List[str], doc_id_map:Dict[str, List[str]]):
    return [
        node_id
        for node_id in node_ids
        if node_id not in doc_id_map
    ]

def batches(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def repair_opensearch_vector_store(graph_store_info:str, vector_store_info:str, tenant_ids:List[str]=[], batch_size:int=1000, dry_run:bool=False):

    start = time.time()

    graph_store = GraphStoreFactory.for_graph_store(graph_store_info)
    vector_store = VectorStoreFactory.for_vector_store(vector_store_info)

    logger.info('Initialising...')

    if not tenant_ids:
        tenant_ids = [DEFAULT_TENANT_ID]
        tenant_ids.extend(get_tenant_ids(graph_store))
    else:
        tenant_ids = [to_tenant_id(tenant_id) for tenant_id in tenant_ids]

    logger.info(f'graph_store  : {graph_store_info}')
    logger.info(f'vector_store : {vector_store_info}')
    logger.info(f'tenant_ids   : {[str(tenant_id) for tenant_id in tenant_ids]}')
    logger.info(f'dry_run      : {dry_run}')

    results = []

    total_node_ids = 0
    total_doc_ids = 0
    total_deleted_doc_ids = 0
    total_unindexed = 0

    for tenant_id in tenant_ids:
        for index_name in ALL_EMBEDDING_INDEXES:
            
            logger.info(f'Processing index [tenant_id: {tenant_id}, index: {index_name}, batch_size: {batch_size}]')
            
            if index_exists(tenant_id, index_name, vector_store):
                
                index_total_doc_ids = 0
                index_total_deleted_doc_ids = 0
                index_total_unindexed = 0
                
                node_ids = get_node_ids(tenant_id, index_name, graph_store)
                logger.info(f'  Found {len(node_ids)} {index_name} nodes in graph')

                total_node_ids += len(node_ids)
                
                for node_id_batch in batches(node_ids, batch_size):
                    
                    logger.info(f'  Processing batch [batch_size: {len(node_id_batch)}]')
                    
                    doc_id_map = get_existing_doc_ids_for_node_ids(tenant_id, index_name, vector_store, node_id_batch)
                    
                    num_doc_ids = number_doc_ids(doc_id_map)
                    index_total_doc_ids += num_doc_ids
                    total_doc_ids += num_doc_ids
                    
                    logger.info(f'    Found {num_doc_ids} documents in {index_name} index')

                    missing_nodes = unindexed_nodes(node_id_batch, doc_id_map)
                    if missing_nodes:
                        print(f'    {len(missing_nodes)} unindexed nodes in {index_name} index [node_ids: {missing_nodes}]')
                        index_total_unindexed += len(missing_nodes)
                        total_unindexed += len(missing_nodes)
                    
                    num_deleted_docs = delete_duplicate_docs(tenant_id, index_name, vector_store, doc_id_map, dry_run)
                    index_total_deleted_doc_ids += num_deleted_docs
                    total_deleted_doc_ids += num_deleted_docs

                    logger.info(f'    Deleted {num_deleted_docs} documents from {index_name} index [dry_run: {dry_run}]')

                result = {
                    'tenant_id': str(tenant_id),
                    'index': index_name,
                    'num_nodes': len(node_ids),
                    'num_docs': index_total_doc_ids,
                    'num_deleted': index_total_deleted_doc_ids,
                    'num_unindexed': index_total_unindexed
                }
                results.append(result)

                logger.info(f'  Finished processing index [{result}]')
                
            else:
                logger.info(f'  Index does not exist, skipping processing [tenant_id: {tenant_id}, index: {index_name}]')

            end = time.time()

    return {
        'duration_seconds': int(end-start),
        'dry_run': dry_run,
        'totals': {
            'total_node_ids': total_node_ids,
            'total_doc_ids': total_doc_ids,
            'total_deleted_doc_ids': total_deleted_doc_ids,
            'total_unindexed': total_unindexed
        },
        'results': results
    }
    