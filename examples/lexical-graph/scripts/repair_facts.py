# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import argparse
import json
import logging
import sys
import json
import time

from itertools import islice
from tqdm import tqdm

from graphrag_toolkit.lexical_graph import set_logging_config
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine, TenantId
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

#set_logging_config('DEBUG', ['graphrag_toolkit.lexical_graph.storage.graph'])

def get_anon_rel_ids_count(graph_store, fact_ids, batch_size):
    
    total_rels = 0
    
    progress_bar_1 = tqdm(total=len(fact_ids), desc='Counting invalid SUBJECT|OBJECT relationships')
    for fact_id_batch in iter_batch(fact_ids, batch_size=batch_size):
        cypher = '''
        MATCH (f)<-[r]-(:`vertex`) WHERE id(f) in $fact_ids
        RETURN count(r) AS count
        '''
    
        params = {
            'fact_ids': fact_id_batch
        }
    
        results = graph_store.execute_query_with_retry(cypher, params)
    
        counts = [r['count'] for r in results]
        total_rels += sum(counts)
        progress_bar_1.update(len(fact_id_batch))
    
    return total_rels

def get_anon_rel_ids(graph_store, batch_size):
    
    params = {
        'batch_size': batch_size
    }
    
    cypher = '''
    MATCH (:`vertex`)-[r:`__SUBJECT__`|`__OBJECT__`]->()
    RETURN DISTINCT id(r) AS rel_id LIMIT $batch_size
    '''
    
    results = graph_store.execute_query_with_retry(cypher, params)
    
    return [r['rel_id'] for r in results]
    
def get_anon_node_ids(graph_store, batch_size):
    
    params = {
        'batch_size': batch_size
    }
    
    cypher = '''
    MATCH (n:`vertex`)
    RETURN DISTINCT id(n) AS node_id LIMIT $batch_size
    '''
    
    results = graph_store.execute_query_with_retry(cypher, params)
    
    return [r['node_id'] for r in results]
    

def delete_invalid_relationships(graph_store, fact_ids, batch_size):
    
    total_rels = 0
    total_nodes = 0
    
    num_invalid_rels = get_anon_rel_ids_count(graph_store, fact_ids, batch_size)
    
    progress_bar_1 = tqdm(total=num_invalid_rels, desc='Deleting invalid SUBJECT|OBJECT relationship')
    
    cypher = '''
    MATCH ()-[r]->()
    WHERE id(r) in $rel_ids
    DELETE r
    '''
    
    rel_ids = get_anon_rel_ids(graph_store, batch_size)
    count = len(rel_ids)
    
    params = {
       'rel_ids': rel_ids 
    }
    
    graph_store.execute_query_with_retry(cypher, params)
    
    total_rels += count
    progress_bar_1.update(count)
    
    
    while count > 0:
        rel_ids = get_anon_rel_ids(graph_store, batch_size)
        count = len(rel_ids)
        
        params = {
           'rel_ids': rel_ids 
        }
    
        graph_store.execute_query_with_retry(cypher, params)
    
        total_rels += count
        progress_bar_1.update(count)
        
    print(f'Deleted {total_rels} invalid SUBJECT|OBJECT relationships')
    
    #progress_bar_2 = tqdm(total=1000000, desc='Deleting anon nodes')
    #        
    #cypher = '''
    #MATCH (n)
    #WHERE id(n) in $node_ids
    #DETACH DELETE n
    #'''
    #
    #node_ids = get_anon_node_ids(graph_store, batch_size)
    #count = len(node_ids)
    #
    #params = {
    #   'node_ids': node_ids 
    #}
    #
    #graph_store.execute_query_with_retry(cypher, params)
#
    #total_nodes += count
    #progress_bar_2.update(count)
    #
    #while count > 0:
    #    node_ids = get_anon_node_ids(graph_store, batch_size)
    #    count = len(node_ids)
    #
    #    params = {
    #       'node_ids': node_ids 
    #    }
    #
    #    graph_store.execute_query_with_retry(cypher, params)
#
    #    total_nodes += count
    #    progress_bar_2.update(count)
    #    
    #print(f'Deleted {total_nodes} nodes relationships')
    

def get_fact_ids(graph_store):
    
    cypher = '''
    MATCH (n:`__Fact__`) 
    RETURN id(n) AS fact_id'''
    
    results = graph_store.execute_query_with_retry(cypher, {})
    
    return [r['fact_id'] for r in results]
    
def get_fact_ids_from_sources(graph_store, skip_invalid_relationships):
    
    cypher = '''
    MATCH (n:`__Source__`) 
    RETURN id(n) AS source_id'''
    
    results = graph_store.execute_query_with_retry(cypher, {})
    
    source_ids = [r['source_id'] for r in results]
    
    and_clause = ' AND NOT ((f)<-[:`__SUBJECT__`|`__OBJECT__`]-(:`__Entity__`))' if skip_invalid_relationships else ''
    
    cypher = f'''
    MATCH (n:`__Source__`)<-[:`__EXTRACTED_FROM__`]-(:`__Chunk__`)
    <-[:`__MENTIONED_IN__`]-(:`__Statement__`)
    <-[:`__SUPPORTS__`]-(f:`__Fact__`)
    WHERE id(n) = $source_id{and_clause}
    RETURN id(f) AS fact_id
    '''
    
    fact_ids = []
    
    desc = 'Getting fact ids for facts without SUBJECT|OBJECT relationships' if skip_invalid_relationships else 'Getting fact ids from sources'
    progress_bar_1 = tqdm(total=len(source_ids), desc=desc)
    
    for source_id in source_ids:
        params = {
            'source_id': source_id
        }
        results = graph_store.execute_query_with_retry(cypher, params)
        source_fact_ids = [r['fact_id'] for r in results]
        fact_ids.extend(source_fact_ids)
        progress_bar_1.update(1)
    
    return list(set(fact_ids))

def get_facts(graph_store, fact_ids):
    
    facts = []
    
    cypher = '''
    MATCH (n:`__Fact__`) 
    WHERE id(n) in $fact_ids 
    RETURN id(n) AS fact_id, n.value AS value'''
    
    params = {
        'fact_ids': fact_ids
    }
    
    results = graph_store.execute_query_with_retry(cypher, params)
    
    for result in results:
        s = []
        p = []
        o = []
        fact_id = result['fact_id']
        value = result['value']
        parts = value.split(' ')
        for part in parts:
            if part.upper() == part and part.replace('-', '').isalpha():
                p.append(part)
            else:
                if p:
                    o.append(part)
                else:
                    s.append(part)
                
        fact = {
            'fact_id': fact_id,
            'subject': ' '.join(s),
            'predicate': '_'.join(p),
            'object': ' '.join(o)
        }
        facts.append(fact)
    return facts

def create_entity_fact_relation(graph_store, facts, relationship_type):
    
    params = []
    
    for fact in facts:
        params.append({
            'fact_id': fact['fact_id'],
            'entity_value': fact[relationship_type]
        })
    
    parameters = {
        'params': params    
    }
    
    cypher = f'''
    UNWIND $params AS params
    MATCH (f:`__Fact__`{{`~id`: params.fact_id}}), (e:`__Entity__`{{value: params.entity_value}})
    MERGE (e)-[:`__{relationship_type.upper()}__`]->(f)
    '''
    
    graph_store.execute_query_with_retry(cypher, parameters)
    
    
    
def create_entity_entity_relation(graph_store, facts):
    
    params = []
    
    for fact in facts:
        params.append({
            's_value': fact['subject'],
            'o_value': fact['object'],
            'p': fact['predicate']
        })
    
    parameters = {
        'params': params    
    }
    
    cypher = '''
    UNWIND $params AS params
    MATCH (s:`__Entity__`{value: params.s_value}), (o:`__Entity__`{value: params.o_value})
    MERGE (s)-[r:`__RELATION__`{value: params.p}]->(o) ON CREATE SET r.count = 1 ON MATCH SET r.count = r.count + 1
    '''
    
    graph_store.execute_query_with_retry(cypher, parameters)
    
    
def get_stats(graph_store, fact_ids, batch_size):

    stats = {}
    
    total_subject = 0
    
    progress_bar_1 = tqdm(total=len(fact_ids), desc='Counting SUBJECT relationships')
    for fact_id_batch in iter_batch(fact_ids, batch_size=batch_size):
        cypher = '''
        MATCH (f)<-[r:`__SUBJECT__`]-(:`__Entity__`) WHERE id(f) in $fact_ids
        RETURN count(r) AS count
        '''
    
        params = {
            'fact_ids': fact_id_batch
        }
    
        results = graph_store.execute_query_with_retry(cypher, params)
    
        counts = [r['count'] for r in results]
        total_subject += sum(counts)
        progress_bar_1.update(len(fact_id_batch))
    
    stats['num_subject_relationships'] = total_subject
    
    total_object = 0
    
    progress_bar_1 = tqdm(total=len(fact_ids), desc='Counting OBJECT relationships')
    for fact_id_batch in iter_batch(fact_ids, batch_size=batch_size):
        cypher = '''
        MATCH (f)<-[r:`__OBJECT__`]-(:`__Entity__`) WHERE id(f) in $fact_ids
        RETURN count(r) AS count
        '''
    
        params = {
            'fact_ids': fact_id_batch
        }
    
        results = graph_store.execute_query_with_retry(cypher, params)
    
        counts = [r['count'] for r in results]
        total_object += sum(counts)
        progress_bar_1.update(len(fact_id_batch))
    
    stats['num_object_relationships'] = total_object
    
    return stats

def iter_batch(iterable, batch_size):
    source_iter = iter(iterable)
    while source_iter:
        b = list(islice(source_iter, batch_size))
        if len(b) == 0:
            break
        yield b

def repair(graph_store_info, batch_size, skip_invalid_relationships, skip_entity_fact_relationships, tenant_id=None):

    graph_store = GraphStoreFactory.for_graph_store(
        graph_store_info,
        log_formatting=NonRedactedGraphQueryLogFormatting()
    )
    

    if not tenant_id:
        print(f'Repairing for default tenant')
    else:
        print(f'Repairing for tenant {tenant_id}')
        graph_store = MultiTenantGraphStore.wrap(
            graph_store,
            TenantId(tenant_id)
        )

    fact_ids = get_fact_ids_from_sources(graph_store, False)
    fact_ids_to_process = fact_ids if not skip_invalid_relationships else get_fact_ids_from_sources(graph_store, True)
   
            
    stats = {
        'tenant_id': tenant_id
    }
    
    stats['before'] = get_stats(graph_store, fact_ids, batch_size)
        
    if not skip_invalid_relationships:
        delete_invalid_relationships(graph_store, fact_ids, batch_size=batch_size)

    if not skip_entity_fact_relationships:
    
        progress_bar_1 = tqdm(total=len(fact_ids_to_process), desc='Creating SUBJECT|OBJECT entity-fact relationships')
        for fact_id_batch in iter_batch(fact_ids_to_process, batch_size=batch_size):
            facts = get_facts(graph_store, fact_id_batch)
            create_entity_fact_relation(graph_store, facts, 'subject')
            create_entity_fact_relation(graph_store, facts, 'object')
            progress_bar_1.update(len(fact_id_batch))

    stats['after'] = get_stats(graph_store, fact_ids, batch_size)

    return stats


def do_repair():

    parser = argparse.ArgumentParser()
    parser.add_argument('--graph-store', help = 'Graph store connection string')
    parser.add_argument('--tenant-id', nargs='*', help = 'Space-separated list of tenant ids (optional)')
    parser.add_argument('--batch-size', nargs='?', help = 'Batch size (optional, default 100)')
    parser.add_argument('--skip-invalid-relationships', action='store_true', help = 'Skip deleting invalid relationships (optional)')
    parser.add_argument('--skip-entity-fact-relationships', action='store_true', help = 'Skip creating entity-fact relationships (optional)')
    
    args, _ = parser.parse_known_args()
    args_dict = { k:v for k,v in vars(args).items() if v}

    graph_store_info = args_dict['graph_store']
    tenant_ids = args_dict.get('tenant_id', [])
    batch_size = int(args_dict.get('batch_size', 100))
    skip_invalid_relationships = bool(args_dict.get('skip_invalid_relationships', False))
    skip_entity_fact_relationships = bool(args_dict.get('skip_entity_fact_relationships', False))

    print(f'graph_store_info               : {graph_store_info}')
    print(f'tenant_ids                     : {tenant_ids}')
    print(f'batch_size                     : {batch_size}')
    print(f'skip_invalid_relationships     : {skip_invalid_relationships}')
    print(f'skip_entity_fact_relationships : {skip_entity_fact_relationships}')
    print()

    results = []
    
    if not tenant_ids:
            results.append(repair(graph_store_info, batch_size, skip_invalid_relationships, skip_entity_fact_relationships))
    else:
        for tenant_id in tenant_ids:
            results.append(repair(graph_store_info, batch_size, skip_invalid_relationships, skip_entity_fact_relationships, tenant_id))
                
    print()
    print(json.dumps(results, indent=2))
    
            
if __name__ == '__main__':
    start = time.time()
    do_repair()
    end = time.time()
    print(end - start)