# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph.storage.graph import GraphQueryOperation


def write(graph_store, operation, **row):
    return graph_store.execute_query('', {'params': [row]}, operation=operation)


def add_fact(graph_store, subject, object_id, statement):
    for entity in (subject, object_id):
        write(graph_store, GraphQueryOperation.UPSERT_ENTITY,
              e_id=entity, v=entity, e_search_str=entity, ec='Person')
    write(graph_store, GraphQueryOperation.UPSERT_FACT,
          fact_id=statement, statement_id=statement, fact=statement, _predicate='KNOWS')
    for relationship, entity in [('subject', subject), ('object', object_id)]:
        write(graph_store, GraphQueryOperation.LINK_FACT_ENTITY,
              fact_id=statement, entity_id=entity, _relationship_type=relationship)


def test_multi_entity_search_includes_facts_between_all_path_entities(graph_store):
    for subject, object_id, statement in [
        ('a', 'b', 'ab'), ('b', 'd', 'bd'),
        ('a', 'c', 'ac'), ('c', 'e', 'ce'),
        ('b', 'c', 'bc'), ('a', 'unrelated', 'excluded'),
    ]:
        add_fact(graph_store, subject, object_id, statement)
    rows = graph_store.execute_query('', {'startId': 'a', 'endIds': ['d', 'e']},
                                     operation=GraphQueryOperation.SEARCH_BY_ENTITIES)
    assert {row['l'] for row in rows} == {'ab', 'bd', 'ac', 'ce', 'bc'}


@pytest.mark.parametrize('operation', [GraphQueryOperation.SEARCH_BY_ENTITY, GraphQueryOperation.SEARCH_BY_ENTITIES])
def test_entity_search_includes_adjacent_statements(graph_store, operation):
    add_fact(graph_store, 'a', 'b', 'middle')
    write(graph_store, GraphQueryOperation.LINK_STATEMENTS, statement_id='middle', prev_statement_id='before')
    write(graph_store, GraphQueryOperation.LINK_STATEMENTS, statement_id='after', prev_statement_id='middle')
    rows = graph_store.execute_query('', {'startId': 'a', 'endIds': ['b']}, operation=operation)
    assert {row['l'] for row in rows} == {'before', 'middle', 'after'}


def test_neighbor_ranking_does_not_count_the_same_relation_twice(graph_store):
    for subject, object_id, statement in [
        ('a', 'b', 'ab1'), ('a', 'b', 'ab2'),
        ('a', 'c', 'ac'), ('c', 'd', 'cd'), ('c', 'e', 'ce'),
    ]:
        add_fact(graph_store, subject, object_id, statement)
    rows = graph_store.execute_query('', {'entityIds': ['a'], 'numNeighbours': 1},
                                     operation=GraphQueryOperation.FIND_ENTITY_NEIGHBORS)
    assert rows[0]['result']['others'] == ['c']
