# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Carrying typed values across a local-entity rewrite.

`LocalEntityRewritesGraphBuilder` folds a complement node into a real entity when
the two turn out to be the same thing, and the fold `DETACH DELETE`s the complement.
At a complement placement that node is where `typed_value` and `datatype` live, so
they have to be copied onto the surviving node while the doomed one is still bound.

The builder emits Cypher rather than executing it, so these tests read the emitted
query text through a recording stand-in for the graph store. That is the whole of
what can be checked without a live store, and it is the part that regresses: the
clause is appended by string concatenation, and it is appended *conditionally*.
"""

import logging

import pytest
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.build.local_entity_rewrites_graph_builder import (
    LocalEntityRewritesGraphBuilder,
)
from graphrag_toolkit.lexical_graph.indexing.model import Entity, Fact, Relation

XSD = 'http://www.w3.org/2001/XMLSchema#'
LOCAL = '__Local_Entity__'

class RecordingGraphStore:
    """The two methods this builder asks a store for."""

    def __init__(self):
        self.trees = []

    def node_id(self, name):
        return name

    def execute_query_with_retry(self, query_tree, params, **kwargs):
        self.trees.append((query_tree, params))

def fact(subject_class='Company', complement_class=LOCAL):
    return Fact(
        factId='f1',
        subject=Entity(entityId='e1', value='Meridian Freight', classification=subject_class),
        predicate=Relation(value='foundedYear', canonicalName='foundedYear'),
        complement=Entity(
            entityId='c1', altEntityId='e1', value='1994',
            classification=complement_class, datatype=f'{XSD}integer',
        ),
    )

def build(typed_properties=None, include_local_entities=True, node_fact=None):
    store = RecordingGraphStore()
    kwargs = {'include_local_entities': include_local_entities}
    if typed_properties is not None:
        kwargs['typed_properties'] = typed_properties

    metadata = {} if node_fact is False else {'fact': (node_fact or fact()).model_dump()}
    LocalEntityRewritesGraphBuilder().build(TextNode(text='x', metadata=metadata), store, **kwargs)
    return store

def queries(store):
    """Every query in every tree the builder issued, as text."""
    found = []

    def walk(query):
        found.append(query.query)
        for child in getattr(query, 'child_queries', None) or []:
            walk(child)

    for (tree, _) in store.trees:
        walk(tree.root_query)
    return found

def copy_query(store):
    return next(q for q in queries(store) if 'copy complement relationships' in q)

class TestTheCarryClause:

    @pytest.mark.parametrize('placement', ['complement', 'both'])
    def test_a_complement_placement_carries_both_properties(self, placement):
        query = copy_query(build(placement))

        assert 'SET n.`typed_value`' in query
        assert 'n.`datatype`' in query

    @pytest.mark.parametrize('placement', ['complement', 'both'])
    def test_the_carry_is_first_writer_wins(self, placement):
        """Two complements can fold into the same entity, and the second must not
        overwrite the first. Arbitrary, but stable within a run - the alternative
        is a value that changes with build order."""
        query = copy_query(build(placement))

        assert 'coalesce(n.`typed_value`, c.`typed_value`)' in query
        assert 'coalesce(n.`datatype`, c.`datatype`)' in query

    @pytest.mark.parametrize('placement', [None, 'off', 'subject'])
    def test_no_clause_is_emitted_where_nothing_was_asked_for(self, placement):
        """`SET x = null` deletes the property in some stores, it changes the query
        text every existing user sends, and it is write work nobody asked for. So
        the clause is absent rather than emitted as a harmless no-op - which is
        also what keeps the query byte-identical for a caller who never set the
        kwarg at all."""
        query = copy_query(build(placement))

        assert 'typed_value' not in query
        assert 'coalesce' not in query

    def test_the_query_is_byte_identical_across_the_placements_that_do_not_carry(self):
        assert copy_query(build(None)) == copy_query(build('off'))
        assert copy_query(build('off')) == copy_query(build('subject'))

    def test_only_the_copy_query_changes(self):
        """The delete half of the fold is not a function of the placement."""
        for placement in ('off', 'complement', 'both'):
            deletes = [q for q in queries(build(placement)) if 'delete complement relationships' in q]
            assert deletes == [q for q in queries(build('off')) if 'delete complement relationships' in q]

class TestWhatTheBuilderIssues:

    def test_both_directions_of_the_fold_are_attempted(self):
        """A subject may turn out to be a local entity, and a complement may turn
        out to be a real one, so the builder asks about each."""
        store = build('complement')

        assert len(store.trees) == 2
        assert any('matching subject' in q for q in queries(store))
        assert any('matching complement' in q for q in queries(store))

    def test_a_local_entity_subject_is_skipped_when_local_entities_are_off(self):
        store = build('complement', include_local_entities=False, node_fact=fact(subject_class=LOCAL))

        assert store.trees == []

    def test_a_local_entity_subject_is_processed_when_they_are_on(self):
        store = build('complement', include_local_entities=True, node_fact=fact(subject_class=LOCAL))

        assert store.trees

    def test_a_node_carrying_no_fact_warns_and_writes_nothing(self, caplog):
        with caplog.at_level(logging.WARNING):
            store = build('complement', node_fact=False)

        assert store.trees == []
        assert 'fact_id missing' in caplog.text
