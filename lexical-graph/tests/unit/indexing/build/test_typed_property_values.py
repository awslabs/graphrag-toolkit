# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Where a coerced attribute value gets written, if anywhere.

Most of the decision is in two pure functions that answer "what,
if anything, does *this* fact contribute", so most of this file tests those
directly. The builder is exercised through a recording stand-in for the graph
store, for the one property that is about the queries rather than the values: the
entity insert has to be byte-identical at every placement, so turning typed
properties on cannot disturb `value`, `search_str` or `class`.
"""

import logging

import pytest

from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import (
    EntityGraphBuilder,
    _typed_complement_values,
    _typed_subject_property,
)
from graphrag_toolkit.lexical_graph.indexing.model import Entity, Fact, Relation

XSD = 'http://www.w3.org/2001/XMLSchema#'
LOCAL = '__Local_Entity__'

def attribute_fact(canonical_name='foundedYear', value='1994', datatype=f'{XSD}integer'):
    """An annotated attribute fact, as the ontology filter leaves one."""
    return Fact(
        factId='f1',
        subject=Entity(entityId='e1', value='Meridian Freight', classification='Company'),
        predicate=Relation(value='foundedYear', canonicalName=canonical_name),
        complement=Entity(entityId='c1', value=value, classification=LOCAL, datatype=datatype),
    )

def relation_fact():
    """An annotated *relation*: `canonicalName` is set, but there is no literal."""
    return Fact(
        factId='f2',
        subject=Entity(entityId='e1', value='Priya Raman', classification='Person'),
        predicate=Relation(value='worksFor', canonicalName='worksFor'),
        object=Entity(entityId='e2', value='Meridian Freight', classification='Company'),
    )

class RecordingGraphStore:
    """The three methods `EntityGraphBuilder` asks a store for."""

    def __init__(self):
        self.calls = []

    def node_id(self, name):
        return name

    def property_assigment_fn(self, key, value):
        return lambda placeholder: placeholder

    def execute_query_with_retry(self, query, params, **kwargs):
        self.calls.append((query, params))

def build(fact, typed_properties, include_local_entities=True):
    from llama_index.core.schema import TextNode

    store = RecordingGraphStore()
    EntityGraphBuilder().build(
        TextNode(text='x', metadata={'fact': fact.model_dump()}),
        store,
        include_domain_labels=False,
        include_local_entities=include_local_entities,
        typed_properties=typed_properties,
    )
    return store.calls

def queries(calls):
    return [query for (query, _) in calls]

def row(params):
    """The one parameter row `UNWIND $params AS params` is given."""
    return params['params'][0]

class TestWhichPlacementWrites:

    @pytest.mark.parametrize('placement,writes', [
        ('off', False), ('subject', True), ('complement', False), ('both', True),
    ])
    def test_the_subject_property(self, placement, writes):
        assert bool(_typed_subject_property(attribute_fact(), placement)) is writes

    @pytest.mark.parametrize('placement,writes', [
        ('off', False), ('subject', False), ('complement', True), ('both', True),
    ])
    def test_the_complement_values(self, placement, writes):
        complement = attribute_fact().complement
        assert bool(_typed_complement_values(complement, placement)) is writes

    def test_both_is_not_a_fallback_chain(self):
        """Each placement writes to its own node, and `'both'` does each."""
        fact = attribute_fact()
        assert _typed_subject_property(fact, 'both') == ('foundedYear', 1994)
        assert _typed_complement_values(fact.complement, 'both') == (1994, f'{XSD}integer')

class TestTheSubjectProperty:

    def test_the_key_is_the_canonical_name_and_the_value_is_coerced(self):
        assert _typed_subject_property(attribute_fact(), 'subject') == ('foundedYear', 1994)

    def test_a_relation_contributes_nothing_even_though_it_is_annotated(self):
        """`complement.datatype` is the discriminator, not `canonicalName`.

        The filter sets `canonicalName` for every resolved predicate including
        object properties. Keying off it alone would write the object entity's
        display string into an attribute slot on the subject.
        """
        assert _typed_subject_property(relation_fact(), 'subject') is None

    @pytest.mark.parametrize('kwargs', [
        {'canonical_name': None},
        {'datatype': None},
    ])
    def test_an_unannotated_fact_contributes_nothing(self, kwargs):
        """An unresolved predicate has no declared datatype."""
        assert _typed_subject_property(attribute_fact(**kwargs), 'subject') is None

    def test_a_literal_that_does_not_coerce_is_refused_rather_than_stored_raw(self):
        """Storing `'nineteen ninety four'` under a key whose
        name promises a number is worse than storing nothing."""
        assert _typed_subject_property(attribute_fact(value='nineteen ninety four'), 'subject') is None

    @pytest.mark.parametrize('value,datatype,expected', [
        ('0', f'{XSD}integer', 0),
        ('false', f'{XSD}boolean', False),
        ('0.0', f'{XSD}double', 0.0),
    ])
    def test_a_falsy_value_is_still_written(self, value, datatype, expected):
        """Callers must test the pair, not the value."""
        pair = _typed_subject_property(attribute_fact(value=value, datatype=datatype), 'subject')
        assert pair == ('foundedYear', expected)

    @pytest.mark.parametrize('name', ['value', 'search_str', 'class'])
    def test_a_name_the_graph_model_owns_is_skipped_with_a_warning(self, name, caplog):
        """the reserved-name rule, defence in depth: the config already refuses such an
        ontology, but a fact can arrive from a checkpoint written under another."""
        with caplog.at_level(logging.WARNING):
            assert _typed_subject_property(
                attribute_fact(canonical_name=name, datatype=f'{XSD}string'), 'subject',
            ) is None

        assert 'already owns' in caplog.text

    @pytest.mark.parametrize('name', ['typed_value', 'datatype'])
    def test_the_complement_names_are_owned_only_once_complement_placement_writes(self, name):
        fact = attribute_fact(canonical_name=name, datatype=f'{XSD}string', value='x')

        assert _typed_subject_property(fact, 'subject') == (name, 'x')
        assert _typed_subject_property(fact, 'both') is None

    @pytest.mark.parametrize('name', ['founded\nYear', 'founded\rYear'])
    def test_a_name_carrying_a_line_break_is_rejected(self, name, caplog):
        """Every other character is escaped; a line break would
        split the query across lines the builder treats as one statement each, and
        no well-formed Turtle term contains one."""
        with caplog.at_level(logging.WARNING):
            assert _typed_subject_property(attribute_fact(canonical_name=name), 'subject') is None

        assert 'line break' in caplog.text

class TestTheComplementValues:

    def test_the_pair_is_the_coerced_value_and_the_declared_datatype(self):
        complement = attribute_fact().complement
        assert _typed_complement_values(complement, 'complement') == (1994, f'{XSD}integer')

    def test_an_unannotated_complement_contributes_nothing(self):
        complement = attribute_fact(datatype=None).complement
        assert _typed_complement_values(complement, 'complement') is None

    def test_a_datatype_is_never_written_without_a_value(self):
        """Asserting a type for a value that is not there is
        worse than the absence of both. The string is still on the node as
        `value`, where every existing consumer reads it."""
        complement = attribute_fact(value='nineteen ninety four').complement
        assert _typed_complement_values(complement, 'complement') is None

    def test_a_falsy_value_is_still_written(self):
        complement = attribute_fact(value='false', datatype=f'{XSD}boolean').complement
        assert _typed_complement_values(complement, 'complement') == (False, f'{XSD}boolean')

class TestWhatReachesTheGraph:

    def test_off_issues_no_typed_write(self):
        assert not [query for query in queries(build(attribute_fact(), 'off')) if 'typed' in query]

    def test_subject_placement_sets_the_property_under_its_own_name(self):
        typed = [(q, p) for (q, p) in build(attribute_fact(), 'subject') if 'insert typed property' in q]

        assert len(typed) == 1
        (query, params) = typed[0]
        assert 'SET' in query and '`foundedYear`' in query
        assert row(params)['typedValue'] == 1994
        assert row(params)['entityId'] == 'e1'

    def test_the_value_is_bound_and_never_interpolated(self):
        """The key is a Cypher identifier and is escaped; the
        value is a parameter."""
        typed = [(q, p) for (q, p) in build(attribute_fact(value='1994'), 'subject')
                 if 'insert typed property' in q]

        (query, params) = typed[0]
        assert '1994' not in query
        assert row(params)['typedValue'] == 1994

    def test_complement_placement_sets_typed_value_and_datatype(self):
        typed = [(q, p) for (q, p) in build(attribute_fact(), 'complement')
                 if 'typed_value' in q or 'typedValue' in str(p)]

        assert typed
        (_, params) = typed[0]
        assert row(params)['typedValue'] == 1994
        assert row(params)['datatype'] == f'{XSD}integer'

    def test_the_entity_insert_is_byte_identical_at_every_placement(self):
        """The byte-for-byte guarantee, and the reason each typed write is a separate
        query rather than an extra `SET` on the insert.

        Turning typed properties on must not be able to disturb `value`,
        `search_str` or `class`, and this is what makes that structural rather than
        a promise.
        """
        inserts = {
            placement: [q for q in queries(build(attribute_fact(), placement)) if 'insert entities' in q]
            for placement in ('off', 'subject', 'complement', 'both')
        }

        assert inserts['off']
        for placement in ('subject', 'complement', 'both'):
            assert inserts[placement] == inserts['off']

    def test_a_fact_with_no_annotations_warns_once_when_a_placement_was_asked_for(self, caplog):
        """A build that silently writes no typed properties is the failure worth a
        log line: the setting is on, and nothing arrives."""
        import graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder as module

        module._reset_no_annotations_warning()

        with caplog.at_level(logging.WARNING):
            for _ in range(60):
                build(attribute_fact(canonical_name=None, datatype=None), 'subject')

        assert caplog.text.count('typed_properties=') == 1

    def test_typed_properties_defaults_to_off_when_the_caller_omits_it(self):
        """A pipeline built before this setting existed, or a
        builder called directly, must not raise a KeyError from a feature it never
        asked for."""
        from llama_index.core.schema import TextNode

        store = RecordingGraphStore()
        EntityGraphBuilder().build(
            TextNode(text='x', metadata={'fact': attribute_fact().model_dump()}),
            store,
            include_domain_labels=False,
            include_local_entities=True,
        )

        assert not [query for query in queries(store.calls) if 'typed' in query]

class TestTheSurroundingWriteConditions:
    """The branches a placement runs inside, which decide whether it runs at all."""

    def test_a_local_entity_subject_is_skipped_when_local_entities_are_off(self):
        """Nothing is written for a fact whose subject is itself a value node, so
        the typed write cannot conjure the node the setting suppressed."""
        fact = attribute_fact()
        fact.subject.classification = LOCAL

        assert build(fact, 'subject', include_local_entities=False) == []
        assert build(fact, 'subject', include_local_entities=True)

    def test_the_complement_node_is_only_inserted_when_local_entities_are_on(self):
        calls = build(attribute_fact(), 'complement', include_local_entities=False)

        assert not [q for (q, _) in calls if 'typed_value' in q]

    def test_domain_labels_are_added_alongside_a_typed_write(self):
        """The two features are independent, and both write to the same node."""
        from llama_index.core.schema import TextNode

        store = RecordingGraphStore()
        EntityGraphBuilder().build(
            TextNode(text='x', metadata={'fact': attribute_fact().model_dump()}),
            store,
            include_domain_labels=True,
            include_local_entities=True,
            typed_properties='subject',
        )

        labelled = [q for (q, _) in store.calls if 'awsqid:' in q]
        typed = [q for (q, _) in store.calls if 'insert typed property' in q]

        assert labelled and typed

    def test_a_local_entity_never_gets_a_domain_label(self):
        """Its classification is a marker, not a type anyone would query by."""
        from llama_index.core.schema import TextNode

        store = RecordingGraphStore()
        EntityGraphBuilder().build(
            TextNode(text='x', metadata={'fact': attribute_fact().model_dump()}),
            store,
            include_domain_labels=True,
            include_local_entities=True,
            typed_properties='complement',
        )

        assert not [q for (q, _) in store.calls if f'`{LOCAL}`' in q]

    def test_a_node_carrying_no_fact_warns_and_writes_nothing(self, caplog):
        from llama_index.core.schema import TextNode

        store = RecordingGraphStore()
        with caplog.at_level(logging.WARNING):
            EntityGraphBuilder().build(
                TextNode(text='x', metadata={}), store,
                include_domain_labels=False, include_local_entities=True,
                typed_properties='subject',
            )

        assert store.calls == []
        assert 'fact_id missing' in caplog.text

    def test_an_annotated_fact_silences_the_no_annotations_warning_permanently(self, caplog):
        """One annotated fact proves the filter ran, so the warning can never be
        right afterwards however many unannotated facts follow."""
        from graphrag_toolkit.lexical_graph.indexing.build import entity_graph_builder as module

        module._reset_no_annotations_warning()
        build(attribute_fact(), 'subject')

        with caplog.at_level(logging.WARNING):
            for _ in range(60):
                build(attribute_fact(canonical_name=None, datatype=None), 'subject')

        assert 'typed_properties=' not in caplog.text
