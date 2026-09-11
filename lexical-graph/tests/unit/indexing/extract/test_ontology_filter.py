# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The filter: the one component in this feature that changes extracted data.

Every test here runs the component the way the pipeline does - through
`__call__` on a node carrying `TOPICS_KEY` - so the `model_validate` /
`model_dump` round trip is exercised rather than bypassed. Facts are built by
hand: what is being asserted is what the code does with a given fact, which is
not a question about any model's output.

The gates are independent by design, so each one is tested with the others off.
That is the whole point of the per-dimension escape hatches: `enforce_domain_range`
alone must not start rejecting unresolvable classifications.
"""

import logging
import pickle
from pathlib import Path

import pytest
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import Ontology
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_filter import (
    FilterCounters,
    OntologyFilter,
    _warned_unvalidated_datatypes,
    authored_name,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_index import (
    DatatypeProperty,
    ObjectProperty,
    OntologyClass,
    OntologyIndex,
)
from graphrag_toolkit.lexical_graph.indexing.model import (
    Entity,
    Fact,
    Relation,
    Statement,
    Topic,
    TopicCollection,
)

FIXTURES = Path(__file__).parent.parent.parent.parent / 'fixtures' / 'ontologies'
NS = 'http://example.com/company#'
XSD = 'http://www.w3.org/2001/XMLSchema#'
LOCAL = '__Local_Entity__'

@pytest.fixture(scope='module')
def index():
    return Ontology.load(FIXTURES / 'company.ttl').index()

def attribute(predicate, value='1994', subject_class='Company', subject='Meridian Freight'):
    """A subject-predicate-complement fact, as the parser leaves an attribute."""
    return Fact(
        subject=Entity(value=subject, classification=subject_class),
        predicate=Relation(value=predicate),
        complement=Entity(value=value, classification=LOCAL),
    )

def relation(predicate, subject_class='Person', object_class='Company'):
    return Fact(
        subject=Entity(value='Priya Raman', classification=subject_class),
        predicate=Relation(value=predicate),
        object=Entity(value='Meridian Freight', classification=object_class),
    )

def run(filter_, *facts, entities=None):
    """Put facts through the component the way the pipeline does."""
    topics = TopicCollection(topics=[Topic(
        value='t',
        entities=list(entities or []),
        statements=[Statement(value='s', facts=list(facts))],
    )])
    node = TextNode(text='x', metadata={TOPICS_KEY: topics.model_dump()})

    returned = filter_([node])

    assert returned[0] is node
    return TopicCollection.model_validate(node.metadata[TOPICS_KEY]).topics[0]

def facts_of(topic):
    return [fact for statement in topic.statements for fact in statement.facts]

def kept(index, fact, **flags):
    return len(facts_of(run(OntologyFilter(index=index, **flags), fact))) == 1

def counted(index, *facts, **flags):
    """The counters for one topic, which `__call__` accumulates and then discards."""
    counters = FilterCounters()
    topic = Topic(value='t', statements=[Statement(value='s', facts=list(facts))])
    OntologyFilter(index=index, **flags)._filter_topic(topic, counters)
    return counters

class TestNormalization:
    """A resolved name is stored as the ontology's author wrote it."""

    def test_a_resolved_predicate_takes_the_authored_spelling(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index, normalize_names=True), relation('WORKS FOR')))
        assert fact.predicate.value == 'worksFor'

    def test_a_resolved_classification_takes_the_authored_spelling(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index, normalize_names=True), relation('WORKS FOR', subject_class='ATHLETE')))
        assert fact.subject.classification == 'Athlete'

    def test_a_declared_label_wins_over_the_local_name(self, index):
        [fact] = facts_of(run(
            OntologyFilter(index=index, normalize_names=True),
            relation('WORKS FOR', object_class='SPORTS TEAM'),
        ))
        assert fact.object.classification == 'Sports Team'

    def test_the_authored_name_is_verbatim_and_gets_no_house_convention(self, index):
        """The stored spelling is deliberately not the prompt's rendering.

        `:worksFor` renders as `WORKS_FOR` for the model and stores as `worksFor`.
        Collapsing the two into one helper is the bug `naming.py` exists to
        prevent.
        """
        assert authored_name(index.object_properties[f'{NS}worksFor']) == 'worksFor'
        assert authored_name(index.classes[f'{NS}SportsTeam']) == 'Sports Team'
        assert authored_name(index.classes[f'{NS}Athlete']) == 'Athlete'

    def test_an_unresolved_name_is_left_exactly_as_the_parser_produced_it(self, index):
        [fact] = facts_of(run(
            OntologyFilter(index=index, normalize_names=True),
            relation('HIRED BY', subject_class='BOARD MEMBER'),
        ))
        assert (fact.predicate.value, fact.subject.classification) == ('HIRED BY', 'BOARD MEMBER')

    def test_nothing_is_rewritten_with_normalize_names_off(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index), relation('WORKS FOR', subject_class='PERSON')))
        assert (fact.predicate.value, fact.subject.classification) == ('WORKS FOR', 'PERSON')

    def test_the_topic_entity_list_is_normalized_and_annotated_too(self, index):
        """`topic.entities` is a separate view of the same names after the round
        trip, so relying on object sharing to propagate a rewrite would pass in a
        unit test and fail in the pipeline."""
        topic = run(
            OntologyFilter(index=index, normalize_names=True),
            relation('WORKS FOR', subject_class='PERSON'),
            entities=[Entity(value='Priya Raman', classification='PERSON')],
        )

        assert topic.entities[0].classification == 'Person'
        assert topic.entities[0].classIri == f'{NS}Person'

    def test_an_entity_is_never_pruned_from_the_topic(self, index):
        """That list is not written to the graph, and pruning it
        would change entity extraction rather than fact conformance."""
        topic = run(
            OntologyFilter(index=index, normalize_names=True, enforce_entity_types=True),
            entities=[Entity(value='A Trail', classification='TRAIL')],
        )
        assert [entity.classification for entity in topic.entities] == ['TRAIL']

    def test_normalization_is_idempotent(self, index):
        """Resolution runs before normalization, which is only sound if resolving
        an already-authored name returns the same term."""
        filter_ = OntologyFilter(index=index, normalize_names=True)
        once = facts_of(run(filter_, relation('WORKS FOR', subject_class='PERSON')))
        twice = facts_of(run(filter_, once[0]))

        assert twice[0].model_dump() == once[0].model_dump()

class TestEnforceEntityTypes:

    @pytest.mark.parametrize('subject_class,object_class,keep', [
        ('Person', 'Company', True),
        ('Vehicle', 'Company', False),
        ('Person', 'Vehicle', False),
    ])
    def test_a_classification_that_does_not_resolve_is_dropped(self, index, subject_class, object_class, keep):
        fact = relation('WORKS FOR', subject_class=subject_class, object_class=object_class)
        assert kept(index, fact, enforce_entity_types=True) is keep

    def test_an_attribute_has_no_object_to_check(self, index):
        assert kept(index, attribute('FOUNDED YEAR'), enforce_entity_types=True)

class TestEnforceRelationshipTypes:

    @pytest.mark.parametrize('fact,keep', [
        ('WORKS FOR', True),
        ('HIRED BY', False),
    ])
    def test_a_predicate_that_does_not_resolve_is_dropped(self, index, fact, keep):
        assert kept(index, relation(fact), enforce_relationship_types=True) is keep

    @pytest.mark.parametrize('predicate,keep', [('FOUNDED YEAR', True), ('CONDITION', False)])
    def test_the_attribute_shape_too(self, index, predicate, keep):
        assert kept(index, attribute(predicate), enforce_relationship_types=True) is keep

    def test_a_relation_whose_employer_was_not_a_named_entity_still_resolves(self, index):
        """The parser emits a complement whenever it could not match the object
        text to an entity it had already seen, so a genuine object property
        arrives attribute-shaped and must not be read as undeclared."""
        assert kept(index, attribute('WORKS FOR', value='Meridian Freight'), enforce_relationship_types=True)

class TestEnforceDomainAndRange:

    @pytest.mark.parametrize('subject_class,object_class,keep', [
        ('Person', 'Company', True),
        ('Athlete', 'Sports Team', True),
        ('Company', 'Company', False),
        ('Person', 'Person', False),
    ])
    def test_a_declared_domain_and_range_bound_both_ends(self, index, subject_class, object_class, keep):
        fact = relation('WORKS FOR', subject_class=subject_class, object_class=object_class)
        assert kept(index, fact, enforce_domain_range=True) is keep

    def test_subclass_closure_is_honoured_in_both_directions(self, index):
        """`:playsFor` is narrower than `:worksFor`, so a Person and a Company do
        not satisfy it even though an Athlete and a Sports Team do."""
        assert kept(index, relation('PLAYS FOR', 'Athlete', 'Sports Team'), enforce_domain_range=True)
        assert not kept(index, relation('PLAYS FOR', 'Person', 'Company'), enforce_domain_range=True)

    def test_an_undeclared_domain_or_range_constrains_nothing(self, index):
        assert kept(index, relation('ACQUIRED', 'Person', 'Person'), enforce_domain_range=True)

    def test_a_datatype_property_has_only_a_domain_to_check(self, index):
        assert kept(index, attribute('FOUNDED YEAR', subject_class='Sports Team'), enforce_domain_range=True)
        assert not kept(index, attribute('FOUNDED YEAR', subject_class='Person'), enforce_domain_range=True)
        assert kept(index, attribute('OFFICIAL NAME', value='x', subject_class='Person'), enforce_domain_range=True)

    def test_an_unresolved_class_is_unknown_rather_than_violating(self, index):
        """A classification that resolves to nothing cannot be *shown* to breach a
        declared domain, so this gate passes it and only `enforce_entity_types`
        rejects it."""
        assert kept(index, relation('WORKS FOR', subject_class='Vehicle'), enforce_domain_range=True)

    def test_an_unresolved_predicate_has_no_domain_to_violate(self, index):
        assert kept(index, relation('HIRED BY', subject_class='Company'), enforce_domain_range=True)

    def test_this_gate_checks_types_and_never_meaning(self, index):
        """the types-not-meaning limit, stated as a test so nobody reads more into the gate.

        A board member recorded as an employee satisfies `:worksFor` exactly, and
        is kept at every level. Domain and range bound which entities a property
        may relate; they cannot see a wrong predicate whose endpoints fit.
        """
        assert kept(index, relation('WORKS FOR', 'Person', 'Company'), enforce_domain_range=True)

class TestEnforceDatatypes:

    @pytest.mark.parametrize('value,keep', [
        ('1994', True),
        ('1,994', True),
        ('nineteen ninety four', False),
        ('1994.5', False),
    ])
    def test_a_literal_that_does_not_parse_as_the_declared_type_is_dropped(self, index, value, keep):
        assert kept(index, attribute('FOUNDED YEAR', value=value), enforce_datatypes=True) is keep

    def test_a_string_range_accepts_what_a_string_range_promises(self, index):
        assert kept(index, attribute('TICKER SYMBOL', value='MFR'), enforce_datatypes=True)

    def test_a_relation_has_no_declared_datatype(self, index):
        assert kept(index, relation('WORKS FOR'), enforce_datatypes=True)

    def test_an_unenforceable_declaration_keeps_the_fact_and_says_so(self, caplog):
        """A value stored without validation must not look like a
        validated one, so the WARN is unconditional on `report_violations`."""
        _warned_unvalidated_datatypes.clear()
        index = OntologyIndex(datatype_properties={'urn:x#checksum': DatatypeProperty(
            iri='urn:x#checksum', local_name='checksum', datatype=f'{XSD}hexBinary',
        )})

        with caplog.at_level(logging.WARNING):
            assert kept(index, attribute('CHECKSUM', value='not hex at all'), enforce_datatypes=True)

        assert 'cannot validate' in caplog.text
        assert f'{XSD}hexBinary' in caplog.text

    def test_the_warning_is_emitted_once_per_datatype(self, caplog):
        _warned_unvalidated_datatypes.clear()
        index = OntologyIndex(datatype_properties={'urn:x#checksum': DatatypeProperty(
            iri='urn:x#checksum', local_name='checksum', datatype=f'{XSD}hexBinary',
        )})
        filter_ = OntologyFilter(index=index, enforce_datatypes=True)

        with caplog.at_level(logging.WARNING):
            run(filter_, attribute('CHECKSUM', value='a'), attribute('CHECKSUM', value='b'))

        assert caplog.text.count('cannot validate') == 1

class TestTheGatesAreIndependent:

    def test_one_gate_does_not_imply_another(self, index):
        """A fact that breaches only entity types survives every other gate."""
        fact = relation('WORKS FOR', subject_class='Vehicle')

        assert not kept(index, fact, enforce_entity_types=True)
        for gate in ('enforce_relationship_types', 'enforce_domain_range', 'enforce_datatypes'):
            assert kept(index, fact, **{gate: True})

    def test_nothing_is_dropped_with_every_gate_off(self, index):
        assert kept(index, relation('HIRED BY', subject_class='Vehicle', object_class='Vehicle'))

    def test_a_fact_breaching_two_dimensions_is_dropped_once(self, index):
        """The counters break down *drops*, so they must sum to facts lost."""
        counters = counted(
            index,
            relation('HIRED BY', subject_class='Vehicle'),
            enforce_entity_types=True,
            enforce_relationship_types=True,
        )

        assert counters.facts_dropped() == 1
        assert counters.facts_dropped_entity_type == 1
        assert counters.facts_dropped_relationship_type == 0

class TestDropTypeRestatements:
    """The only dropping gate on at `align`, so what it drops has to be exact."""

    @pytest.mark.parametrize('predicate', ['rdf:type', 'rdfs:subClassOf', 'owl:sameAs', 'skos:prefLabel'])
    def test_ontology_language_is_dropped_whatever_the_value(self, index, predicate):
        assert not kept(index, attribute(predicate, value='anything'), drop_type_restatements=True)
        assert not kept(index, relation(predicate), drop_type_restatements=True)

    def test_a_bare_word_sharing_a_local_name_is_kept(self, index):
        """Only the prefixed form is language. `RANGE` is also a real attribute of
        a delivery van, and `DOMAIN` of a website."""
        assert kept(index, attribute('RANGE', value='400 miles'), drop_type_restatements=True)
        assert kept(index, attribute('DOMAIN', value='meridian.example'), drop_type_restatements=True)

    @pytest.mark.parametrize('predicate', ['TYPE', 'CLASSIFICATION', 'IS A', 'is_a', 'DESCRIBED BY', 'CATEGORY'])
    def test_a_value_restating_the_subjects_own_class_is_dropped(self, index, predicate):
        assert not kept(index, attribute(predicate, value='Company'), drop_type_restatements=True)

    @pytest.mark.parametrize('value', ['company', 'COMPANY'])
    def test_the_comparison_ignores_case_and_separators(self, index, value):
        assert not kept(index, attribute('CLASSIFICATION', value=value), drop_type_restatements=True)

    def test_a_type_asserting_name_carrying_real_content_is_kept(self, index):
        """Both conditions are necessary: a name-only rule would destroy these."""
        assert kept(index, attribute('CLASSIFICATION', value='football club'), drop_type_restatements=True)
        assert kept(index, attribute('TYPE', value='haulage contractor'), drop_type_restatements=True)

    def test_a_predicate_that_is_not_type_asserting_is_kept(self, index):
        """Even when the value repeats the class. Vagueness is not this gate's business."""
        assert kept(index, attribute('COMPETES WITH', value='Company'), drop_type_restatements=True)
        assert kept(index, attribute('OCCUPATION', value='Company'), drop_type_restatements=True)

    def test_the_relation_shape_is_left_alone(self, index):
        """The object then has its own identity in the graph, so dropping the edge
        would not be the information-preserving move the attribute case is."""
        assert kept(index, relation('TYPE', subject_class='Company'), drop_type_restatements=True)

    @pytest.mark.parametrize('value', [' ', ''])
    def test_an_empty_value_cannot_be_shown_to_duplicate_anything(self, index, value):
        assert kept(index, attribute('TYPE', value=value), drop_type_restatements=True)

    def test_a_fact_with_neither_object_nor_complement_is_left_alone(self, index):
        fact = Fact(
            subject=Entity(value='Meridian Freight', classification='Company'),
            predicate=Relation(value='TYPE'),
        )
        assert kept(index, fact, drop_type_restatements=True)

    def test_a_declared_predicate_is_never_touched(self):
        """An author who declares `:classification` has said what it means."""
        index = OntologyIndex(
            classes={'urn:x#Company': OntologyClass(iri='urn:x#Company', local_name='Company')},
            datatype_properties={'urn:x#classification': DatatypeProperty(
                iri='urn:x#classification', local_name='classification', datatype=f'{XSD}string',
            )},
        )
        assert kept(index, attribute('CLASSIFICATION', value='Company'), drop_type_restatements=True)

    def test_nothing_is_dropped_with_the_flag_off(self, index):
        assert kept(index, attribute('rdf:type', value='Company'))
        assert kept(index, attribute('CLASSIFICATION', value='Company'))

class TestAnnotation:
    """Written once here so the build stage reads an answer."""

    def test_a_resolved_class_records_its_iri(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index), relation('WORKS FOR')))
        assert (fact.subject.classIri, fact.object.classIri) == (f'{NS}Person', f'{NS}Company')

    def test_a_resolved_predicate_records_its_iri_and_canonical_name(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index), attribute('FOUNDED YEAR')))
        assert fact.predicate.propertyIri == f'{NS}foundedYear'
        assert fact.predicate.canonicalName == 'foundedYear'

    def test_a_datatype_property_records_the_datatype_on_the_complement(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index), attribute('FOUNDED YEAR')))
        assert fact.complement.datatype == f'{XSD}integer'

    def test_an_object_property_leaves_no_datatype(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index), relation('WORKS FOR')))
        assert fact.object.datatype is None

    def test_annotation_is_not_gated_on_any_flag(self, index):
        """This is how typed storage works at `off`."""
        [fact] = facts_of(run(OntologyFilter(index=index), attribute('FOUNDED YEAR')))
        assert fact.predicate.propertyIri is not None

    def test_an_unresolved_term_is_annotated_with_nothing(self, index):
        [fact] = facts_of(run(OntologyFilter(index=index), relation('HIRED BY', subject_class='Vehicle')))
        assert (fact.predicate.propertyIri, fact.subject.classIri) == (None, None)

    def test_the_canonical_name_is_the_local_name_and_not_the_label(self):
        """It is the key typed storage writes under, and an `rdfs:label` may
        contain spaces."""
        index = OntologyIndex(datatype_properties={'urn:x#founded': DatatypeProperty(
            iri='urn:x#founded', local_name='founded', label='Founded Year', datatype=f'{XSD}integer',
        )})

        [fact] = facts_of(run(
            OntologyFilter(index=index, normalize_names=True), attribute('FOUNDED YEAR'),
        ))

        assert fact.predicate.value == 'Founded Year'
        assert fact.predicate.canonicalName == 'founded'

    def test_every_annotation_is_a_plain_string(self, index):
        """The metadata is written through `json.dump` and revalidated strictly."""
        [fact] = facts_of(run(OntologyFilter(index=index), attribute('FOUNDED YEAR')))
        for value in (fact.subject.classIri, fact.predicate.propertyIri,
                      fact.predicate.canonicalName, fact.complement.datatype):
            assert type(value) is str

class TestCounters:

    def test_the_two_rewrite_kinds_are_counted_apart(self, index):
        """A classification rewrite changes entity *identity*, since the id hashes
        the classification in; a predicate rewrite only changes an edge label."""
        counters = counted(index, relation('WORKS FOR', subject_class='PERSON'), normalize_names=True)

        assert counters.predicates_rewritten == 1
        assert counters.classifications_rewritten == 1

    @pytest.mark.parametrize('fact,flag,field', [
        (attribute('rdf:type'), 'drop_type_restatements', 'facts_dropped_type_restatement'),
        (relation('WORKS FOR', subject_class='Vehicle'), 'enforce_entity_types', 'facts_dropped_entity_type'),
        (relation('HIRED BY'), 'enforce_relationship_types', 'facts_dropped_relationship_type'),
        (relation('WORKS FOR', subject_class='Company'), 'enforce_domain_range', 'facts_dropped_domain_range'),
        (attribute('FOUNDED YEAR', value='x'), 'enforce_datatypes', 'facts_dropped_datatype'),
    ])
    def test_each_drop_is_counted_under_the_setting_that_caused_it(self, index, fact, flag, field):
        counters = counted(index, fact, **{flag: True})

        assert getattr(counters, field) == 1
        assert counters.facts_dropped() == 1

    def test_the_total_is_summed_from_the_dimensions(self, index):
        """Derived rather than hand-written, so a new gate cannot arrive with a
        counter the total omits."""
        counters = counted(
            index,
            attribute('rdf:type'),
            relation('HIRED BY'),
            drop_type_restatements=True,
            enforce_relationship_types=True,
        )
        assert counters.facts_dropped() == 2

    def test_any_change_ignores_annotation(self, index):
        """Annotation happens to every surviving fact at every level, so counting
        it would make every call a change and the report carry no signal."""
        assert not counted(index, attribute('FOUNDED YEAR')).any_change()
        assert counted(index, relation('WORKS FOR'), normalize_names=True).any_change()

    def test_the_summary_names_only_the_gates_that_dropped_something(self, index):
        summary = counted(index, relation('HIRED BY'), enforce_relationship_types=True).summary()

        assert 'facts dropped: 1' in summary
        assert 'enforce_relationship_types: 1' in summary
        assert 'enforce_datatypes' not in summary

    def test_the_summary_always_carries_the_three_totals(self, index):
        summary = counted(index, attribute('FOUNDED YEAR')).summary()

        assert 'classifications rewritten: 0' in summary
        assert 'predicates rewritten: 0' in summary
        assert 'facts dropped: 0' in summary
        assert 'dropped by' not in summary

class TestTheComponentContract:

    def test_a_node_without_topics_passes_through_untouched(self, index):
        node = TextNode(text='x', metadata={'other': 1})
        assert OntologyFilter(index=index, normalize_names=True)([node])[0].metadata == {'other': 1}

    def test_the_topics_key_is_the_only_key_read_or_written(self, index):
        topics = TopicCollection(topics=[Topic(value='t')])
        node = TextNode(text='x', metadata={TOPICS_KEY: topics.model_dump(), 'keep': 'me'})

        OntologyFilter(index=index, normalize_names=True)([node])

        assert node.metadata['keep'] == 'me'
        assert set(node.metadata) == {TOPICS_KEY, 'keep'}

    def test_a_topic_is_never_dropped(self, index):
        topics = TopicCollection(topics=[Topic(value='empty'), Topic(value='also empty')])
        node = TextNode(text='x', metadata={TOPICS_KEY: topics.model_dump()})

        OntologyFilter(index=index, enforce_entity_types=True)([node])

        assert len(TopicCollection.model_validate(node.metadata[TOPICS_KEY]).topics) == 2

    def test_the_component_survives_the_spawn_boundary(self, index):
        """Extraction pickles the component per node batch per worker, so a filter
        that cannot round-trip is a filter that does not run."""
        filter_ = OntologyFilter(index=index, normalize_names=True, enforce_datatypes=True)

        revived = pickle.loads(pickle.dumps(filter_))

        assert revived.normalize_names is True
        assert revived.enforce_datatypes is True
        assert revived.index.resolve_class('Company').iri == f'{NS}Company'
        assert facts_of(run(revived, relation('WORKS FOR')))[0].predicate.value == 'worksFor'

    def test_the_serialization_name_is_stable(self, index):
        """llama-index records it in a serialized pipeline, so it may not drift
        with the class name."""
        assert OntologyFilter.class_name() == 'OntologyFilter'

    def test_a_bare_string_complement_is_read_the_same_as_an_entity(self, index):
        """`Fact.complement` is `Union[Entity, str]` and both forms occur: the
        parser builds an `Entity`, an older or hand-built payload may carry a str."""
        fact = Fact(
            subject=Entity(value='Meridian Freight', classification='Company'),
            predicate=Relation(value='TYPE'),
            complement='Company',
        )
        assert not kept(index, fact, drop_type_restatements=True)

    def test_the_index_holds_no_rdflib_term(self, index):
        """What crosses the boundary is `str`, `list`, `dict` and `frozenset`."""
        for value in index.model_dump().values():
            assert type(value) in (dict, list, str)

class TestReporting:

    def test_a_call_that_changed_something_logs_at_info(self, index, caplog):
        with caplog.at_level(logging.INFO):
            run(
                OntologyFilter(index=index, normalize_names=True, report_violations=True),
                relation('WORKS FOR', subject_class='PERSON'),
            )

        assert 'Ontology filter' in caplog.text
        assert 'nodes: 1' in caplog.text
        assert 'predicates rewritten: 1' in caplog.text

    def test_a_call_that_changed_nothing_does_not_log_at_info(self, index, caplog):
        """Every gate is off by default, so a line of zeros per batch would bury
        the batches that did something."""
        with caplog.at_level(logging.INFO):
            run(OntologyFilter(index=index, report_violations=True), attribute('FOUNDED YEAR'))

        assert 'Ontology filter' not in caplog.text

    def test_nothing_is_logged_without_report_violations(self, index, caplog):
        with caplog.at_level(logging.INFO):
            run(
                OntologyFilter(index=index, normalize_names=True),
                relation('WORKS FOR', subject_class='PERSON'),
            )

        assert 'Ontology filter' not in caplog.text

    def test_the_line_says_how_many_nodes_it_covers(self, index, caplog):
        """One `__call__` is the largest unit available on the far side of the
        spawn boundary, so a reader has to be able to add the lines up."""
        topics = TopicCollection(topics=[Topic(value='t', statements=[Statement(
            value='s', facts=[relation('WORKS FOR', subject_class='PERSON')],
        )])])
        nodes = [
            TextNode(text='a', metadata={TOPICS_KEY: topics.model_dump()}),
            TextNode(text='b', metadata={TOPICS_KEY: topics.model_dump()}),
            TextNode(text='c', metadata={'other': 1}),
        ]

        with caplog.at_level(logging.INFO):
            OntologyFilter(index=index, normalize_names=True, report_violations=True)(nodes)

        assert 'nodes: 2' in caplog.text

class TestAnIndexWithNoTerms:
    """The degenerate ontology, which must be inert rather than fatal."""

    def test_nothing_resolves_and_nothing_is_annotated(self):
        [fact] = facts_of(run(OntologyFilter(index=OntologyIndex(), normalize_names=True), relation('WORKS FOR')))

        assert fact.predicate.value == 'WORKS FOR'
        assert fact.predicate.propertyIri is None

    def test_every_gate_drops_everything_it_can_be_asked_about(self):
        index = OntologyIndex()

        assert not kept(index, relation('WORKS FOR'), enforce_entity_types=True)
        assert not kept(index, relation('WORKS FOR'), enforce_relationship_types=True)
        assert kept(index, relation('WORKS FOR'), enforce_domain_range=True)
        assert kept(index, relation('WORKS FOR'), enforce_datatypes=True)

    def test_a_property_with_no_domain_declared_still_annotates(self):
        index = OntologyIndex(object_properties={'urn:x#acquired': ObjectProperty(
            iri='urn:x#acquired', local_name='acquired',
        )})
        [fact] = facts_of(run(OntologyFilter(index=index, enforce_domain_range=True), relation('ACQUIRED')))

        assert fact.predicate.propertyIri == 'urn:x#acquired'
