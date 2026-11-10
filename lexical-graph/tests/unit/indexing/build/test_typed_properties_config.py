# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""How the `typed_properties` setting is plumbed, and what refuses it.

The setting exists in one place a user can set it, reaches `builder.build()` from
there through `LexicalGraphIndex` and `BuildPipeline`, defaults to `'off'` at every
layer, and the two configurations that cannot work are refused up front rather than
half-performed.

Much of what these tests assert is an absence, which is the point: the feature is
opt-in, and a user who does not opt in must be unaffected.

The four things under test, and where each lives:

* `OntologyConfig.typed_properties` - the only place a user can set it, plus the
  reserved-name refusal;
* `GraphRAGConfig.typed_properties` - a `coalesce` floor only, deliberately not
  environment-readable;
* `BuildPipeline` - validation of the placement, and the
  complement-without-local-entities refusal;
* `EntityGraphBuilder` - reads the kwarg tolerantly, and warns when it was asked
  for and cannot be honoured.

What each placement actually *writes* is in `test_typed_property_values.py` and
`test_local_entity_rewrites_typed_carry.py`.
"""

import logging
from typing import get_args
from unittest.mock import MagicMock, patch

import pytest

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.build import entity_graph_builder as entity_graph_builder_module
from graphrag_toolkit.lexical_graph.indexing.build.build_pipeline import BuildPipeline
from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import EntityGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.build.graph_construction import GraphConstruction, default_builders
from graphrag_toolkit.lexical_graph.indexing.constants import (
    COMPLEMENT_ENTITY_PROPERTIES,
    COMPLEMENT_PLACEMENTS,
    RESERVED_ENTITY_PROPERTIES,
    SUBJECT_PLACEMENTS,
    TYPED_PROPERTIES_OFF,
    TYPED_PROPERTY_PLACEMENTS,
)

from llama_index.core.schema import TextNode


# A minimal ontology declaring a datatype property named `value`, which is a name
# `__Entity__` already owns. Written inline rather than added to
# `tests/fixtures/ontologies/` because it is not a plausible ontology - its only
# purpose is to be rejected.
RESERVED_NAME_TURTLE = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://example.org/reserved#> .

: a owl:Ontology .

:Thing a owl:Class ; rdfs:label "Thing" .

:value a owl:DatatypeProperty ;
    rdfs:label "value" ;
    rdfs:domain :Thing ;
    rdfs:range xsd:string .
"""

# One well-formed fact, annotated as `OntologyFilter` would leave it.
ANNOTATED_FACT = {
    'factId': 'f-1',
    'subject': {
        'entityId': 's-1', 'value': 'Amazon', 'classification': 'Company',
        'classIri': 'http://example.org/company#Company',
    },
    'predicate': {
        'value': 'FOUNDED YEAR',
        'propertyIri': 'http://example.org/company#foundedYear',
        'canonicalName': 'foundedYear',
    },
    'complement': {
        'entityId': 'c-1', 'value': '1994', 'classification': '__Local_Entity__',
        'datatype': 'http://www.w3.org/2001/XMLSchema#integer',
    },
}

UNANNOTATED_FACT = {
    'factId': 'f-2',
    'subject': {'entityId': 's-2', 'value': 'Amazon', 'classification': 'Company'},
    'predicate': {'value': 'acquired'},
    'object': {'entityId': 'o-2', 'value': 'Whole Foods', 'classification': 'Company'},
}

def fact_node(fact:dict, node_id:str='n-1') -> TextNode:
    node = TextNode(text='', id_=node_id)
    node.metadata = {'fact': fact}
    return node

@pytest.fixture
def reserved_name_ontology():
    '''
    An ontology declaring `:value a owl:DatatypeProperty`.
    '''
    from graphrag_toolkit.lexical_graph.indexing.extract.ontology import Ontology
    return Ontology.from_turtle_string(RESERVED_NAME_TURTLE, base_iri='http://example.org/reserved#')

@pytest.fixture
def reset_annotation_warning():
    '''
    Clear `entity_graph_builder`'s module-level keeping graph writes out of ambient configuration3 state around a
    test. It is process-wide by design, so a test that trips it would otherwise
    leak into every test that runs after it.
    '''
    entity_graph_builder_module._reset_no_annotations_warning()
    yield
    entity_graph_builder_module._reset_no_annotations_warning()

@pytest.fixture
def restore_graphrag_typed_properties():
    '''
    Restore `GraphRAGConfig`'s typed_properties around a test that sets it. The
    config is a process-wide singleton.
    '''
    previous = GraphRAGConfig._typed_properties
    yield
    GraphRAGConfig._typed_properties = previous

class TestPlacementVocabulary:
    '''
    Tests that the placement names agree everywhere they are spelled out.
    '''

    def test_literal_and_tuple_agree(self):
        '''
        `TypedProperties` is what a type checker sees and `TYPED_PROPERTY_PLACEMENTS`
        is what validation checks, and they live in different modules - the
        `Literal` in `ontology_config`, the tuple in `indexing.constants`, because
        `build_pipeline` must read the tuple without importing rdflib. Nothing but
        this test keeps them in step.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import TypedProperties

        assert get_args(TypedProperties) == TYPED_PROPERTY_PLACEMENTS

    def test_placement_subsets_cover_every_placement_but_off(self):
        '''
        Every placement other than `'off'` writes somewhere, and `'off'` writes
        nowhere. A placement in neither subset would be silently inert.
        '''
        writing = set(SUBJECT_PLACEMENTS) | set(COMPLEMENT_PLACEMENTS)

        assert writing == set(TYPED_PROPERTY_PLACEMENTS) - {TYPED_PROPERTIES_OFF}
        assert TYPED_PROPERTIES_OFF not in writing

    def test_both_is_in_both_subsets(self):
        '''
        `'both'` is not a fallback chain: it writes to the subject *and* to the
        complement.
        '''
        assert 'both' in SUBJECT_PLACEMENTS
        assert 'both' in COMPLEMENT_PLACEMENTS

class TestDefaultsToOff:
    '''
    Tests that the default is `'off'` at every layer.
    '''

    def test_graphrag_config_default(self):
        '''
        The `coalesce` floor.
        '''
        assert GraphRAGConfig.typed_properties == TYPED_PROPERTIES_OFF

    def test_no_environment_variable_can_turn_it_on(self, monkeypatch, restore_graphrag_typed_properties):
        '''
        No ambient configuration turns on graph writes. The
        property is deliberately not environment-readable, unlike its neighbours,
        so that a user with no ontology cannot reach any placement but `'off'`
. Several plausible spellings are tried, because the
        failure this guards against is someone adding one of them later.
        '''
        for name in ('TYPED_PROPERTIES', 'typed_properties', 'GRAPHRAG_TYPED_PROPERTIES'):
            monkeypatch.setenv(name, 'both')

        GraphRAGConfig._typed_properties = None

        assert GraphRAGConfig.typed_properties == TYPED_PROPERTIES_OFF

    def test_ontology_config_default(self, company_ontology):
        '''
        An ontology that says nothing about typed properties writes none.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        config = OntologyConfig(company_ontology, ontology_authority='strict')

        assert config.typed_properties == TYPED_PROPERTIES_OFF
        assert config.writes_subject_properties() is False
        assert config.writes_complement_properties() is False

    def test_index_with_no_ontology_passes_none(self):
        '''
        `LexicalGraphIndex._typed_properties()` returns None with no ontology,
        leaving `BuildPipeline` to coalesce to the config floor. None rather than
        `'off'` so that a caller's explicit argument still wins.
        '''
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex

        index = LexicalGraphIndex.__new__(LexicalGraphIndex)
        index.indexing_config = MagicMock()
        index.indexing_config.extraction.ontology = None

        assert index._typed_properties() is None

    def test_index_with_ontology_passes_its_placement(self, company_ontology):
        '''
        With an ontology, the ontology's own setting is what travels.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex

        index = LexicalGraphIndex.__new__(LexicalGraphIndex)
        index.indexing_config = MagicMock()
        index.indexing_config.extraction.ontology = OntologyConfig(
            company_ontology, ontology_authority='align', typed_properties='subject'
        )

        assert index._typed_properties() == 'subject'

class TestOntologyConfigValidation:
    '''
    Tests for `OntologyConfig`'s own refusals.
    '''

    @pytest.mark.parametrize('placement', TYPED_PROPERTY_PLACEMENTS)
    def test_every_documented_placement_is_accepted(self, company_ontology, placement):
        '''
        The four placements the type says exist all construct.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        assert OntologyConfig(company_ontology, typed_properties=placement).typed_properties == placement

    def test_unknown_placement_raises_listing_the_supported_ones(self, company_ontology):
        '''
        A typo should say what was expected, not just that something was wrong.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        with pytest.raises(ValueError) as excinfo:
            OntologyConfig(company_ontology, typed_properties='sub-ject')

        message = str(excinfo.value)
        assert "'sub-ject'" in message
        for placement in TYPED_PROPERTY_PLACEMENTS:
            assert placement in message

    @pytest.mark.parametrize('placement', SUBJECT_PLACEMENTS)
    def test_reserved_property_name_raises_under_subject_placement(self, reserved_name_ontology, placement):
        '''
        A declared `:value` would be keyed onto `__Entity__` with
        the same name as the entity's own identity string.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        with pytest.raises(ValueError) as excinfo:
            OntologyConfig(reserved_name_ontology, typed_properties=placement)

        message = str(excinfo.value)
        assert "'value'" in message
        assert placement in message
        # The message must be actionable: name the offending term, and say what
        # to do about it.
        assert 'Rename' in message

    def test_reserved_name_message_widens_the_set_under_both(self, reserved_name_ontology):
        '''
        `'both'` writes `typed_value` and `datatype` too, so those names are
        reserved as well and the message should say so; `'subject'` alone does
        not write them and should not claim otherwise.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        with pytest.raises(ValueError) as both_error:
            OntologyConfig(reserved_name_ontology, typed_properties='both')
        with pytest.raises(ValueError) as subject_error:
            OntologyConfig(reserved_name_ontology, typed_properties='subject')

        # Read the parenthesized reserved set rather than searching the whole
        # message: the prose says "declares datatype property", so a bare
        # substring test for `datatype` matches text that is not the set.
        def reserved_set(error):
            message = str(error.value)
            listed = message[message.index('__Entity__ (') + len('__Entity__ ('):]
            return {name.strip() for name in listed[:listed.index(')')].split(',')}

        assert reserved_set(subject_error) == set(RESERVED_ENTITY_PROPERTIES)
        assert reserved_set(both_error) == set(RESERVED_ENTITY_PROPERTIES) | set(COMPLEMENT_ENTITY_PROPERTIES)

    @pytest.mark.parametrize('placement', [TYPED_PROPERTIES_OFF, 'complement'])
    def test_reserved_property_name_accepted_when_nothing_keys_from_it(self, reserved_name_ontology, placement):
        '''
        The check is scoped to the placement that actually writes. `'off'` writes
        nothing, and `'complement'` writes fixed names rather than keying from the
        ontology's vocabulary, so neither can collide - and rejecting them would
        refuse a configuration that works.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        assert OntologyConfig(reserved_name_ontology, typed_properties=placement) is not None

class TestFilterRequired:
    '''
    Tests that asking for typed properties keeps the filter in the pipeline.
    '''

    @pytest.mark.parametrize('placement', sorted(set(TYPED_PROPERTY_PLACEMENTS) - {TYPED_PROPERTIES_OFF}))
    def test_typed_properties_alone_requires_the_filter(self, company_ontology, placement):
        '''
        The annotations the builders read are written by `OntologyFilter` and by
        nothing else, so `ontology_authority='off'` plus a placement must still build the
        filter. Without this the request would be accepted and silently write
        nothing.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        config = OntologyConfig(company_ontology, ontology_authority='off', typed_properties=placement)

        assert config.resolved().any_enabled() is False
        assert config.filter_required() is True

    def test_off_plus_off_does_not_require_the_filter(self, company_ontology):
        '''
        The pre-existing behaviour is unchanged: nothing to do means no transform.
        '''
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology import OntologyConfig

        config = OntologyConfig(company_ontology, ontology_authority='off', typed_properties='off')

        assert config.filter_required() is False

class TestBuildPipelineValidation:
    '''
    Tests for `BuildPipeline`'s validation, including the local-entities rule.
    '''

    def _create(self, **kwargs):
        # `BuildPipeline.create` wraps the pipeline in a `Pipe` and returns that,
        # so the object under test is unreachable through it. Constructed directly
        # here; `create` is a pass-through and is covered by the `run_pipeline`
        # test below.
        return BuildPipeline(components=[], **kwargs)

    @pytest.mark.parametrize('placement', [TYPED_PROPERTIES_OFF, 'subject'])
    def test_placements_needing_no_complement_are_accepted(self, placement):
        '''
        Neither of these writes to the complement node, so neither depends on one
        existing.
        '''
        pipeline = self._create(typed_properties=placement, include_local_entities=False)

        assert pipeline.typed_properties == placement

    @pytest.mark.parametrize('placement', COMPLEMENT_PLACEMENTS)
    def test_complement_placement_without_local_entities_raises(self, placement):
        '''
        Complement placement writes to a node that
        `include_local_entities=False` never creates, so the write has no target.
        Refused at construction: the alternative is a build that completes and
        stores nothing, which is the failure mode hardest to diagnose.
        '''
        with pytest.raises(ValueError) as excinfo:
            self._create(typed_properties=placement, include_local_entities=False)

        message = str(excinfo.value)
        assert placement in message
        assert 'include_local_entities' in message
        # Actionable: both ways out are named.
        assert 'include_local_entities=True' in message
        assert "typed_properties='subject'" in message

    @pytest.mark.parametrize('placement', COMPLEMENT_PLACEMENTS)
    def test_complement_placement_with_local_entities_is_accepted(self, placement):
        '''
        With a complement node to write to, the same placement is fine.
        '''
        pipeline = self._create(typed_properties=placement, include_local_entities=True)

        assert pipeline.typed_properties == placement

    def test_unknown_placement_raises(self):
        '''
        Validated here as well as in `OntologyConfig`, because a caller can
        construct a pipeline directly and never touch an `OntologyConfig`.
        '''
        with pytest.raises(ValueError) as excinfo:
            self._create(typed_properties='on')

        assert "'on'" in str(excinfo.value)

    def test_default_resolves_to_off(self):
        '''
        No argument, no ontology, no writes.
        '''
        assert self._create().typed_properties == TYPED_PROPERTIES_OFF

    def test_explicit_argument_beats_the_config_floor(self, restore_graphrag_typed_properties):
        '''
        `coalesce` order: the caller's argument wins over `GraphRAGConfig`.
        '''
        GraphRAGConfig.typed_properties = 'subject'

        assert self._create().typed_properties == 'subject'
        assert self._create(typed_properties=TYPED_PROPERTIES_OFF).typed_properties == TYPED_PROPERTIES_OFF

class TestKwargReachesTheBuilders:
    '''
    Tests that the setting travels from the pipeline to `builder.build()`.
    '''

    def test_build_pipeline_passes_it_to_run_pipeline(self):
        '''
        `run_pipeline`'s `**kwargs` are what eventually reach `accept`, so this is
        the hand-off that matters at the pipeline end.
        '''
        pipeline = BuildPipeline(
            components=[],
            typed_properties='subject',
            include_local_entities=True,
        )

        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import NodeRelationship, RelatedNodeInfo

        chunk = TextNode(text='a', id_='a1')
        chunk.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id='src-1')
        doc = SourceDocument(nodes=[chunk])

        with patch(
            'graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.run_pipeline',
            return_value=[]
        ) as run:
            list(pipeline.build([doc]))

        assert run.call_args.kwargs['typed_properties'] == 'subject'

    def test_graph_construction_passes_it_to_build(self, mock_neptune_store):
        '''
        And this is the hand-off at the builder end: `accept`'s surviving kwargs
        go to every builder for the node's index.
        '''
        from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
        from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

        builder = MagicMock(spec=GraphBuilder)
        builder.index_key.return_value = 'fact'

        construction = GraphConstruction(graph_client=mock_neptune_store, builders=[builder])

        node = fact_node(ANNOTATED_FACT)
        node.metadata[INDEX_KEY] = {'index': 'fact'}

        list(construction.accept(
            [node],
            batch_writes_enabled=False,
            batch_write_size=10,
            include_domain_labels=False,
            include_local_entities=True,
            typed_properties='subject',
        ))

        builder.build.assert_called_once()
        assert builder.build.call_args.kwargs['typed_properties'] == 'subject'

    @pytest.mark.parametrize(
        'builder',
        default_builders(),
        ids=lambda builder: type(builder).__name__
    )
    def test_every_default_builder_tolerates_the_kwarg(self, builder):
        '''
        keeping graph writes out of ambient configuration5's other half. Every builder receives the same kwargs, so
        adding one must not break the eight that ignore it. Asserted against all
        of them rather than just `EntityGraphBuilder`, because the others read
        their own kwargs with hard subscripts and a future kwarg added the same
        way would fail here first.
        '''
        node = fact_node(ANNOTATED_FACT, node_id=ANNOTATED_FACT['factId'])

        graph_client = MagicMock()
        graph_client.node_id = lambda field: f'params.{field}'
        graph_client.property_assigment_fn = lambda key, value: (lambda x: x)
        graph_client.execute_query_with_retry = MagicMock(return_value=[])

        # Not every builder handles a fact node - most look for their own
        # metadata key and do nothing. Doing nothing is a pass; raising is not.
        builder.build(
            node,
            graph_client,
            include_domain_labels=False,
            include_local_entities=True,
            typed_properties='subject',
        )

    @pytest.mark.parametrize(
        'builder',
        default_builders(),
        ids=lambda builder: type(builder).__name__
    )
    def test_every_default_builder_tolerates_its_absence(self, builder):
        '''
        A caller who built a pipeline before this setting
        existed, or who calls a builder directly as several tests do, supplies no
        `typed_properties` at all - and must not get a `KeyError` from a feature
        they never asked for.
        '''
        node = fact_node(ANNOTATED_FACT, node_id=ANNOTATED_FACT['factId'])

        graph_client = MagicMock()
        graph_client.node_id = lambda field: f'params.{field}'
        graph_client.property_assigment_fn = lambda key, value: (lambda x: x)
        graph_client.execute_query_with_retry = MagicMock(return_value=[])

        builder.build(
            node,
            graph_client,
            include_domain_labels=False,
            include_local_entities=True,
        )

class TestNoAnnotationsWarning:
    '''
    Typed properties asked for, and none can be written.
    '''

    def _build_unannotated_facts(self, count, typed_properties):
        graph_client = MagicMock()
        graph_client.node_id = lambda field: f'params.{field}'
        graph_client.execute_query_with_retry = MagicMock(return_value=[])

        builder = EntityGraphBuilder()
        for i in range(count):
            fact = dict(UNANNOTATED_FACT, factId=f'f-{i}')
            builder.build(
                fact_node(fact, node_id=fact['factId']),
                graph_client,
                include_domain_labels=False,
                include_local_entities=False,
                typed_properties=typed_properties,
            )

    def test_warns_once_after_enough_unannotated_facts(self, caplog, reset_annotation_warning):
        '''
        The warning names the cause and the remedy, because the symptom - an empty
        property - points at the graph store rather than at the pipeline order.
        '''
        threshold = entity_graph_builder_module._NO_ANNOTATIONS_WARNING_AFTER

        with caplog.at_level(logging.WARNING):
            self._build_unannotated_facts(threshold * 2, 'subject')

        warnings = [r for r in caplog.records if 'typed_properties' in r.getMessage()]

        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert 'Re-extract' in message
        assert 'ontology annotations' in message

    def test_silent_below_the_threshold(self, caplog, reset_annotation_warning):
        '''
        One unresolved predicate carries no annotations and is entirely normal at
        `align`, so a single unannotated fact must not cry wolf.
        '''
        threshold = entity_graph_builder_module._NO_ANNOTATIONS_WARNING_AFTER

        with caplog.at_level(logging.WARNING):
            self._build_unannotated_facts(threshold - 1, 'subject')

        assert not [r for r in caplog.records if 'typed_properties' in r.getMessage()]

    def test_silent_at_off(self, caplog, reset_annotation_warning):
        '''
        Nobody asked for typed properties, so their absence is not news.
        '''
        threshold = entity_graph_builder_module._NO_ANNOTATIONS_WARNING_AFTER

        with caplog.at_level(logging.WARNING):
            self._build_unannotated_facts(threshold * 2, TYPED_PROPERTIES_OFF)

        assert not [r for r in caplog.records if 'typed_properties' in r.getMessage()]

    def test_one_annotated_fact_silences_it_permanently(self, caplog, reset_annotation_warning):
        '''
        Evidence that the filter ran is evidence enough. Facts the filter could
        not resolve are expected at every level below `strict`, and counting them
        after that point would warn about normal operation.
        '''
        graph_client = MagicMock()
        graph_client.node_id = lambda field: f'params.{field}'
        graph_client.execute_query_with_retry = MagicMock(return_value=[])

        builder = EntityGraphBuilder()

        with caplog.at_level(logging.WARNING):
            builder.build(
                fact_node(ANNOTATED_FACT),
                graph_client,
                include_domain_labels=False,
                include_local_entities=True,
                typed_properties='subject',
            )
            self._build_unannotated_facts(
                entity_graph_builder_module._NO_ANNOTATIONS_WARNING_AFTER * 2, 'subject'
            )

        assert not [r for r in caplog.records if 'typed_properties' in r.getMessage()]

    def test_annotation_detection_reads_the_whole_fact(self):
        '''
        The question is "did the filter run", not "can this fact be written", so
        any one annotation counts - a fact whose subject resolved but whose
        predicate did not is still proof the filter ran.
        '''
        from graphrag_toolkit.lexical_graph.indexing.model import Fact

        has = entity_graph_builder_module._has_ontology_annotations

        assert has(Fact.model_validate(ANNOTATED_FACT)) is True
        assert has(Fact.model_validate(UNANNOTATED_FACT)) is False

        subject_only = dict(
            UNANNOTATED_FACT,
            subject=dict(UNANNOTATED_FACT['subject'], classIri='http://example.org/company#Company'),
        )
        assert has(Fact.model_validate(subject_only)) is True

        predicate_only = dict(
            UNANNOTATED_FACT,
            predicate={'value': 'acquired', 'canonicalName': 'acquired'},
        )
        assert has(Fact.model_validate(predicate_only)) is True
