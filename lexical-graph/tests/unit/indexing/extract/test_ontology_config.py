# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""`ontology_authority` as a level, its per-dimension overrides, and the wiring.

The level is the user-facing knob and the six dimensions are what the code acts
on, so the mapping between them is a contract: this file pins the whole table
rather than sampling it, because a dimension that silently fails to turn on is
the one failure in this feature that looks like success.
"""

from dataclasses import fields
from pathlib import Path

import pytest

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import Ontology
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_config import (
    DIMENSIONS,
    ONTOLOGY_AUTHORITY_LEVELS,
    OntologyConfig,
    ResolvedDimensions,
    to_ontology_config,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_filter import OntologyFilter
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.prompt_constraint import (
    PROMPT_CONSTRAINT_LEVELS,
)
from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex

FIXTURES = Path(__file__).parent.parent.parent.parent / 'fixtures' / 'ontologies'
COMPANY = FIXTURES / 'company.ttl'

# The level table, read out as data. `off` is the all-False default; `align` owns
# naming; `strict` alone decides membership.
LEVEL_TABLE = {
    'off': (),
    'align': ('normalize_names', 'drop_type_restatements'),
    'strict': DIMENSIONS,
}

@pytest.fixture(scope='module')
def company():
    return Ontology.load(COMPANY)

def turtle_declaring(property_name):
    return Ontology.from_turtle_string(
        '@prefix : <urn:x#> . '
        '@prefix owl: <http://www.w3.org/2002/07/owl#> . '
        '@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . '
        '@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . '
        f':{property_name} a owl:DatatypeProperty ; rdfs:range xsd:string .'
    )

class TestTheLevelResolvesToTheDimensions:

    def test_the_six_dimensions_are_derived_from_the_dataclass(self):
        """So the defaults table and the override loop cannot fall out of step."""
        assert DIMENSIONS == tuple(field.name for field in fields(ResolvedDimensions))
        assert len(DIMENSIONS) == 6

    @pytest.mark.parametrize('level', LEVEL_TABLE)
    @pytest.mark.parametrize('dimension', DIMENSIONS)
    def test_the_whole_table(self, company, level, dimension):
        resolved = OntologyConfig(company, ontology_authority=level).resolved()
        assert getattr(resolved, dimension) is (dimension in LEVEL_TABLE[level])

    def test_the_default_level_is_align(self, company):
        assert OntologyConfig(company).ontology_authority == 'align'

    def test_the_levels_are_the_renderers_levels_and_not_a_second_list(self):
        """A level this module accepted but the renderer did not would raise from
        inside pipeline setup, after configuration had already succeeded."""
        assert ONTOLOGY_AUTHORITY_LEVELS is PROMPT_CONSTRAINT_LEVELS

    def test_resolved_is_recomputed_rather_than_cached(self, company):
        config = OntologyConfig(company, ontology_authority='strict')
        config.ontology_authority = 'off'
        assert config.resolved() == ResolvedDimensions()

class TestOverrides:
    """The level chooses defaults, it does not constrain them."""

    @pytest.mark.parametrize('dimension', DIMENSIONS)
    def test_any_dimension_can_be_turned_on_at_off(self, company, dimension):
        config = OntologyConfig(company, ontology_authority='off', **{dimension: True})
        assert getattr(config.resolved(), dimension) is True

    @pytest.mark.parametrize('dimension', DIMENSIONS)
    def test_any_dimension_can_be_turned_off_at_strict(self, company, dimension):
        config = OntologyConfig(company, ontology_authority='strict', **{dimension: False})
        assert getattr(config.resolved(), dimension) is False

    @pytest.mark.parametrize('dimension', DIMENSIONS)
    def test_none_means_follow_the_level(self, company, dimension):
        config = OntologyConfig(company, ontology_authority='align', **{dimension: None})
        assert getattr(config.resolved(), dimension) is (dimension in LEVEL_TABLE['align'])

    def test_say_nothing_in_the_prompt_but_still_coerce_datatypes(self, company):
        """The combination the override mechanism exists for, and not a contradiction."""
        resolved = OntologyConfig(company, ontology_authority='off', enforce_datatypes=True).resolved()
        assert (resolved.enforce_datatypes, resolved.normalize_names) == (True, False)

class TestValidation:
    """Settings are checked before the ontology is parsed, except where they cannot be."""

    @pytest.mark.parametrize('level', ['stricter', 'STRICT', 'guide', '', None])
    def test_an_unknown_authority_level_raises_and_lists_the_levels(self, company, level):
        with pytest.raises(ValueError, match='Unknown ontology authority level'):
            OntologyConfig(company, ontology_authority=level)

    def test_an_unknown_typed_properties_placement_raises(self, company):
        with pytest.raises(ValueError, match='Unknown typed_properties placement'):
            OntologyConfig(company, typed_properties='subjects')

    def test_an_unknown_vocabulary_format_raises(self, company):
        with pytest.raises(ValueError, match='Unknown ontology vocabulary_format'):
            OntologyConfig(company, vocabulary_format='ttl')

    def test_a_setting_is_validated_before_the_ontology_is_loaded(self):
        """A typo should not first cost a parse of a file that does not exist."""
        with pytest.raises(ValueError, match='Unknown ontology authority level'):
            OntologyConfig(FIXTURES / 'absent.ttl', ontology_authority='nonsense')

    @pytest.mark.parametrize('name', ['value', 'search_str', 'class'])
    def test_a_property_that_would_overwrite_the_entity_is_refused(self, name):
        """Under subject placement this key is the entity's own."""
        with pytest.raises(ValueError, match='the graph model already owns'):
            OntologyConfig(turtle_declaring(name), typed_properties='subject')

    @pytest.mark.parametrize('name', ['typed_value', 'datatype'])
    def test_the_complement_property_names_are_reserved_only_under_both(self, name):
        """The check is scoped to the placement actually requested.

        Only subject placement keys a property from the ontology's vocabulary, so
        `'complement'` alone has nothing to collide with - it widens the reserved
        set only when subject placement is active too.
        """
        OntologyConfig(turtle_declaring(name), typed_properties='complement')

        with pytest.raises(ValueError, match='the graph model already owns'):
            OntologyConfig(turtle_declaring(name), typed_properties='both')

    def test_a_collision_is_not_raised_for_a_write_that_will_never_happen(self):
        OntologyConfig(turtle_declaring('value'), typed_properties='off')
        OntologyConfig(turtle_declaring('value'), typed_properties='complement')

class TestNormalizingWhatTheUserConfigured:

    def test_the_ontology_is_loaded_whatever_form_it_arrived_in(self):
        assert isinstance(OntologyConfig(COMPANY).ontology, Ontology)
        assert isinstance(OntologyConfig(str(COMPANY)).ontology, Ontology)

    def test_an_existing_config_is_never_rebuilt_at_defaults(self, company):
        """Settings a user made must survive normalization."""
        config = OntologyConfig(company, ontology_authority='strict', report_violations=True)
        assert to_ontology_config(config) is config

    def test_a_bare_source_gets_the_default_level(self):
        assert to_ontology_config(COMPANY).ontology_authority == 'align'

class TestWhatTheBuildersAreTold:

    @pytest.mark.parametrize('placement,subject,complement', [
        ('off', False, False),
        ('subject', True, False),
        ('complement', False, True),
        ('both', True, True),
    ])
    def test_each_placement_writes_to_its_own_node(self, company, placement, subject, complement):
        """`'both'` runs each write to its own node; it is not a fallback chain."""
        config = OntologyConfig(company, typed_properties=placement)
        assert config.writes_subject_properties() is subject
        assert config.writes_complement_properties() is complement

    def test_typed_properties_defaults_to_off(self, company):
        assert OntologyConfig(company).typed_properties == 'off'

class TestWhetherTheFilterIsBuiltAtAll:
    """the rule that a filter with nothing to do is left out, and the reason `off` is not a filter with every flag off."""

    @pytest.mark.parametrize('kwargs,required', [
        ({'ontology_authority': 'off'}, False),
        ({'ontology_authority': 'align'}, True),
        ({'ontology_authority': 'strict'}, True),
        ({'ontology_authority': 'off', 'enforce_datatypes': True}, True),
        ({'ontology_authority': 'off', 'typed_properties': 'subject'}, True),
        ({'ontology_authority': 'off', 'typed_properties': 'complement'}, True),
    ])
    def test_filter_required(self, company, kwargs, required):
        assert OntologyConfig(company, **kwargs).filter_required() is required

    def test_no_ontology_means_no_filter(self):
        assert LexicalGraphIndex._ontology_filter(None) is None

    def test_off_puts_no_component_in_the_pipeline(self, company):
        """Not a no-op component: a filter would still round-trip TOPICS_KEY
        through validate/dump and would still annotate, which `off` rules out."""
        assert LexicalGraphIndex._ontology_filter(
            OntologyConfig(company, ontology_authority='off')
        ) is None

    @pytest.mark.parametrize('level', ['align', 'strict'])
    def test_every_resolved_dimension_reaches_the_filter(self, company, level):
        """The `asdict` spread, asserted dimension by dimension.

        A hand-written argument list can omit one, and the symptom is a gate the
        user asked for silently not running.
        """
        config = OntologyConfig(company, ontology_authority=level)
        filter_ = LexicalGraphIndex._ontology_filter(config)

        for dimension in DIMENSIONS:
            assert getattr(filter_, dimension) is getattr(config.resolved(), dimension)

    def test_the_filter_accepts_every_dimension_by_name(self):
        """Guards the spread from the other end: a new dimension with no matching
        flag on the filter would raise at pipeline construction."""
        for dimension in DIMENSIONS:
            assert dimension in OntologyFilter.model_fields

    def test_typed_properties_alone_builds_an_annotate_only_filter(self, company):
        """`canonicalName` and `datatype` are written by the filter and nothing
        else, so dropping it here would silently write no typed properties."""
        filter_ = LexicalGraphIndex._ontology_filter(
            OntologyConfig(company, ontology_authority='off', typed_properties='subject')
        )

        assert filter_ is not None
        assert not any(getattr(filter_, dimension) for dimension in DIMENSIONS)

    def test_report_violations_reaches_the_filter(self, company):
        filter_ = LexicalGraphIndex._ontology_filter(
            OntologyConfig(company, report_violations=True)
        )
        assert filter_.report_violations is True

    def test_the_filter_carries_the_index_and_not_the_graph(self, company):
        """What crosses the spawn boundary is plain data."""
        filter_ = LexicalGraphIndex._ontology_filter(OntologyConfig(company))
        assert filter_.index is company.index()

class TestWhatEachPromptStageIsGiven:
    """The two blocks are different blocks, and swapping them would be silent."""

    def test_no_ontology_renders_two_empty_blocks(self):
        assert LexicalGraphIndex._render_ontology_constraints(None) == ('', '')

    def test_the_topics_block_gets_the_vocabulary_and_the_propositions_block_the_classes(self, company):
        constraints = LexicalGraphIndex._render_ontology_constraints(OntologyConfig(company))

        assert 'WORKS_FOR' in constraints.topics
        assert 'WORKS_FOR' not in constraints.propositions
        assert 'Sports Team' in constraints.propositions

    def test_off_renders_neither_block(self, company):
        constraints = LexicalGraphIndex._render_ontology_constraints(
            OntologyConfig(company, ontology_authority='off')
        )
        assert constraints == ('', '')

    def test_the_vocabulary_format_reaches_the_topics_block_only(self, company):
        """That stage classifies the entities it names and extracts nothing else,
        so there is no property vocabulary there for a serialization to present
        differently."""
        prose = LexicalGraphIndex._render_ontology_constraints(OntologyConfig(company))
        turtle = LexicalGraphIndex._render_ontology_constraints(
            OntologyConfig(company, vocabulary_format='turtle')
        )

        assert '```turtle' in turtle.topics
        assert '```turtle' not in prose.topics
        assert turtle.propositions == prose.propositions

class TestWhatTheBuildPipelineIsTold:

    def index_for(self, ontology):
        from graphrag_toolkit.lexical_graph.lexical_graph_index import (
            ExtractionConfig,
            IndexingConfig,
            LexicalGraphIndex,
        )

        index = LexicalGraphIndex.__new__(LexicalGraphIndex)
        index.indexing_config = IndexingConfig(extraction=ExtractionConfig(ontology=ontology))
        return index

    def test_no_ontology_defers_rather_than_pinning_off(self):
        """Returned as None so the `coalesce` chain behaves as it does for every
        other setting: an unasked-for value defers to the layer below, and no
        environment variable can reach a placement that writes."""
        assert self.index_for(None)._typed_properties() is None

    @pytest.mark.parametrize('placement', ['off', 'subject', 'complement', 'both'])
    def test_a_configured_placement_is_passed_through(self, company, placement):
        ontology_config = OntologyConfig(company, typed_properties=placement)
        assert self.index_for(ontology_config)._typed_properties() == placement

class TestSeedingThePreferredClassifications:
    """Seeding, and the user's own list."""

    def test_the_slot_is_seeded_from_the_ontology_when_the_user_said_nothing(self, company):
        from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig

        config = ExtractionConfig(ontology=OntologyConfig(company))

        assert LexicalGraphIndex._preferred_entity_classifications(config) == company.class_names()

    def test_a_user_list_is_kept_with_a_warning(self, company, caplog):
        """Honouring the ontology instead would discard a setting the user made
        deliberately; merging the two would produce a vocabulary neither asked for."""
        import logging

        from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig

        config = ExtractionConfig(
            ontology=OntologyConfig(company),
            preferred_entity_classifications=['Widget'],
        )

        with caplog.at_level(logging.WARNING):
            assert LexicalGraphIndex._preferred_entity_classifications(config) == ['Widget']

        assert 'Honouring preferred_entity_classifications' in caplog.text

    def test_without_an_ontology_the_users_value_is_returned_unchanged(self):
        from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig

        config = ExtractionConfig(preferred_entity_classifications=['Widget'])

        assert LexicalGraphIndex._preferred_entity_classifications(config) == ['Widget']

# The pipeline is built with the store factories patched, so the store's *type* is
# the only thing the pipeline learns from it - which is what
# `_configure_extraction_pipeline` branches on.
MODULE = 'graphrag_toolkit.lexical_graph.lexical_graph_index'

def build_pipeline(extraction, batch=False, graph_store=None, pre_processors=False):
    from unittest.mock import Mock, patch

    from llama_index.core.llms.mock import MockLLM

    from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig
    from graphrag_toolkit.lexical_graph.lexical_graph_index import IndexingConfig

    # The extractors fall back to `GraphRAGConfig.extraction_llm` when the config
    # names none, and that constructs a real `BedrockConverse` - which needs a
    # region, and so fails wherever AWS is not configured. Nothing here depends on
    # which model it is.
    if extraction.extraction_llm is None:
        extraction.extraction_llm = MockLLM()

    config = IndexingConfig(
        extraction=extraction,
        batch_config=BatchConfig(
            role_arn='arn:aws:iam::123456789012:role/test-batch-role',
            region='us-east-1',
            bucket_name='test-batch-bucket',
        ) if batch else None,
    )
    store = Mock() if graph_store is None else graph_store

    with (
        patch(f'{MODULE}.GraphStoreFactory.for_graph_store', return_value=store),
        patch(f'{MODULE}.MultiTenantGraphStore.wrap', return_value=store),
        patch(f'{MODULE}.VectorStoreFactory.for_vector_store', return_value=store),
        patch(f'{MODULE}.MultiTenantVectorStore.wrap', return_value=store),
    ):
        index = LexicalGraphIndex(
            graph_store='dummy://', vector_store='dummy://', indexing_config=config,
        )

    return index.extraction_pre_processors if pre_processors else index.extraction_components

def only(components, component_type):
    matches = [c for c in components if isinstance(c, component_type)]
    assert len(matches) == 1, f'expected one {component_type.__name__}, found {len(matches)}'
    return matches[0]

class TestTheBlocksReachTheExtractors:
    """Rendering the right block is not enough; each stage has to be handed its own.

    Swapping them would leave both prompts rendering and both stages extracting,
    which is why this is asserted per extractor rather than inferred from
    `_render_ontology_constraints`.
    """

    @pytest.mark.parametrize('batch', [False, True], ids=['non_batch', 'batch'])
    def test_each_extractor_receives_its_own_block(self, company, batch):
        from graphrag_toolkit.lexical_graph import ExtractionConfig
        from graphrag_toolkit.lexical_graph.indexing.extract import (
            BatchLLMPropositionExtractorSync,
            BatchTopicExtractorSync,
            LLMPropositionExtractor,
            TopicExtractor,
        )

        components = build_pipeline(ExtractionConfig(ontology=COMPANY), batch=batch)
        propositions = BatchLLMPropositionExtractorSync if batch else LLMPropositionExtractor
        topics = BatchTopicExtractorSync if batch else TopicExtractor

        assert only(components, propositions).ontology_constraints == \
            company.format_as_proposition_constraint('align')
        assert only(components, topics).ontology_constraints == \
            company.format_as_prompt_constraint('align')

    @pytest.mark.parametrize('extraction_kwargs', [{}, {'ontology_authority': 'off'}])
    def test_no_ontology_and_off_both_leave_every_extractor_empty(self, company, extraction_kwargs):
        """The empty string, not None: an empty block composes to no change."""
        from llama_index.core.node_parser import SentenceSplitter

        from graphrag_toolkit.lexical_graph import ExtractionConfig

        ontology = OntologyConfig(company, **extraction_kwargs) if extraction_kwargs else None
        components = build_pipeline(ExtractionConfig(ontology=ontology))

        for component in components:
            if not isinstance(component, SentenceSplitter):
                assert component.ontology_constraints == ''

    def test_the_filter_is_the_last_component_when_it_is_present(self, company):
        """It reads what the topic extractor wrote, so ordering is not cosmetic."""
        from graphrag_toolkit.lexical_graph import ExtractionConfig

        components = build_pipeline(ExtractionConfig(ontology=COMPANY))

        assert isinstance(components[-1], OntologyFilter)

    def test_no_filter_component_exists_at_off(self, company):
        from graphrag_toolkit.lexical_graph import ExtractionConfig

        components = build_pipeline(ExtractionConfig(
            ontology=OntologyConfig(company, ontology_authority='off'),
        ))

        assert not [c for c in components if isinstance(c, OntologyFilter)]

class TestOntologyAndClassificationInference:
    """One combination has no coherent reading and is rejected at config time."""

    def test_inference_alongside_an_ontology_is_fine(self):
        from graphrag_toolkit.lexical_graph import ExtractionConfig

        assert ExtractionConfig(
            ontology=COMPANY, infer_entity_classifications=True,
        ).infer_entity_classifications is True

    def test_replacing_the_seeded_classifications_is_rejected_and_names_the_way_out(self):
        """Inference that replaces the defaults would discard the very class names
        the ontology seeded, leaving the vocabulary block and the preference slot
        disagreeing."""
        from graphrag_toolkit.lexical_graph import ExtractionConfig
        from graphrag_toolkit.lexical_graph.indexing.extract import InferClassificationsConfig

        with pytest.raises(ValueError) as error:
            ExtractionConfig(
                ontology=COMPANY,
                infer_entity_classifications=InferClassificationsConfig(
                    replace_default_classifications=True,
                ),
            )

        assert 'replace_default_classifications=False' in str(error.value)

    def test_replacing_without_an_ontology_is_still_fine(self):
        """The rejection is about the combination, not about the setting."""
        from graphrag_toolkit.lexical_graph import ExtractionConfig
        from graphrag_toolkit.lexical_graph.indexing.extract import InferClassificationsConfig

        config = ExtractionConfig(infer_entity_classifications=InferClassificationsConfig(
            replace_default_classifications=True,
        ))

        assert config.infer_entity_classifications.replace_default_classifications is True

class TestSeedingReachesTheProvider:
    """preference seeding delivered, not just decided.

    `_preferred_entity_classifications` picks the list; these assert the pipeline
    then hands that list to whichever provider the configuration asks for.
    """

    def classifications_of(self, components):
        from llama_index.core.schema import TextNode

        from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor

        extractor = only(components, TopicExtractor)
        return extractor.entity_classification_provider(TextNode(text='x', id_='chunk-1'))

    def test_the_ontologys_class_names_are_what_the_extractor_offers(self, company):
        from graphrag_toolkit.lexical_graph import ExtractionConfig

        components = build_pipeline(ExtractionConfig(ontology=COMPANY))

        assert self.classifications_of(components) == company.class_names()

    def test_a_users_own_provider_is_passed_through_untouched(self):
        """A callable is not a list, so it cannot be merged with anything - and a
        user who wrote one has said how the slot is filled."""
        from graphrag_toolkit.lexical_graph import ExtractionConfig
        from graphrag_toolkit.lexical_graph.indexing.extract import PreferredValuesProvider

        class FixedProvider(PreferredValuesProvider):
            def __call__(self, node):
                return ['From Provider']

        components = build_pipeline(ExtractionConfig(
            ontology=COMPANY, preferred_entity_classifications=FixedProvider(),
        ))

        assert self.classifications_of(components) == ['From Provider']

    def test_inference_starts_from_the_ontologys_class_names(self, company):
        """The seeded list becomes the inferencer's defaults rather than being
        discarded, which is why replacing them is refused at config time."""
        from graphrag_toolkit.lexical_graph import ExtractionConfig
        from graphrag_toolkit.lexical_graph.indexing.extract import InferClassifications

        pre_processors = build_pipeline(ExtractionConfig(
            ontology=COMPANY, infer_entity_classifications=True,
        ), pre_processors=True)
        inferencer = only(pre_processors, InferClassifications)

        assert inferencer.default_classifications == company.class_names()

    def test_a_dummy_store_still_gets_the_prompt_block(self):
        """Providers are forced empty on that path; the vocabulary block is not."""
        from graphrag_toolkit.lexical_graph import ExtractionConfig
        from graphrag_toolkit.lexical_graph.indexing.extract import TopicExtractor
        from graphrag_toolkit.lexical_graph.storage.graph import DummyGraphStore

        components = build_pipeline(
            ExtractionConfig(ontology=COMPANY), graph_store=DummyGraphStore(),
        )

        assert only(components, TopicExtractor).ontology_constraints
        assert self.classifications_of(components) == []
