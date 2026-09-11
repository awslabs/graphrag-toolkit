# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Rendering the ontology into the two prompt blocks.

The block is prose fed to a model, so almost nothing about its wording is a
correctness claim and none of it is asserted here. What is asserted is the
handful of structural properties other code depends on: that `off` renders
nothing, that the two output channels stay separated, that a name rendered here
is a name the response parser can hand back, and that the level changes only the
closing guidance and never the vocabulary.
"""

from pathlib import Path

import pytest

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.naming import resolution_key
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import Ontology
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.prompt_constraint import (
    rendered_class_names,
)

FIXTURES = Path(__file__).parent.parent.parent.parent / 'fixtures' / 'ontologies'

CLASSES_HEADING = '## Entity types'
RELATIONSHIPS_HEADING = '## Relationships'
ATTRIBUTES_HEADING = '## Attributes'
PROTOCOL_HEADING = '## Using this vocabulary'

@pytest.fixture(scope='module')
def company():
    return Ontology.load(FIXTURES / 'company.ttl')

@pytest.fixture(scope='module')
def empty():
    return Ontology.from_turtle_string(
        '@prefix owl: <http://www.w3.org/2002/07/owl#> . '
        '<urn:x> a owl:Ontology .'
    )

def section(block, heading, headings=(CLASSES_HEADING, RELATIONSHIPS_HEADING,
                                      ATTRIBUTES_HEADING, PROTOCOL_HEADING)):
    """The text of one `##` section of a rendered block."""
    start = block.index(heading)
    ends = [block.index(other) for other in headings if other in block and block.index(other) > start]
    return block[start:min(ends)] if ends else block[start:]

class TestOffRendersNothing:
    """the `off`-changes-nothing guarantee: `off` says nothing, in either block or either format."""

    @pytest.mark.parametrize('vocabulary_format', ['prose', 'turtle'])
    def test_the_topics_block_is_empty(self, company, vocabulary_format):
        assert company.format_as_prompt_constraint('off', vocabulary_format) == ''

    def test_the_propositions_block_is_empty(self, company):
        assert company.format_as_proposition_constraint('off') == ''

    def test_the_class_names_are_still_available(self, company):
        """`class_names` is independent of the level: the level decides how much
        the prompt says *about* the vocabulary, not what the vocabulary is."""
        assert company.class_names() == rendered_class_names(company.index())
        assert len(company.class_names()) == 5

class TestAnOntologyWithNoTerms:
    """A header with no vocabulary under it is worse than no block at all."""

    @pytest.mark.parametrize('level', ['align', 'strict'])
    @pytest.mark.parametrize('vocabulary_format', ['prose', 'turtle'])
    def test_nothing_is_rendered(self, empty, level, vocabulary_format):
        assert empty.format_as_prompt_constraint(level, vocabulary_format) == ''

    @pytest.mark.parametrize('level', ['align', 'strict'])
    def test_the_propositions_block_is_empty_too(self, empty, level):
        assert empty.format_as_proposition_constraint(level) == ''

class TestValidation:

    @pytest.mark.parametrize('level', ['guide', 'STRICT', '', None])
    @pytest.mark.parametrize('vocabulary_format', ['prose', 'turtle'])
    def test_an_unknown_level_raises_in_either_format(self, company, level, vocabulary_format):
        with pytest.raises(ValueError, match='Unknown ontology authority level'):
            company.format_as_prompt_constraint(level, vocabulary_format)

    @pytest.mark.parametrize('level', ['guide', 'STRICT', '', None])
    def test_an_unknown_level_raises_for_the_propositions_block(self, company, level):
        with pytest.raises(ValueError, match='Unknown ontology authority level'):
            company.format_as_proposition_constraint(level)

    def test_an_unknown_vocabulary_format_raises(self, company):
        with pytest.raises(ValueError, match='Unknown ontology vocabulary format'):
            company.format_as_prompt_constraint('align', 'json-ld')

class TestTheChannelsStaySeparated:
    """An attribute emitted where a relationship belongs is lost,
    so which section a term appears under is load-bearing."""

    @pytest.fixture(scope='class')
    def block(self, company):
        return company.format_as_prompt_constraint('align')

    def test_all_three_sections_and_the_protocol_are_present(self, block):
        for heading in (CLASSES_HEADING, RELATIONSHIPS_HEADING, ATTRIBUTES_HEADING, PROTOCOL_HEADING):
            assert heading in block

    def test_every_declared_class_appears_in_the_class_section(self, company, block):
        classes = section(block, CLASSES_HEADING)
        for name in company.class_names():
            assert name in classes

    def test_every_object_property_appears_only_on_the_relationship_channel(self, company, block):
        relationships = section(block, RELATIONSHIPS_HEADING)
        attributes = section(block, ATTRIBUTES_HEADING)

        for name in ('WORKS_FOR', 'PLAYS_FOR', 'SUBSIDIARY_OF', 'ACQUIRED'):
            assert name in relationships
            assert name not in attributes

    def test_every_datatype_property_appears_only_on_the_attribute_channel(self, company, block):
        relationships = section(block, RELATIONSHIPS_HEADING)
        attributes = section(block, ATTRIBUTES_HEADING)

        for name in ('FOUNDED_YEAR', 'REVENUE', 'IS_PUBLICLY_TRADED', 'TICKER_SYMBOL'):
            assert name in attributes
            assert name not in relationships

class TestWhatTheRenderedNamesPromise:

    @pytest.fixture(scope='class')
    def block(self, company):
        return company.format_as_prompt_constraint('align')

    def test_every_name_the_block_shows_resolves_when_it_comes_back(self, company, block):
        """The invariant `naming.py` documents, checked against the rendered text.

        Each name is taken out of the block itself and put back through the
        parser's transform and then the index, so a rendering change that broke
        the round trip fails here rather than at extraction time.
        """
        index = company.index()

        for name in rendered_class_names(index):
            assert name in block
            assert index.resolve_class(name.title()) is not None

        for heading, resolve in (
            (RELATIONSHIPS_HEADING, index.resolve_object_predicate),
            (ATTRIBUTES_HEADING, index.resolve_datatype_predicate),
        ):
            names = [
                word for line in section(block, heading).splitlines()
                for word in [line.strip().split('  ')[0]]
                if word.isupper() and word.replace('_', '').isalpha()
            ]
            assert names
            for name in names:
                assert resolve(name.replace('_', ' ')) is not None, name

    def test_the_class_names_the_block_shows_are_the_ones_seeded_as_preferences(self, company, block):
        """The vocabulary block and the
        `{preferred_entity_classifications}` slot cannot name a class two ways."""
        classes = section(block, CLASSES_HEADING)
        for name in company.class_names():
            assert name in classes

    def test_a_declared_label_is_preferred_over_the_local_name(self, block):
        assert 'Sports Team' in block
        assert 'SportsTeam' not in block

    def test_a_declared_alias_is_offered_to_the_model(self, block):
        assert 'Ball Club' in block
        assert 'REQ_TO_HC' in block

    def test_an_alias_that_renders_to_the_primary_name_is_not_repeated(self, company):
        """It would tell the model nothing. `:Company rdfs:label "Company"` with
        `skos:altLabel "Corporation"` shows the alias but not itself twice."""
        block = company.format_as_prompt_constraint('align')
        assert block.count('Corporation') == 1

    def test_a_declared_comment_becomes_the_description(self, block):
        assert 'An incorporated commercial organization.' in block

    def test_the_hierarchy_is_shown_by_indentation_parent_first(self, company):
        """Alphabetical order would put `Athlete` above `Company` and lose the
        structure the model is being shown."""
        classes = section(company.format_as_prompt_constraint('align'), CLASSES_HEADING)
        lines = [line for line in classes.splitlines() if line.startswith('  ')]
        depth = {line.strip().split('  ')[0]: len(line) - len(line.lstrip()) for line in lines}

        assert depth['Agent'] < depth['Company'] < depth['Sports Team']
        assert depth['Agent'] < depth['Person'] < depth['Athlete']

    @pytest.mark.parametrize('property_name,rendered_type', [
        ('FOUNDED_YEAR', 'integer'),
        ('REVENUE', 'decimal number'),
        ('IS_PUBLICLY_TRADED', 'true/false'),
        ('INCORPORATED_ON', 'date'),
        ('TICKER_SYMBOL', 'text'),
    ])
    def test_an_xsd_range_is_named_in_terms_the_model_can_act_on(self, block, property_name, rendered_type):
        line = next(line for line in block.splitlines() if property_name in line)
        assert rendered_type in line

    def test_no_xsd_iri_is_shown_in_the_prose_format(self, block):
        """The prose rendering paraphrases the ranges; showing the IRI as well
        reads as an instruction to convert."""
        assert 'XMLSchema#' not in block

    def test_an_unconstrained_domain_or_range_says_so(self, block):
        assert 'anything' in section(block, RELATIONSHIPS_HEADING)
        assert 'any entity' in section(block, ATTRIBUTES_HEADING)

class TestTheLevelChangesOnlyTheClosing:

    def test_the_vocabulary_is_byte_identical_at_align_and_strict(self, company):
        """How much authority the vocabulary has is not a fact about what the
        vocabulary is, so the sections must not move between levels."""
        align = company.format_as_prompt_constraint('align')
        strict = company.format_as_prompt_constraint('strict')

        assert align[:align.index(PROTOCOL_HEADING)] == strict[:strict.index(PROTOCOL_HEADING)]
        assert align != strict

    def test_strict_tells_the_model_what_happens_to_an_unlisted_name(self, company):
        """The claim that makes `strict` honest: the filter does discard them."""
        assert 'discarded' in company.format_as_prompt_constraint('strict')
        assert 'discarded' not in company.format_as_prompt_constraint('align')

    def test_the_propositions_block_carries_the_class_names_at_both_levels(self, company):
        for level in ('align', 'strict'):
            block = company.format_as_proposition_constraint(level)
            for name in company.class_names():
                assert name in block

    def test_the_propositions_block_names_no_property(self, company):
        """That stage classifies the entities it names and extracts nothing else,
        so a property vocabulary there is prompt spent on work it cannot do."""
        block = company.format_as_proposition_constraint('strict')
        for name in ('WORKS_FOR', 'FOUNDED_YEAR', 'worksFor', 'foundedYear'):
            assert name not in block

class TestTheTurtleFormat:
    """Experimental, and measured a net loss - but it ships, so it has to be sound."""

    @pytest.fixture(scope='class')
    def block(self, company):
        return company.format_as_prompt_constraint('align', 'turtle')

    def test_it_shows_the_ontology_source_in_a_fenced_block(self, block):
        assert '```turtle' in block
        assert 'owl:Class' in block
        assert 'rdfs:subClassOf' in block

    def test_it_keeps_the_same_protocol_section_as_the_prose_format(self, company, block):
        """How to *use* a vocabulary is not a function of how it was written down,
        and holding this constant is what makes the two formats comparable."""
        prose = company.format_as_prompt_constraint('align')
        assert section(block, PROTOCOL_HEADING) == section(prose, PROTOCOL_HEADING)

    def test_it_still_maps_each_construct_to_a_channel(self, block):
        """The prose layout separates the channels silently; Turtle interleaves
        them, so the header has to say it in words."""
        assert 'owl:ObjectProperty' in block
        assert 'owl:DatatypeProperty' in block
        assert 'entity|RELATIONSHIP|entity' in block
        assert 'entity|ATTRIBUTE_NAME|value' in block

    def test_the_level_closing_is_the_same_text_as_the_prose_format(self, company):
        for level in ('align', 'strict'):
            turtle = company.format_as_prompt_constraint(level, 'turtle')
            prose = company.format_as_prompt_constraint(level, 'prose')
            assert section(turtle, PROTOCOL_HEADING) == section(prose, PROTOCOL_HEADING)

    def test_the_propositions_block_has_no_turtle_in_it(self, company):
        """There is no format argument on that method, by design: the stage renders
        classes as a flat list either way, because no property declaration could
        steer it."""
        block = company.format_as_proposition_constraint('align')

        assert '```' not in block
        assert 'owl:' not in block

class TestRenderingIsDeterministic:
    """Rendered once at configuration time and compared across runs by the
    recorded-prompt tests, so ordering may not depend on dict iteration."""

    @pytest.mark.parametrize('vocabulary_format', ['prose', 'turtle'])
    def test_the_same_ontology_renders_the_same_bytes(self, vocabulary_format):
        first = Ontology.load(FIXTURES / 'company.ttl')
        second = Ontology.load(FIXTURES / 'company.ttl')

        assert first.format_as_prompt_constraint('strict', vocabulary_format) == \
            second.format_as_prompt_constraint('strict', vocabulary_format)

    def test_the_class_names_are_sorted(self, company):
        assert company.class_names() == sorted(company.class_names())

    def test_a_name_the_parser_would_mangle_never_reaches_the_prompt(self, company):
        """Underscores in a class name and spaces in a property name both survive
        the parser as something else."""
        for name in company.class_names():
            assert '_' not in name
            assert resolution_key(name) == resolution_key(name.title())

class TestTheTurtleWrapperOnItsOwn:

    def test_empty_turtle_renders_no_block(self):
        """Reached when a caller serializes a graph that holds only prefixes."""
        from graphrag_toolkit.lexical_graph.indexing.extract.ontology.prompt_constraint import (
            format_turtle_vocabulary,
        )
        assert format_turtle_vocabulary('   \n', 'align') == ''
