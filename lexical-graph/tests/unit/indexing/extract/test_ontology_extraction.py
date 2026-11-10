# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Loading an ontology, indexing it, and resolving emitted names against it.

Correctness only. Nothing here asserts a rate, a count over model output, or a
comparison between two configurations - those belong in a measurement document,
not in a test that gates a build.
"""

import logging

from pathlib import Path

import pytest
from rdflib import Graph

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.naming import (
    camel_to_upper_snake,
    resolution_key,
    title_case_with_spaces,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import (
    Ontology,
    OntologyLoadError,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology_index import (
    OntologyClass,
    OntologyIndex,
)
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import (
    format_classification,
    format_value,
)

FIXTURES = Path(__file__).parent.parent.parent.parent / 'fixtures' / 'ontologies'
COMPANY = FIXTURES / 'company.ttl'
NS = 'http://example.com/company#'
XSD = 'http://www.w3.org/2001/XMLSchema#'

@pytest.fixture(scope='module')
def index():
    return Ontology.load(COMPANY).index()

class TestNaming:
    """`naming.py`'s three jobs, and the invariant that ties two of them together."""

    @pytest.mark.parametrize('name,expected', [
        ('worksFor', 'works for'),
        ('WORKS FOR', 'works for'),
        ('WORKS_FOR', 'works for'),
        ('works  for', 'works for'),
        ('SportsTeam', 'sports team'),
        ('Sports Team', 'sports team'),
        ('', ''),
        (None, ''),
    ])
    def test_resolution_key_folds_every_convention_onto_one(self, name, expected):
        assert resolution_key(name) == expected

    def test_resolution_key_does_not_fold_word_boundaries(self):
        """The documented limit of the filter's reach: one word is not two."""
        assert resolution_key('Sportsteam') != resolution_key('SportsTeam')

    @pytest.mark.parametrize('name,expected', [
        ('worksFor', 'WORKS_FOR'),
        ('WORKS_FOR', 'WORKS_FOR'),
        ('founded year', 'FOUNDED_YEAR'),
        ('', ''),
    ])
    def test_properties_render_as_upper_snake(self, name, expected):
        assert camel_to_upper_snake(name) == expected

    @pytest.mark.parametrize('name,expected', [
        ('SportsTeam', 'Sports Team'),
        ('SPORTS_TEAM', 'Sports Team'),
        ('Sports Team', 'Sports Team'),
        ('', ''),
    ])
    def test_classes_render_as_title_case(self, name, expected):
        assert title_case_with_spaces(name) == expected

    def test_a_rendered_name_survives_the_parser_and_still_resolves(self, index):
        """The invariant the module exists for, over every term in the ontology.

        Render the term for the prompt, put it through the transform the response
        parser applies, and it must still fold onto the term's own key. This is
        what makes `title_case_with_spaces` rather than the local name the right
        rendering: `SportsTeam` comes back from the parser as `Sportsteam`.
        """
        for ontology_class in index.classes.values():
            rendered = title_case_with_spaces(ontology_class.local_name)
            assert resolution_key(format_classification(rendered)) == \
                resolution_key(ontology_class.local_name)

        properties = list(index.object_properties.values()) + list(index.datatype_properties.values())
        for prop in properties:
            rendered = camel_to_upper_snake(prop.local_name)
            assert resolution_key(format_value(rendered)) == resolution_key(prop.local_name)

class TestLoading:
    """`Ontology.load` accepts what configuration accepts, and refuses the rest."""

    def test_a_path_loads(self):
        assert Ontology.load(COMPANY).index().classes

    def test_a_string_path_loads(self):
        assert Ontology.load(str(COMPANY)).index().classes

    def test_a_graph_is_adopted(self):
        graph = Graph()
        graph.parse(source=str(COMPANY), format='turtle')
        assert len(Ontology.load(graph).index().classes) == 5

    def test_an_ontology_is_returned_unchanged(self):
        ontology = Ontology.load(COMPANY)
        assert Ontology.load(ontology) is ontology

    def test_a_turtle_string_needs_its_own_constructor(self):
        turtle = f'@prefix : <{NS}> . @prefix owl: <http://www.w3.org/2002/07/owl#> . :Thing a owl:Class .'
        assert Ontology.from_turtle_string(turtle).index().classes

    @pytest.mark.parametrize('source', [42, None, ['company.ttl']])
    def test_an_unsupported_source_raises(self, source):
        with pytest.raises(OntologyLoadError, match='Cannot load an ontology'):
            Ontology.load(source)

    def test_a_non_turtle_suffix_raises(self, tmp_path):
        path = tmp_path / 'company.owl'
        path.write_text('')
        with pytest.raises(OntologyLoadError, match='Unsupported ontology file format'):
            Ontology.load(path)

    def test_a_missing_file_raises(self, tmp_path):
        with pytest.raises(OntologyLoadError, match='not found'):
            Ontology.load(tmp_path / 'absent.ttl')

    @pytest.mark.parametrize('name', [
        'malformed_syntax.ttl',
        'missing_datatype_range.ttl',
        'non_xsd_range.ttl',
        'dual_typed_property.ttl',
        'dangling_subclass_reference.ttl',
        'dangling_domain_reference.ttl',
        'dangling_range_reference.ttl',
        'subclass_cycle.ttl',
    ])
    def test_a_structurally_invalid_ontology_is_refused_at_load_time(self, name):
        """Every failure is refused where it can still be reported to a user.

        Loading happens in the parent process at configuration time; the filter
        runs in a spawn worker per node batch. Anything not caught here surfaces
        as a warning nobody reads.
        """
        with pytest.raises(OntologyLoadError):
            Ontology.load(FIXTURES / 'malformed' / name)

    def test_the_message_names_the_term_and_the_rule(self):
        """A load error a user cannot act on is a load error that wasted their time."""
        with pytest.raises(OntologyLoadError, match='not an XSD datatype'):
            Ontology.load(FIXTURES / 'malformed' / 'non_xsd_range.ttl')

class TestTheIndex:
    """What `OntologyIndex` says about a loaded ontology."""

    def test_every_declared_term_is_indexed_under_its_kind(self, index):
        assert set(index.classes) == {
            f'{NS}Agent', f'{NS}Person', f'{NS}Athlete', f'{NS}Company', f'{NS}SportsTeam',
        }
        assert set(index.object_properties) == {
            f'{NS}worksFor', f'{NS}playsFor', f'{NS}subsidiaryOf', f'{NS}acquired',
        }
        assert len(index.datatype_properties) == 7

    def test_a_declared_label_and_alias_are_carried(self, index):
        sports_team = index.classes[f'{NS}SportsTeam']
        assert (sports_team.local_name, sports_team.label) == ('SportsTeam', 'Sports Team')
        assert sports_team.aliases == ['Ball Club']

    def test_ancestors_are_the_reflexive_transitive_closure(self, index):
        assert index.classes[f'{NS}SportsTeam'].ancestors == {
            f'{NS}SportsTeam', f'{NS}Company', f'{NS}Agent',
        }
        assert index.classes[f'{NS}Agent'].ancestors == {f'{NS}Agent'}

    @pytest.mark.parametrize('child,parent,expected', [
        ('SportsTeam', 'Company', True),
        ('SportsTeam', 'Agent', True),
        ('SportsTeam', 'SportsTeam', True),
        ('Company', 'SportsTeam', False),
        ('Athlete', 'Company', False),
    ])
    def test_is_subclass_of_honours_the_closure(self, index, child, parent, expected):
        assert index.is_subclass_of(f'{NS}{child}', f'{NS}{parent}') is expected

    def test_is_subclass_of_an_undeclared_iri_is_false_not_an_error(self, index):
        """Callers pass unresolved classifications straight through."""
        assert index.is_subclass_of(f'{NS}Absent', f'{NS}Agent') is False

    def test_a_declared_domain_range_and_datatype_are_carried(self, index):
        works_for = index.object_properties[f'{NS}worksFor']
        assert (works_for.domain, works_for.range) == (f'{NS}Person', f'{NS}Company')
        assert index.datatype_properties[f'{NS}foundedYear'].datatype == f'{XSD}integer'

    def test_an_undeclared_domain_or_range_is_none_meaning_anything(self, index):
        acquired = index.object_properties[f'{NS}acquired']
        assert (acquired.domain, acquired.range) == (None, None)
        assert index.datatype_properties[f'{NS}officialName'].domain is None

class TestResolution:
    """Turning a name the model emitted into a declared term."""

    @pytest.mark.parametrize('emitted', ['Sports Team', 'SPORTS_TEAM', 'SportsTeam', 'sports team'])
    def test_a_class_resolves_from_any_convention(self, index, emitted):
        assert index.resolve_class(emitted).iri == f'{NS}SportsTeam'

    @pytest.mark.parametrize('emitted,iri', [('Ball Club', 'SportsTeam'), ('Corporation', 'Company')])
    def test_a_class_resolves_from_a_declared_alias(self, index, emitted, iri):
        assert index.resolve_class(emitted).iri == f'{NS}{iri}'

    @pytest.mark.parametrize('emitted', ['WORKS FOR', 'WORKS_FOR', 'worksFor', 'REQ_TO_HC'])
    def test_an_object_predicate_resolves_from_any_convention_or_alias(self, index, emitted):
        assert index.resolve_object_predicate(emitted).iri == f'{NS}worksFor'

    @pytest.mark.parametrize('emitted', ['FOUNDED YEAR', 'foundedYear', 'founded_year'])
    def test_a_datatype_predicate_resolves_from_any_convention(self, index, emitted):
        assert index.resolve_datatype_predicate(emitted).iri == f'{NS}foundedYear'

    @pytest.mark.parametrize('emitted', ['HIRED BY', 'EMPLOYER', ''])
    def test_a_name_the_ontology_does_not_carry_resolves_to_nothing(self, index, emitted):
        assert index.resolve_class(emitted) is None
        assert index.resolve_object_predicate(emitted) is None
        assert index.resolve_datatype_predicate(emitted) is None

    @pytest.mark.parametrize('emitted', ['Sportsteam', 'SPORTSTEAM', 'sportsteam'])
    def test_a_class_resolves_even_with_the_word_boundary_destroyed(self, index, emitted):
        """The ontology's own spelling is the authority, so `:SportsTeam` has to
        work when the model writes it.

        `format_classification` applies `.title()`, so `SportsTeam` comes back from
        the parser as `Sportsteam` - one word where the index holds two. The
        fallback compact key closes that, which is what makes
        `vocabulary_format='turtle'` usable at all: that block shows the ontology's
        own source, so the model writes the local name.
        """
        assert index.resolve_class(emitted).iri == f'{NS}SportsTeam'

    def test_the_fallback_never_applies_to_predicates(self, index):
        """Properties need no fallback - `format_value` only maps `_` to a space,
        so `worksFor` survives it and `resolution_key` splits the camel boundary at
        both ends. Adding one would only widen what a predicate can mean."""
        assert index.resolve_object_predicate('worksfor') is None
        assert index.resolve_datatype_predicate('foundedyear') is None

    def test_the_two_predicate_kinds_do_not_answer_for_each_other(self, index):
        assert index.resolve_datatype_predicate('WORKS FOR') is None
        assert index.resolve_object_predicate('FOUNDED YEAR') is None

    def test_a_local_name_outranks_another_terms_alias(self):
        """Resolution precedence, which decides a genuine authoring collision.

        Two classes claim the key `company`: one owns it as its local name, the
        other as an alias. The owner wins, so resolution is a function of the
        ontology's content and not of dict iteration order.
        """
        index = OntologyIndex(classes={
            'urn:x#Company': OntologyClass(iri='urn:x#Company', local_name='Company'),
            'urn:x#Firm': OntologyClass(iri='urn:x#Firm', local_name='Firm', aliases=['Company']),
        })

        assert index.resolve_class('Company').iri == 'urn:x#Company'

    def test_the_key_maps_are_derived_and_survive_a_round_trip(self):
        """The index is pickled into a spawn worker, so a rebuilt one must resolve.

        Keys passed in are discarded and recomputed, so an index cannot carry a
        lookup table that disagrees with its terms.
        """
        index = OntologyIndex(
            classes={'urn:x#Company': OntologyClass(iri='urn:x#Company', local_name='Company')},
            class_by_key={'nonsense': 'urn:x#Absent'},
        )

        assert index.resolve_class('Company').iri == 'urn:x#Company'
        assert index.resolve_class('nonsense') is None

        rebuilt = OntologyIndex.model_validate(index.model_dump())
        assert rebuilt.resolve_class('COMPANY').iri == 'urn:x#Company'

class TestOntologiesThatAreNotTheFixture:
    """Shapes a real ontology has that `company.ttl` deliberately does not."""

    PREFIXES = (
        '@prefix owl: <http://www.w3.org/2002/07/owl#> . '
        '@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . '
        '@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . '
    )

    def load(self, body, prefix='@prefix : <urn:x#> . '):
        return Ontology.from_turtle_string(self.PREFIXES + prefix + body)

    def test_a_slash_iri_still_yields_a_local_name(self):
        """Plenty of published vocabularies separate on `/` rather than `#`."""
        ontology = self.load(
            ':Company a owl:Class .', prefix='@prefix : <http://example.com/vocab/> . ',
        )
        [ontology_class] = ontology.index().classes.values()

        assert ontology_class.local_name == 'Company'
        assert ontology.index().resolve_class('Company') is ontology_class

    def test_an_iri_with_no_separator_is_its_own_local_name(self):
        graph = Graph()
        graph.parse(
            data='<urn:Company> a <http://www.w3.org/2002/07/owl#Class> .', format='turtle',
        )
        assert Ontology.load(graph).index().classes['urn:Company'].local_name == 'urn:Company'

    def test_an_anonymous_class_axiom_is_not_vocabulary(self):
        """An `owl:Restriction` body is not a term a user can name in a domain or
        range, and not something a model can be asked to emit."""
        ontology = self.load(
            ':Company a owl:Class . '
            '[] a owl:Class, owl:ObjectProperty, owl:DatatypeProperty ; rdfs:range xsd:string .'
        )
        index = ontology.index()

        assert list(index.classes) == ['urn:x#Company']
        assert index.object_properties == {}
        assert index.datatype_properties == {}

    def test_a_second_domain_declaration_is_narrowed_with_a_warning(self, caplog):
        """One domain and one range per property is what the rendering and the
        domain/range check are defined over."""
        import logging

        with caplog.at_level(logging.WARNING):
            ontology = self.load(
                ':A a owl:Class . :B a owl:Class . '
                ':rel a owl:ObjectProperty ; rdfs:domain :A, :B ; rdfs:range :A .'
            )

        assert ontology.index().object_properties['urn:x#rel'].domain in ('urn:x#A', 'urn:x#B')
        assert 'One domain and one range per property is supported' in caplog.text

    def test_owl_thing_in_a_slot_is_the_same_as_no_slot(self):
        ontology = self.load(
            ':A a owl:Class . :rel a owl:ObjectProperty ; rdfs:domain owl:Thing ; rdfs:range :A .'
        )
        assert ontology.index().object_properties['urn:x#rel'].domain is None

    def test_a_dangling_datatype_property_domain_is_refused(self):
        with pytest.raises(OntologyLoadError, match='dangling rdfs:domain'):
            self.load(':attr a owl:DatatypeProperty ; rdfs:domain :Absent ; rdfs:range xsd:string .')

    def test_a_class_with_two_parents_is_rendered_once_and_names_the_others(self):
        """Rendered under the first parent, with the rest named on its own line, so
        nothing is lost and no subtree is duplicated."""
        ontology = self.load(
            ':Agent a owl:Class . :Legal a owl:Class . '
            ':Company a owl:Class ; rdfs:subClassOf :Agent, :Legal .'
        )
        block = ontology.format_as_prompt_constraint('align')

        assert block.count('Company') == 1
        assert 'also a kind of' in block
        assert ontology.index().classes['urn:x#Company'].ancestors == {
            'urn:x#Company', 'urn:x#Agent', 'urn:x#Legal',
        }

    def test_an_alias_that_renders_to_nothing_is_dropped(self):
        """It would tell the model nothing, and an empty "also known as" is noise."""
        ontology = self.load(
            '@prefix skos: <http://www.w3.org/2004/02/skos/core#> . '
            ':Company a owl:Class ; skos:altLabel " ", "Corporation", "Corporation" .'
        )
        block = ontology.format_as_prompt_constraint('align')

        assert block.count('Corporation') == 1
        assert 'also known as Corporation)' in block

    def test_an_alias_identical_to_the_primary_name_is_not_offered_twice(self):
        """It tells the model nothing, and reads as two names for one thing."""
        ontology = self.load(
            '@prefix skos: <http://www.w3.org/2004/02/skos/core#> . '
            ':Company a owl:Class ; skos:altLabel "COMPANY" .'
        )
        assert 'also known as' not in ontology.format_as_prompt_constraint('align')

    def test_a_graph_that_is_not_a_graph_is_refused(self):
        with pytest.raises(OntologyLoadError):
            Ontology.from_graph('@prefix : <urn:x#> .')

    def test_turtle_that_does_not_parse_is_refused(self):
        with pytest.raises(OntologyLoadError, match='Failed to parse Turtle string'):
            Ontology.from_turtle_string(':Company a owl:Class')

    @pytest.mark.parametrize('turtle,expected', [
        ('@prefix : <urn:base#> . <urn:base> a owl:Ontology .', 'urn:base#'),
        ('<urn:header> a owl:Ontology . <urn:header#A> a owl:Class .', 'urn:header'),
    ])
    def test_the_namespace_prefers_an_in_file_declaration(self, turtle, expected):
        """In priority order: the `@prefix :` declaration, then rdflib's default
        namespace, then the first `owl:Ontology` subject, then the supplied base -
        last, so a declaration in the file always wins."""
        ontology = Ontology.from_turtle_string(self.PREFIXES + turtle, base_iri='urn:given#')
        assert ontology.namespace == expected

    def test_the_supplied_base_is_used_when_the_file_declares_nothing(self):
        graph = Graph()
        graph.parse(data='<urn:x#A> a <http://www.w3.org/2002/07/owl#Class> .', format='turtle')

        assert Ontology(graph, base_iri='urn:given#').namespace == 'urn:given#'


class TestAxiomsThatShouldLoad:
    """Legal axioms that were refused, and silent no-ops that now warn."""

    PREFIXES = (
        '@prefix : <urn:x#> . '
        '@prefix owl: <http://www.w3.org/2002/07/owl#> . '
        '@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . '
        '@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . '
        '@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . '
    )

    def test_subclass_of_owl_thing_loads(self):
        """A legal axiom, and one Protege emits by default. It was rejected as a
        dangling `subClassOf` reference, and the only workaround - declaring
        `owl:Thing a owl:Class` - put a meaningless `Thing` type in the prompt.
        `_class_reference` already exempted `owl:Thing` from domain and range."""
        ontology = Ontology.from_turtle_string(
            self.PREFIXES + ':Company a owl:Class ; rdfs:subClassOf owl:Thing .'
        )
        company = ontology.index().classes['urn:x#Company']

        assert company.parents == []
        assert company.ancestors == frozenset({'urn:x#Company'})
        assert ontology.class_names() == ['Company']

    def test_an_rdfs_dialect_ontology_warns_that_it_declares_nothing(self, caplog):
        """It parses cleanly and yields no terms, which is worse than passing no
        ontology: `filter_required()` is still True and the preferred
        classifications are seeded empty, so at `strict` every fact is dropped -
        after the whole extraction run is paid for."""
        with caplog.at_level(logging.WARNING):
            ontology = Ontology.from_turtle_string(
                self.PREFIXES
                + ':Company a rdfs:Class ; rdfs:label "Company" . '
                + ':worksFor a rdf:Property ; rdfs:domain :Person ; rdfs:range :Company .'
            )

        assert ontology.index().classes == {}
        assert 'declares no owl:Class' in caplog.text
        assert 'rdfs:Class' in caplog.text

    def test_an_ontology_with_terms_does_not_warn(self, caplog):
        with caplog.at_level(logging.WARNING):
            Ontology.from_turtle_string(self.PREFIXES + ':Company a owl:Class .')

        assert 'declares no owl:Class' not in caplog.text

    def test_a_multiply_declared_datatype_range_warns(self, caplog):
        """`_class_reference` warns for exactly this ambiguity on a domain; the
        range was taken silently, and which of two ranges wins decides whether a
        literal coerces at all."""
        with caplog.at_level(logging.WARNING):
            Ontology.from_turtle_string(
                self.PREFIXES
                + ':foundedYear a owl:DatatypeProperty ; rdfs:range xsd:integer, xsd:string .'
            )

        assert 'rdfs:range values' in caplog.text

    def test_a_class_name_collision_warns(self, caplog):
        """One of the two becomes unreachable by every name it has, while the
        renderers still offer that name to the model."""
        with caplog.at_level(logging.WARNING):
            Ontology.from_turtle_string(
                self.PREFIXES
                + ':Company a owl:Class ; rdfs:label "Organisation" . '
                + ':Organisation a owl:Class .'
            )

        assert 'resolve under the name' in caplog.text


class TestTheCompactFallbackNeverGuesses:
    """The fallback widens resolution; it must not make it ambiguous."""

    PREFIXES = (
        '@prefix : <urn:x#> . '
        '@prefix owl: <http://www.w3.org/2002/07/owl#> . '
    )

    def index_for(self, body):
        return Ontology.from_turtle_string(self.PREFIXES + body).index()

    def test_a_compact_key_two_classes_claim_is_omitted(self):
        """`:ABCorp` and `:AB_Corp` are distinct classes with distinct resolution
        keys. Resolving a coarse match to a winner would lose the exactness the
        rest of the index guarantees, so the key is dropped from the fallback."""
        index = self.index_for(':ABCorp a owl:Class . :AB_Corp a owl:Class .')

        assert 'abcorp' not in index.class_by_compact_key

    def test_the_exact_path_is_unaffected_by_an_omitted_key(self):
        """Both classes still resolve by their own names - only the *coarse*
        lookup is withheld."""
        index = self.index_for(':ABCorp a owl:Class . :AB_Corp a owl:Class .')

        assert index.resolve_class('ABCorp').local_name == 'ABCorp'
        assert index.resolve_class('AB Corp').local_name == 'AB_Corp'

    def test_an_exact_match_always_wins_over_the_fallback(self):
        """`:Sportsteam` owns `'sportsteam'` outright, so it must not lose it to
        `:SportsTeam` via the coarse index."""
        index = self.index_for(':SportsTeam a owl:Class . :Sportsteam a owl:Class .')

        assert index.resolve_class('Sportsteam').local_name == 'Sportsteam'
        assert index.resolve_class('Sports Team').local_name == 'SportsTeam'

    def test_a_name_no_class_carries_still_resolves_to_nothing(self):
        index = self.index_for(':SportsTeam a owl:Class .')

        assert index.resolve_class('Football Club') is None
        assert index.resolve_class('') is None
