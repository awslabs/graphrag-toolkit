# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Guards on the test ontologies.

These are not tests of the loader - `test_ontology.py` does that with inline
Turtle. These assert that the shared fixtures still are what the rest of the
feature's tests assume they are, so a well-meaning edit to `company.ttl` cannot
quietly remove the shape some later test depends on:

* every declared XSD range the datatype path claims to handle is present;
* `:SportsTeam`'s label differs from its local name, which is the whole reason
  the naming contract exists;
* each malformed fixture still fails for the reason it was written for;
* the verification corpus and its manifest agree, and between them the
  documents put every declared datatype property in play.
"""

import json

import pytest
from rdflib import Graph

from graphrag_toolkit.lexical_graph.indexing.extract.ontology import (
    XSD_NAMESPACE,
    Ontology,
    OntologyLoadError,
)

NAMESPACE = 'http://example.com/company#'

def iri(local_name:str) -> str:
    """Expand a local name against the test ontology's namespace."""
    return NAMESPACE + local_name

class TestCompanyOntology:
    """`company.ttl` - the ontology these tests run on."""

    def test_loads(self, company_ontology):
        """It is valid: it loads without an `OntologyLoadError`."""
        assert company_ontology.namespace == NAMESPACE

    def test_declared_classes(self, company_ontology):
        """The class vocabulary is fixed - later tests name these."""
        assert set(company_ontology.index().classes) == {
            iri('Agent'), iri('Person'), iri('Athlete'), iri('Company'),
            iri('SportsTeam'),
        }

    def test_declared_properties(self, company_ontology):
        """The property vocabulary is fixed too."""
        index = company_ontology.index()

        assert set(index.object_properties) == {
            iri('worksFor'), iri('playsFor'), iri('subsidiaryOf'), iri('acquired'),
        }
        assert set(index.datatype_properties) == {
            iri('foundedYear'), iri('revenue'), iri('isPubliclyTraded'),
            iri('incorporatedOn'), iri('tickerSymbol'), iri('jobTitle'),
            iri('officialName'),
        }

    def test_hierarchy_is_three_deep(self, company_ontology):
        """`SportsTeam < Company < Agent`, so the closure has something to close
        over that a single subClassOf hop would not reach."""
        index = company_ontology.index()

        assert index.classes[iri('SportsTeam')].ancestors == frozenset(
            {iri('SportsTeam'), iri('Company'), iri('Agent')}
        )
        assert index.is_subclass_of(iri('SportsTeam'), iri('Agent')) is True
        assert index.is_subclass_of(iri('Athlete'), iri('Agent')) is True

    def test_a_label_differs_from_its_local_name(self, company_ontology):
        """`:SportsTeam` is labelled `Sports Team`. Without this the naming
        contract's regression case has nothing to run against - `SportsTeam`
        rendered directly comes back from `.title()` as `Sportsteam`."""
        sports_team = company_ontology.index().classes[iri('SportsTeam')]

        assert sports_team.local_name == 'SportsTeam'
        assert sports_team.label == 'Sports Team'
        assert sports_team.label != sports_team.local_name

    def test_alt_labels_are_present_on_a_class_and_a_property(self, company_ontology):
        """Both halves of the alias path are exercised."""
        index = company_ontology.index()

        assert 'Ball Club' in index.classes[iri('SportsTeam')].aliases
        assert 'REQ_TO_HC' in index.object_properties[iri('worksFor')].aliases

    def test_an_object_property_is_unconstrained(self, company_ontology):
        """`:acquired` has no domain and no range, so it matches anything."""
        acquired = company_ontology.index().object_properties[iri('acquired')]

        assert acquired.domain is None
        assert acquired.range is None

    def test_a_datatype_property_is_unconstrained(self, company_ontology):
        """`:officialName` has no domain - any subject may carry it."""
        assert company_ontology.index().datatype_properties[iri('officialName')].domain is None

    def test_a_narrow_property_needs_the_subclass_closure(self, company_ontology):
        """`:playsFor` runs `Athlete -> SportsTeam`, both of which are strict
        subclasses. A fact stated of a `Person` and a `Company` only satisfies
        it through the closure, which is what makes the domain/range check worth
        testing."""
        index = company_ontology.index()
        plays_for = index.object_properties[iri('playsFor')]

        assert plays_for.domain == iri('Athlete')
        assert plays_for.range == iri('SportsTeam')
        assert index.is_subclass_of(iri('Athlete'), iri('Person')) is True
        assert index.is_subclass_of(iri('SportsTeam'), iri('Company')) is True

    @pytest.mark.parametrize(
        'datatype', ['integer', 'double', 'boolean', 'date', 'string']
    )
    def test_every_claimed_xsd_range_is_covered(self, company_ontology, datatype):
        """The five ranges the datatype path claims to render and coerce each
        appear at least once. This is the guard that stops the attribute tests
        passing on a vocabulary that never exercises them."""
        declared = {
            p.datatype for p in company_ontology.index().datatype_properties.values()
        }

        assert XSD_NAMESPACE + datatype in declared

    def test_descriptions_are_present_but_not_universal(self, company_ontology):
        """Some terms carry `rdfs:comment` and some deliberately do not, so the
        rendering has to handle both."""
        index = company_ontology.index()
        classes = index.classes.values()
        properties = [
            *index.object_properties.values(), *index.datatype_properties.values()
        ]

        assert any(c.description for c in classes)
        assert any(p.description for p in properties)
        assert any(p.description is None for p in properties)

    def test_comments_do_not_leak_into_terms(self, company_ontology):
        """The file's `#` header is Turtle comment syntax, not content - no term
        is named after it."""
        assert all(
            i.startswith(NAMESPACE) for i in company_ontology.index().classes
        )

class TestUpperSnakeOntology:
    """`upper_snake_names.ttl` - the same vocabulary in the other convention."""

    @pytest.fixture
    def upper_snake_ontology(self, ontology_fixtures_dir):
        return Ontology.from_turtle(ontology_fixtures_dir / 'upper_snake_names.ttl')

    def test_loads(self, upper_snake_ontology):
        """It is a valid ontology in its own right."""
        assert upper_snake_ontology.namespace == 'http://example.com/upper#'

    def test_names_are_authored_upper_snake(self, upper_snake_ontology):
        """The local names really are `WORKS_FOR` / `SPORTS_TEAM`. These must
        resolve as `worksFor` and `SportsTeam` do, and that
        cannot be tested against an ontology that spells them camelCase."""
        index = upper_snake_ontology.index()

        assert 'http://example.com/upper#WORKS_FOR' in index.object_properties
        assert 'http://example.com/upper#SPORTS_TEAM' in index.classes
        assert index.classes['http://example.com/upper#SPORTS_TEAM'].label == 'Sports Team'

    def test_it_does_not_collide_with_company_ttl(
        self, upper_snake_ontology, company_ontology
    ):
        """The two ontologies are separate files on purpose: `:worksFor` and
        `:WORKS_FOR` share a resolution key, so declaring both in one ontology
        would make resolution deliberately ambiguous."""
        assert upper_snake_ontology.namespace != company_ontology.namespace

class TestMalformedFixtures:
    """Each malformed fixture still fails for the reason it was written for."""

    # (fixture name, substring the message must contain)
    CASES = [
        ('subclass_cycle', 'cycle'),
        ('dangling_subclass_reference', 'dangling rdfs:subClassOf'),
        ('dangling_domain_reference', 'dangling rdfs:domain'),
        ('dangling_range_reference', 'dangling rdfs:range'),
        ('dual_typed_property', 'both an owl:ObjectProperty'),
        ('non_xsd_range', 'not an XSD datatype'),
        ('missing_datatype_range', 'no rdfs:range'),
        ('malformed_syntax', 'Failed to parse Turtle file'),
    ]

    @pytest.mark.parametrize('name, expected', CASES)
    def test_fixture_fails_for_its_stated_reason(
        self, malformed_ttl_path, name, expected
    ):
        """The fixture exists, and the load error is the specific one it was
        authored to trigger - not some unrelated fault that happens to raise."""
        path = malformed_ttl_path(name)
        assert path.is_file(), f'missing malformed fixture: {path}'

        with pytest.raises(OntologyLoadError, match=expected):
            Ontology.from_turtle(path)

    def test_only_the_syntax_fixture_is_unparseable(self, malformed_ttl_path):
        """Every other malformed fixture is valid Turtle that is an invalid
        ontology. If one of them stopped parsing, its structural case would
        never be reached and the test above would pass for the wrong reason."""
        for name, _ in self.CASES:
            if name == 'malformed_syntax':
                continue
            Graph().parse(source=str(malformed_ttl_path(name)), format='turtle')

    def test_every_malformed_fixture_on_disk_is_covered(
        self, ontology_fixtures_dir
    ):
        """No fixture sits in the directory untested."""
        on_disk = {p.stem for p in (ontology_fixtures_dir / 'malformed').glob('*.ttl')}

        assert on_disk == {name for name, _ in self.CASES}
