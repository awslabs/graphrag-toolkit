# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the ontology helpers and NamespaceConfig validation."""

from importlib.resources import files

import pytest
from rdflib import Graph, Namespace, OWL, RDF, RDFS, XSD

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import (
    NamespaceConfig,
    sparql_literal,
)


def test_invalid_main_prefix_rejected():
    with pytest.raises(ValueError):
        NamespaceConfig(prefix='1bad')


def test_invalid_extra_prefix_rejected():
    with pytest.raises(ValueError):
        NamespaceConfig(extra_prefixes={'1bad': 'https://ex.test/schema#'})


def test_extra_prefix_conflicting_with_main_rejected():
    with pytest.raises(ValueError):
        NamespaceConfig(prefix='lg', extra_prefixes={'lg': 'https://different.test/schema#'})


def test_namespace_without_separator_gets_one_appended():
    ns = NamespaceConfig(schema_namespace='https://ex.test/schema',
                         instance_namespace='https://ex.test/data')
    assert ns.schema_namespace == 'https://ex.test/schema#'
    assert ns.instance_namespace == 'https://ex.test/data/'


@pytest.mark.parametrize('value,expected', [
    (None, None),
    (True, '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'),
    (False, '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'),
    (7, '"7"^^<http://www.w3.org/2001/XMLSchema#integer>'),
    (1.5, '"1.5"^^<http://www.w3.org/2001/XMLSchema#double>'),
    ('x', '"x"'),
])
def test_sparql_literal_renders_each_type(value, expected):
    assert sparql_literal(value) == expected


@pytest.fixture
def schema():
    resource = files('graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql').joinpath('lexical_graph.ttl')
    return Graph().parse(data=resource.read_text(), format='turtle')


def test_schema_property_types_match_ranges(schema):
    for property_iri, range_iri in schema.subject_objects(RDFS.range):
        expected_type = OWL.DatatypeProperty if str(range_iri).startswith(str(XSD)) else OWL.ObjectProperty
        assert (property_iri, RDF.type, expected_type) in schema


def test_schema_subproperties_share_object_property_type(schema):
    for child, parent in schema.subject_objects(RDFS.subPropertyOf):
        assert (child, RDF.type, OWL.ObjectProperty) in schema
        assert (parent, RDF.type, OWL.ObjectProperty) in schema


@pytest.mark.parametrize('property_name', ['subject', 'object'])
def test_schema_preserves_mixed_fact_properties(schema, property_name):
    lg = Namespace(NamespaceConfig().schema_namespace)
    property_iri = lg[property_name]
    assert set(schema.objects(property_iri, RDF.type)) == {RDF.Property}
    assert (property_iri, RDFS.domain, lg.Fact) in schema
    assert not list(schema.objects(property_iri, RDFS.range))
