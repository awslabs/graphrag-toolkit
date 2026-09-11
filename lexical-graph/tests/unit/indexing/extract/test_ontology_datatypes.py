# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Coercing an extracted literal to its declared XSD type.

Two properties matter and everything here serves one of them: a value that
parses must be **JSON-serializable**, because it lands in node metadata and then
in Cypher parameters; and a value that would need *interpreting* rather than
parsing must be refused, because a wrong number stored under a typed key is
trusted by everything downstream.
"""

import json

import pytest

from graphrag_toolkit.lexical_graph.indexing.extract.ontology.datatype_utils import (
    coerce_literal,
    validate_literal_against_xsd,
    validates_datatype,
)

XSD = 'http://www.w3.org/2001/XMLSchema#'

def xsd(name):
    return f'{XSD}{name}'

class TestAcceptedForms:
    """Lexical latitude: the shapes a model actually emits for a right answer."""

    @pytest.mark.parametrize('literal,expected', [
        ('1994', 1994),
        ('  1994  ', 1994),
        ('+1994', 1994),
        ('-1994', -1994),
        ('1994.0', 1994),
        ('2,400,000', 2400000),
    ])
    def test_integers(self, literal, expected):
        assert coerce_literal(literal, xsd('integer')) == expected

    @pytest.mark.parametrize('literal,expected', [
        ('41500000', 41500000.0),
        ('41,500,000', 41500000.0),
        ('4.15e7', 41500000.0),
        ('0', 0.0),
        ('-2.5', -2.5),
    ])
    def test_decimals(self, literal, expected):
        assert coerce_literal(literal, xsd('double')) == expected

    @pytest.mark.parametrize('literal,expected', [
        ('true', True), ('TRUE', True), ('1', True),
        ('false', False), ('False', False), ('0', False),
    ])
    def test_booleans(self, literal, expected):
        assert coerce_literal(literal, xsd('boolean')) is expected

    @pytest.mark.parametrize('literal', [
        '2020-03-03', 'March 3 2020', 'March 3rd 2020', '3 March 2020',
        'Mar 3, 2020', '2020-03-03Z', '2020-03-03+01:00',
    ])
    def test_dates_reduce_to_one_iso_string(self, literal):
        assert coerce_literal(literal, xsd('date')) == '2020-03-03'

    @pytest.mark.parametrize('name,literal,expected', [
        ('dateTime', '2020-03-03T09:30:00', '2020-03-03T09:30:00'),
        ('time', '09:30', '09:30:00'),
        ('time', '09:30:15', '09:30:15'),
        ('anyURI', 'https://example.com/a', 'https://example.com/a'),
        ('string', '  spaced  ', 'spaced'),
    ])
    def test_the_remaining_families(self, name, literal, expected):
        assert coerce_literal(literal, xsd(name)) == expected

class TestRefusedForms:
    """Where parsing stops and interpreting would begin."""

    @pytest.mark.parametrize('literal', [
        'nineteen ninety four',
        '1994.5',
        '1,99,4',
        '1994,5',
        '',
        '   ',
    ])
    def test_a_value_that_is_not_an_integer_in_its_entirety(self, literal):
        assert coerce_literal(literal, xsd('integer')) is None

    @pytest.mark.parametrize('literal', [
        '41,500,000 dollars',
        '$41.5m',
        'nine hundred million',
        'nan',
        'inf',
    ])
    def test_a_unit_or_a_magnitude_is_never_stripped(self, literal):
        """Nothing distinguishes a harmless unit from a magnitude without reading it.

        `nan` and `inf` are refused for a different reason: `float()` accepts
        both, and `json.dumps` then emits tokens no JSON parser must accept.
        """
        assert coerce_literal(literal, xsd('double')) is None

    @pytest.mark.parametrize('literal', ['yes', 'no', 'Y', 'TRUE.'])
    def test_a_boolean_synonym_nobody_was_offered(self, literal):
        assert coerce_literal(literal, xsd('boolean')) is None

    @pytest.mark.parametrize('literal', [
        '01/03/1994',
        '2015-02-29',
        '15-2-9',
        'sometime in March',
    ])
    def test_an_ambiguous_or_impossible_date(self, literal):
        """`01/03/1994` is two different days depending on where the text came from."""
        assert coerce_literal(literal, xsd('date')) is None

    def test_a_sentence_where_a_uri_was_asked_for(self):
        assert coerce_literal('see the company website', xsd('anyURI')) is None

    @pytest.mark.parametrize('literal,datatype', [(None, xsd('integer')), ('1994', None), (None, None)])
    def test_a_missing_literal_or_datatype(self, literal, datatype):
        assert coerce_literal(literal, datatype) is None

    def test_a_non_xsd_range_is_refused_rather_than_guessed_at(self):
        assert coerce_literal('anything', 'http://example.com/company#Money') is None

class TestBounds:
    """The reason the integer types are enumerated rather than treated alike."""

    @pytest.mark.parametrize('name,literal,accepted', [
        ('nonNegativeInteger', '0', True),
        ('nonNegativeInteger', '-5', False),
        ('positiveInteger', '1', True),
        ('positiveInteger', '0', False),
        ('negativeInteger', '-1', True),
        ('negativeInteger', '0', False),
        ('unsignedByte', '255', True),
        ('unsignedByte', '256', False),
        ('byte', '-128', True),
        ('byte', '-129', False),
        ('short', '32767', True),
        ('short', '32768', False),
    ])
    def test_a_value_outside_its_declared_type_is_refused(self, name, literal, accepted):
        assert (coerce_literal(literal, xsd(name)) is not None) is accepted

    def test_gyear_is_an_integer_because_that_is_what_the_prompt_promised(self):
        assert coerce_literal('1994', xsd('gYear')) == 1994

class TestWhatTheCallersDependOn:

    @pytest.mark.parametrize('name,literal', [
        ('integer', '1994'), ('double', '2.5'), ('boolean', 'true'),
        ('date', '2020-03-03'), ('dateTime', '2020-03-03T09:30:00'),
        ('time', '09:30'), ('string', 'text'), ('anyURI', 'https://example.com'),
    ])
    def test_every_successful_coercion_is_json_serializable(self, name, literal):
        value = coerce_literal(literal, xsd(name))
        assert json.loads(json.dumps(value)) == value

    @pytest.mark.parametrize('name,literal', [('boolean', 'false'), ('integer', '0'), ('double', '0')])
    def test_a_falsy_result_is_still_a_successful_coercion(self, name, literal):
        """Callers must test `is not None`. `False`, `0` and `0.0` all coerce."""
        assert coerce_literal(literal, xsd(name)) is not None
        assert not coerce_literal(literal, xsd(name))

    @pytest.mark.parametrize('name,expected', [
        ('integer', True), ('double', True), ('boolean', True), ('date', True),
        ('string', True), ('token', True),
        ('hexBinary', False), ('duration', False), ('gMonthDay', False),
    ])
    def test_validates_datatype_reports_whether_a_check_actually_happens(self, name, expected):
        assert validates_datatype(xsd(name)) is expected

    @pytest.mark.parametrize('datatype', [None, 'http://example.com/company#Money'])
    def test_validates_datatype_is_false_for_a_non_xsd_range(self, datatype):
        assert validates_datatype(datatype) is False

    def test_an_unimplemented_xsd_type_keeps_the_text_and_admits_it(self):
        """The pair that lets `enforce_datatypes` warn instead of claiming a check.

        Coercion returns the trimmed text so nothing is lost, and
        `validates_datatype` returns False so the caller can say out loud that
        the declared type was not honoured.
        """
        assert coerce_literal('  deadbeef  ', xsd('hexBinary')) == 'deadbeef'
        assert validates_datatype(xsd('hexBinary')) is False

    def test_validity_is_coercibility(self):
        """One implementation, so the two can never disagree about a value."""
        for (literal, datatype) in [('1994', xsd('integer')), ('x', xsd('integer')), ('false', xsd('boolean'))]:
            assert validate_literal_against_xsd(literal, datatype) is (
                coerce_literal(literal, datatype) is not None
            )

class TestTheTimeFamilyEdges:
    """`dateTime` and `time` accept only the ISO lexical form, and reject in two
    different ways: the shape fails, or the calendar does."""

    @pytest.mark.parametrize('literal', ['yesterday', '2020-13-01T00:00:00', 'March 3 2020'])
    def test_a_datetime_that_is_not_iso(self, literal):
        """Unlike `xsd:date`, a named month is not accepted here - no model in the
        corpus produced one, and `fromisoformat` is the whole of the contract."""
        assert coerce_literal(literal, xsd('dateTime')) is None

    def test_a_space_separated_datetime_is_accepted(self):
        """`datetime.fromisoformat` takes it, so this module does too."""
        assert coerce_literal('2020-03-03 09:30', xsd('dateTime')) == '2020-03-03T09:30:00'

    @pytest.mark.parametrize('literal', ['9:30', 'half past nine', '25:00', '09:99'])
    def test_a_time_that_is_the_wrong_shape_or_not_on_the_clock(self, literal):
        assert coerce_literal(literal, xsd('time')) is None
