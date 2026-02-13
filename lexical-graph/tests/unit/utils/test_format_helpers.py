import pytest

from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import (
    format_text,
    format_list,
    format_value,
    format_classification,
    strip_full_stop,
)
from graphrag_toolkit.lexical_graph.indexing.utils.fact_utils import (
    string_complement_to_entity,
)
from graphrag_toolkit.lexical_graph.indexing.model import Entity, Fact, Relation
from graphrag_toolkit.lexical_graph.indexing.constants import LOCAL_ENTITY_CLASSIFICATION


# ---------------------------------------------------------------------------
# format_text
# ---------------------------------------------------------------------------

class TestFormatText:

    @pytest.mark.parametrize("input_val,expected", [
        ("hello", "hello"),
        (["a", "b"], "a\nb"),
        (["single"], "single"),
        ([], ""),
    ])
    def test_format_text(self, input_val, expected):
        assert format_text(input_val) == expected


# ---------------------------------------------------------------------------
# format_list
# ---------------------------------------------------------------------------

class TestFormatList:

    @pytest.mark.parametrize("input_val,expected", [
        (["a", "b", "c"], "a\nb\nc"),
        (["only"], "only"),
        ([], ""),
    ])
    def test_format_list(self, input_val, expected):
        assert format_list(input_val) == expected


# ---------------------------------------------------------------------------
# format_value
# ---------------------------------------------------------------------------

class TestFormatValue:

    @pytest.mark.parametrize("input_val,expected", [
        ("hello_world", "hello world"),
        ("nounderscore", "nounderscore"),
        ("", ""),
        (None, ""),
        ("__double__", "  double  "),
    ])
    def test_format_value(self, input_val, expected):
        assert format_value(input_val) == expected


# ---------------------------------------------------------------------------
# format_classification
# ---------------------------------------------------------------------------

class TestFormatClassification:

    @pytest.mark.parametrize("input_val,expected", [
        ("natural_language", "Natural Language"),
        ("UPPER", "Upper"),
        ("", ""),
        (None, ""),
        ("single", "Single"),
    ])
    def test_format_classification(self, input_val, expected):
        assert format_classification(input_val) == expected


# ---------------------------------------------------------------------------
# strip_full_stop
# ---------------------------------------------------------------------------

class TestStripFullStop:

    @pytest.mark.parametrize("input_val,expected", [
        ("hello.", "hello"),
        ("hello", "hello"),
        (".", ""),
        ("", ""),
        (None, None),
        ("hello...", "hello.."),
    ])
    def test_strip_full_stop(self, input_val, expected):
        assert strip_full_stop(input_val) == expected


# ---------------------------------------------------------------------------
# string_complement_to_entity
# ---------------------------------------------------------------------------

class TestStringComplementToEntity:

    def test_converts_string_complement_to_entity(self):
        fact = Fact(
            subject=Entity(value="A", classification="X"),
            predicate=Relation(value="relates"),
            complement="some string",
        )
        result = string_complement_to_entity(fact)

        assert isinstance(result.complement, Entity)
        assert result.complement.value == "some string"
        assert result.complement.classification == LOCAL_ENTITY_CLASSIFICATION

    def test_preserves_existing_entity_complement(self):
        entity = Entity(value="B", classification="Y")
        fact = Fact(
            subject=Entity(value="A", classification="X"),
            predicate=Relation(value="relates"),
            complement=entity,
        )
        result = string_complement_to_entity(fact)

        assert result.complement is entity

    def test_none_complement_unchanged(self):
        fact = Fact(
            subject=Entity(value="A", classification="X"),
            predicate=Relation(value="relates"),
            complement=None,
        )
        result = string_complement_to_entity(fact)

        assert result.complement is None
