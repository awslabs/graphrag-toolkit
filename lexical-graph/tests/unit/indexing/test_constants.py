# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for indexing constants module.

Tests verify that constants are defined correctly with expected values
and types. These constants are foundational to the indexing system.
"""

import pytest
from graphrag_toolkit.lexical_graph.indexing.constants import (
    TOPICS_KEY,
    PROPOSITIONS_KEY,
    SOURCE_DOC_KEY,
    LOCAL_ENTITY_CLASSIFICATION,
    DEFAULT_TOPIC,
    DEFAULT_CLASSIFICATION,
    DEFAULT_ENTITY_CLASSIFICATIONS
)


class TestPropertyNameConstants:
    """Tests for property name constants used in graph metadata."""

    def test_topics_key_value(self):
        """Verify TOPICS_KEY has expected value."""
        assert TOPICS_KEY == 'aws::graph::topics'
        assert isinstance(TOPICS_KEY, str)

    def test_propositions_key_value(self):
        """Verify PROPOSITIONS_KEY has expected value."""
        assert PROPOSITIONS_KEY == 'aws::graph::propositions'
        assert isinstance(PROPOSITIONS_KEY, str)

    def test_source_doc_key_value(self):
        """Verify SOURCE_DOC_KEY has expected value."""
        assert SOURCE_DOC_KEY == 'aws::graph::source_doc'
        assert isinstance(SOURCE_DOC_KEY, str)

    def test_property_keys_are_unique(self):
        """Verify all property keys are unique to prevent collisions."""
        keys = [TOPICS_KEY, PROPOSITIONS_KEY, SOURCE_DOC_KEY]
        assert len(keys) == len(set(keys)), "Property keys must be unique"

    def test_property_keys_follow_naming_convention(self):
        """Verify property keys follow aws::graph:: naming convention."""
        keys = [TOPICS_KEY, PROPOSITIONS_KEY, SOURCE_DOC_KEY]
        for key in keys:
            assert key.startswith('aws::graph::'), \
                f"Property key '{key}' should start with 'aws::graph::'"

    def test_property_keys_are_non_empty(self):
        """Verify property keys are non-empty strings."""
        keys = [TOPICS_KEY, PROPOSITIONS_KEY, SOURCE_DOC_KEY]
        for key in keys:
            assert len(key) > 0, f"Property key should not be empty"
            assert key.strip() == key, f"Property key '{key}' should not have leading/trailing whitespace"


class TestClassificationConstants:
    """Tests for entity classification constants."""

    def test_local_entity_classification_value(self):
        """Verify LOCAL_ENTITY_CLASSIFICATION has expected value."""
        assert LOCAL_ENTITY_CLASSIFICATION == '__Local_Entity__'
        assert isinstance(LOCAL_ENTITY_CLASSIFICATION, str)

    def test_default_topic_value(self):
        """Verify DEFAULT_TOPIC has expected value."""
        assert DEFAULT_TOPIC == 'context'
        assert isinstance(DEFAULT_TOPIC, str)

    def test_default_classification_value(self):
        """Verify DEFAULT_CLASSIFICATION has expected value."""
        assert DEFAULT_CLASSIFICATION == 'unknown'
        assert isinstance(DEFAULT_CLASSIFICATION, str)

    def test_local_entity_classification_format(self):
        """Verify LOCAL_ENTITY_CLASSIFICATION follows special marker format."""
        assert LOCAL_ENTITY_CLASSIFICATION.startswith('__'), \
            "Local entity classification should start with '__'"
        assert LOCAL_ENTITY_CLASSIFICATION.endswith('__'), \
            "Local entity classification should end with '__'"

    def test_default_values_are_non_empty(self):
        """Verify default classification values are non-empty."""
        assert len(DEFAULT_TOPIC) > 0
        assert len(DEFAULT_CLASSIFICATION) > 0
        assert len(LOCAL_ENTITY_CLASSIFICATION) > 0


class TestEntityClassificationsList:
    """Tests for DEFAULT_ENTITY_CLASSIFICATIONS list."""

    def test_default_entity_classifications_is_list(self):
        """Verify DEFAULT_ENTITY_CLASSIFICATIONS is a list."""
        assert isinstance(DEFAULT_ENTITY_CLASSIFICATIONS, list)

    def test_default_entity_classifications_not_empty(self):
        """Verify DEFAULT_ENTITY_CLASSIFICATIONS contains classifications."""
        assert len(DEFAULT_ENTITY_CLASSIFICATIONS) > 0, \
            "Should have at least one default entity classification"

    def test_default_entity_classifications_expected_values(self):
        """Verify DEFAULT_ENTITY_CLASSIFICATIONS contains expected classifications."""
        expected_classifications = [
            'Company',
            'Location',
            'Event',
            'Sports Team',
            'Person',
            'Role',
            'Product',
            'Service',
            'Creative Work',
            'Software',
            'Financial Instrument'
        ]
        assert DEFAULT_ENTITY_CLASSIFICATIONS == expected_classifications

    def test_default_entity_classifications_all_strings(self):
        """Verify all classifications are strings."""
        for classification in DEFAULT_ENTITY_CLASSIFICATIONS:
            assert isinstance(classification, str), \
                f"Classification '{classification}' should be a string"

    def test_default_entity_classifications_no_empty_strings(self):
        """Verify no empty strings in classifications list."""
        for classification in DEFAULT_ENTITY_CLASSIFICATIONS:
            assert len(classification) > 0, \
                "Classifications should not be empty strings"

    def test_default_entity_classifications_unique(self):
        """Verify all classifications are unique."""
        assert len(DEFAULT_ENTITY_CLASSIFICATIONS) == len(set(DEFAULT_ENTITY_CLASSIFICATIONS)), \
            "All entity classifications should be unique"

    def test_default_entity_classifications_no_whitespace_only(self):
        """Verify classifications are not whitespace-only strings."""
        for classification in DEFAULT_ENTITY_CLASSIFICATIONS:
            assert classification.strip() == classification, \
                f"Classification '{classification}' should not have leading/trailing whitespace"
            assert len(classification.strip()) > 0, \
                "Classifications should not be whitespace-only"

    def test_default_entity_classifications_count(self):
        """Verify expected number of default classifications."""
        assert len(DEFAULT_ENTITY_CLASSIFICATIONS) == 11, \
            "Should have exactly 11 default entity classifications"


class TestConstantsIntegration:
    """Integration tests verifying constants work together correctly."""

    def test_all_constants_are_strings_or_lists(self):
        """Verify all exported constants have expected types."""
        constants = {
            'TOPICS_KEY': TOPICS_KEY,
            'PROPOSITIONS_KEY': PROPOSITIONS_KEY,
            'SOURCE_DOC_KEY': SOURCE_DOC_KEY,
            'LOCAL_ENTITY_CLASSIFICATION': LOCAL_ENTITY_CLASSIFICATION,
            'DEFAULT_TOPIC': DEFAULT_TOPIC,
            'DEFAULT_CLASSIFICATION': DEFAULT_CLASSIFICATION,
            'DEFAULT_ENTITY_CLASSIFICATIONS': DEFAULT_ENTITY_CLASSIFICATIONS
        }
        
        for name, value in constants.items():
            if name == 'DEFAULT_ENTITY_CLASSIFICATIONS':
                assert isinstance(value, list), \
                    f"{name} should be a list"
            else:
                assert isinstance(value, str), \
                    f"{name} should be a string"

    def test_no_constant_collisions(self):
        """Verify string constants don't have duplicate values."""
        string_constants = [
            TOPICS_KEY,
            PROPOSITIONS_KEY,
            SOURCE_DOC_KEY,
            LOCAL_ENTITY_CLASSIFICATION,
            DEFAULT_TOPIC,
            DEFAULT_CLASSIFICATION
        ]
        
        # Check for duplicates
        seen = set()
        for constant in string_constants:
            assert constant not in seen, \
                f"Duplicate constant value found: '{constant}'"
            seen.add(constant)

    def test_default_classification_not_in_default_list(self):
        """Verify DEFAULT_CLASSIFICATION is not in DEFAULT_ENTITY_CLASSIFICATIONS."""
        # 'unknown' should not be in the predefined list of specific classifications
        assert DEFAULT_CLASSIFICATION not in DEFAULT_ENTITY_CLASSIFICATIONS, \
            "DEFAULT_CLASSIFICATION should be separate from specific classifications"

    def test_local_entity_not_in_default_list(self):
        """Verify LOCAL_ENTITY_CLASSIFICATION is not in DEFAULT_ENTITY_CLASSIFICATIONS."""
        # Local entity marker should not be in the standard classifications list
        assert LOCAL_ENTITY_CLASSIFICATION not in DEFAULT_ENTITY_CLASSIFICATIONS, \
            "LOCAL_ENTITY_CLASSIFICATION should be separate from standard classifications"


# Edge case and boundary tests

class TestConstantsEdgeCases:
    """Edge case tests for constants."""

    def test_property_keys_case_sensitivity(self):
        """Verify property keys maintain consistent casing."""
        # All property keys should be lowercase after the prefix
        assert TOPICS_KEY == TOPICS_KEY.lower()
        assert PROPOSITIONS_KEY == PROPOSITIONS_KEY.lower()
        assert SOURCE_DOC_KEY == SOURCE_DOC_KEY.lower()

    def test_classification_values_case_sensitivity(self):
        """Verify classification values maintain expected casing."""
        # DEFAULT_TOPIC and DEFAULT_CLASSIFICATION should be lowercase
        assert DEFAULT_TOPIC == DEFAULT_TOPIC.lower()
        assert DEFAULT_CLASSIFICATION == DEFAULT_CLASSIFICATION.lower()

    def test_entity_classifications_title_case(self):
        """Verify entity classifications use title case format."""
        for classification in DEFAULT_ENTITY_CLASSIFICATIONS:
            # Each word should start with uppercase (title case)
            words = classification.split()
            for word in words:
                assert word[0].isupper(), \
                    f"Word '{word}' in '{classification}' should start with uppercase"

    def test_constants_immutability_intent(self):
        """Verify constants are defined at module level (immutability intent)."""
        # This test documents that constants should not be modified
        # Python doesn't enforce immutability for module-level variables,
        # but we verify they are simple types that shouldn't be mutated
        
        # String constants are immutable by nature
        assert isinstance(TOPICS_KEY, str)
        assert isinstance(PROPOSITIONS_KEY, str)
        assert isinstance(SOURCE_DOC_KEY, str)
        
        # List constant should be treated as immutable (don't modify in code)
        assert isinstance(DEFAULT_ENTITY_CLASSIFICATIONS, list)
        # Verify it's a list of immutable strings
        for item in DEFAULT_ENTITY_CLASSIFICATIONS:
            assert isinstance(item, str)


# Parametrized tests for efficiency

@pytest.mark.parametrize("constant_name,expected_value", [
    ("TOPICS_KEY", "aws::graph::topics"),
    ("PROPOSITIONS_KEY", "aws::graph::propositions"),
    ("SOURCE_DOC_KEY", "aws::graph::source_doc"),
    ("LOCAL_ENTITY_CLASSIFICATION", "__Local_Entity__"),
    ("DEFAULT_TOPIC", "context"),
    ("DEFAULT_CLASSIFICATION", "unknown"),
])
def test_constant_values_parametrized(constant_name, expected_value):
    """
    Parametrized test verifying constant values.
    
    Tests each constant has its expected value using a single test function.
    """
    from graphrag_toolkit.lexical_graph.indexing import constants
    actual_value = getattr(constants, constant_name)
    assert actual_value == expected_value, \
        f"{constant_name} should equal '{expected_value}', got '{actual_value}'"


@pytest.mark.parametrize("classification", [
    'Company',
    'Location',
    'Event',
    'Sports Team',
    'Person',
    'Role',
    'Product',
    'Service',
    'Creative Work',
    'Software',
    'Financial Instrument'
])
def test_entity_classification_in_list(classification):
    """
    Parametrized test verifying each expected classification is in the list.
    
    Tests that each standard entity classification is present.
    """
    assert classification in DEFAULT_ENTITY_CLASSIFICATIONS, \
        f"Classification '{classification}' should be in DEFAULT_ENTITY_CLASSIFICATIONS"
