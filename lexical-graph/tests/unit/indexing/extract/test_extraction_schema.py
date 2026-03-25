# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_schema import (
    EntityTypeConfig,
    ExtractionSchema,
)


class TestEntityTypeConfig:
    """Tests for EntityTypeConfig."""

    def test_default_values(self):
        """Verify defaults are None/empty."""
        config = EntityTypeConfig()
        assert config.description is None
        assert config.attributes == []
        assert config.aliases == []

    def test_with_all_fields(self):
        """Verify all fields can be set."""
        config = EntityTypeConfig(
            description='A person entity',
            attributes=['name', 'age'],
            aliases=['Individual', 'Human'],
        )
        assert config.description == 'A person entity'
        assert config.attributes == ['name', 'age']
        assert config.aliases == ['Individual', 'Human']


class TestExtractionSchema:
    """Tests for ExtractionSchema."""

    def test_default_values(self):
        """Verify defaults are empty/False."""
        schema = ExtractionSchema()
        assert schema.entity_types == {}
        assert schema.relationship_types == []
        assert schema.strict is False

    def test_entity_type_names(self):
        """Verify entity_type_names returns sorted names."""
        schema = ExtractionSchema(entity_types={
            'Person': EntityTypeConfig(),
            'Company': EntityTypeConfig(),
            'Location': EntityTypeConfig(),
        })
        assert schema.entity_type_names() == ['Company', 'Location', 'Person']

    def test_entity_type_names_empty(self):
        """Verify entity_type_names returns empty list when no types."""
        schema = ExtractionSchema()
        assert schema.entity_type_names() == []

    def test_format_as_prompt_constraint_empty(self):
        """Verify empty schema produces empty string."""
        schema = ExtractionSchema()
        assert schema.format_as_prompt_constraint() == ''

    def test_format_as_prompt_constraint_entity_types(self):
        """Verify entity types are formatted correctly."""
        schema = ExtractionSchema(entity_types={
            'Person': EntityTypeConfig(description='A human being', attributes=['name', 'age']),
        })
        result = schema.format_as_prompt_constraint()
        assert 'Entity types:' in result
        assert 'Person - A human being' in result
        assert 'Attributes: name, age' in result

    def test_format_as_prompt_constraint_relationship_types(self):
        """Verify relationship types are formatted correctly."""
        schema = ExtractionSchema(relationship_types=['WORKS_FOR', 'LOCATED_IN'])
        result = schema.format_as_prompt_constraint()
        assert 'Relationship types:' in result
        assert 'LOCATED_IN' in result
        assert 'WORKS_FOR' in result

    def test_format_as_prompt_constraint_strict(self):
        """Verify strict mode adds constraint text."""
        schema = ExtractionSchema(
            entity_types={'Person': EntityTypeConfig()},
            strict=True,
        )
        result = schema.format_as_prompt_constraint()
        assert 'STRICT MODE' in result

    def test_format_as_prompt_constraint_not_strict(self):
        """Verify non-strict mode omits constraint text."""
        schema = ExtractionSchema(
            entity_types={'Person': EntityTypeConfig()},
            strict=False,
        )
        result = schema.format_as_prompt_constraint()
        assert 'STRICT MODE' not in result

    def test_format_as_prompt_constraint_no_description(self):
        """Verify entity without description omits dash."""
        schema = ExtractionSchema(entity_types={
            'Person': EntityTypeConfig(),
        })
        result = schema.format_as_prompt_constraint()
        assert 'Person' in result
        assert ' - ' not in result
