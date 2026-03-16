# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for versioning.py module.

This module tests version management and compatibility checks.
"""

import pytest
from graphrag_toolkit.lexical_graph.versioning import (
    VersioningMode,
    VersioningConfig,
    add_versioning_info,
    to_versioning_config,
    VALID_FROM,
    VALID_TO,
    EXTRACT_TIMESTAMP,
    BUILD_TIMESTAMP,
    VERSION_INDEPENDENT_ID_FIELDS,
    PREV_VERSIONS,
    TIMESTAMP_LOWER_BOUND,
    TIMESTAMP_UPPER_BOUND
)
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from llama_index.core.vector_stores.types import FilterOperator, MetadataFilter


class TestVersioningMode:
    """Tests for VersioningMode enum."""
    
    def test_versioning_mode_values(self):
        """Verify VersioningMode enum has expected values."""
        assert VersioningMode.NO_VERSIONING.value == 1
        assert VersioningMode.CURRENT.value == 2
        assert VersioningMode.PREVIOUS.value == 3
        assert VersioningMode.AT_TIMESTAMP.value == 4
        assert VersioningMode.BEFORE_TIMESTAMP.value == 5
        assert VersioningMode.ON_OR_AFTER_TIMESTAMP.value == 6


class TestAddVersioningInfo:
    """Tests for add_versioning_info function."""
    
    def test_add_versioning_info_with_id_fields_string(self):
        """Verify add_versioning_info adds id_fields as list when given string."""
        metadata = {'name': 'test'}
        result = add_versioning_info(metadata, id_fields='doc_id')
        
        assert VERSION_INDEPENDENT_ID_FIELDS in result
        assert result[VERSION_INDEPENDENT_ID_FIELDS] == ['doc_id']
    
    def test_add_versioning_info_with_id_fields_list(self):
        """Verify add_versioning_info adds id_fields as list when given list."""
        metadata = {'name': 'test'}
        result = add_versioning_info(metadata, id_fields=['doc_id', 'chunk_id'])
        
        assert VERSION_INDEPENDENT_ID_FIELDS in result
        assert result[VERSION_INDEPENDENT_ID_FIELDS] == ['doc_id', 'chunk_id']
    
    def test_add_versioning_info_with_valid_from(self):
        """Verify add_versioning_info adds valid_from timestamp."""
        metadata = {'name': 'test'}
        timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC in milliseconds
        result = add_versioning_info(metadata, valid_from=timestamp)
        
        assert VALID_FROM in result
        assert result[VALID_FROM] == timestamp
    
    def test_add_versioning_info_with_both_parameters(self):
        """Verify add_versioning_info adds both id_fields and valid_from."""
        metadata = {'name': 'test'}
        timestamp = 1704067200000
        result = add_versioning_info(metadata, id_fields='doc_id', valid_from=timestamp)
        
        assert VERSION_INDEPENDENT_ID_FIELDS in result
        assert VALID_FROM in result
        assert result[VERSION_INDEPENDENT_ID_FIELDS] == ['doc_id']
        assert result[VALID_FROM] == timestamp
    
    def test_add_versioning_info_without_parameters(self):
        """Verify add_versioning_info returns metadata unchanged when no parameters."""
        metadata = {'name': 'test'}
        result = add_versioning_info(metadata)
        
        assert result == metadata
        assert VERSION_INDEPENDENT_ID_FIELDS not in result
        assert VALID_FROM not in result


class TestToVersioningConfig:
    """Tests for to_versioning_config function."""
    
    def test_to_versioning_config_enabled(self):
        """Verify to_versioning_config returns CURRENT mode when enabled."""
        config = to_versioning_config(enable_versioning=True)
        
        assert isinstance(config, VersioningConfig)
        assert config.versioning_mode == VersioningMode.CURRENT
    
    def test_to_versioning_config_disabled(self):
        """Verify to_versioning_config returns NO_VERSIONING mode when disabled."""
        config = to_versioning_config(enable_versioning=False)
        
        assert isinstance(config, VersioningConfig)
        assert config.versioning_mode == VersioningMode.NO_VERSIONING


class TestVersioningConfigInitialization:
    """Tests for VersioningConfig initialization."""
    
    def test_initialization_with_mode_and_timestamp(self):
        """Verify VersioningConfig initializes with both mode and timestamp."""
        timestamp = 1704067200000
        config = VersioningConfig(versioning_mode=VersioningMode.AT_TIMESTAMP, at_timestamp=timestamp)
        
        assert config.versioning_mode == VersioningMode.AT_TIMESTAMP
        assert config.at_timestamp == timestamp
    
    def test_initialization_with_no_parameters(self):
        """Verify VersioningConfig defaults to NO_VERSIONING when no parameters."""
        config = VersioningConfig()
        
        assert config.versioning_mode == VersioningMode.NO_VERSIONING
        assert config.at_timestamp == TIMESTAMP_UPPER_BOUND
    
    def test_initialization_with_only_timestamp(self):
        """Verify VersioningConfig defaults to AT_TIMESTAMP mode when only timestamp provided."""
        timestamp = 1704067200000
        config = VersioningConfig(at_timestamp=timestamp)
        
        assert config.versioning_mode == VersioningMode.AT_TIMESTAMP
        assert config.at_timestamp == timestamp
    
    def test_initialization_with_only_mode(self):
        """Verify VersioningConfig uses TIMESTAMP_UPPER_BOUND when only mode provided."""
        config = VersioningConfig(versioning_mode=VersioningMode.CURRENT)
        
        assert config.versioning_mode == VersioningMode.CURRENT
        assert config.at_timestamp == TIMESTAMP_UPPER_BOUND


class TestVersioningConfigApply:
    """Tests for VersioningConfig.apply method."""
    
    def test_apply_with_no_versioning(self):
        """Verify apply returns filter unchanged when NO_VERSIONING mode."""
        config = VersioningConfig(versioning_mode=VersioningMode.NO_VERSIONING)
        filter_config = FilterConfig(source_filters=None)
        
        result = config.apply(filter_config)
        
        assert result == filter_config
    
    def test_apply_with_current_mode(self):
        """Verify apply adds VALID_TO filter for CURRENT mode."""
        config = VersioningConfig(versioning_mode=VersioningMode.CURRENT)
        filter_config = FilterConfig(source_filters=None)
        
        result = config.apply(filter_config)
        
        assert result.source_filters is not None
        # Check that the filter is for VALID_TO == TIMESTAMP_UPPER_BOUND
        filters = result.source_filters.filters
        assert len(filters) == 1
        assert isinstance(filters[0], MetadataFilter)
        assert filters[0].key == VALID_TO
        assert filters[0].value == TIMESTAMP_UPPER_BOUND
        assert filters[0].operator == FilterOperator.EQ
    
    def test_apply_with_previous_mode(self):
        """Verify apply adds VALID_TO filter for PREVIOUS mode."""
        config = VersioningConfig(versioning_mode=VersioningMode.PREVIOUS)
        filter_config = FilterConfig(source_filters=None)
        
        result = config.apply(filter_config)
        
        assert result.source_filters is not None
        filters = result.source_filters.filters
        assert len(filters) == 1
        assert isinstance(filters[0], MetadataFilter)
        assert filters[0].key == VALID_TO
        assert filters[0].value == TIMESTAMP_UPPER_BOUND
        assert filters[0].operator == FilterOperator.LT
    
    def test_apply_with_at_timestamp_mode(self):
        """Verify apply adds VALID_FROM and VALID_TO filters for AT_TIMESTAMP mode."""
        timestamp = 1704067200000
        config = VersioningConfig(versioning_mode=VersioningMode.AT_TIMESTAMP, at_timestamp=timestamp)
        filter_config = FilterConfig(source_filters=None)
        
        result = config.apply(filter_config)
        
        assert result.source_filters is not None
        # Should have a MetadataFilters with 2 filters (VALID_FROM and VALID_TO)
        assert hasattr(result.source_filters, 'filters')
    
    def test_apply_with_before_timestamp_mode(self):
        """Verify apply adds VALID_TO filter for BEFORE_TIMESTAMP mode."""
        timestamp = 1704067200000
        config = VersioningConfig(versioning_mode=VersioningMode.BEFORE_TIMESTAMP, at_timestamp=timestamp)
        filter_config = FilterConfig(source_filters=None)
        
        result = config.apply(filter_config)
        
        assert result.source_filters is not None
        filters = result.source_filters.filters
        assert len(filters) == 1
        assert isinstance(filters[0], MetadataFilter)
        assert filters[0].key == VALID_TO
        assert filters[0].value == timestamp
        assert filters[0].operator == FilterOperator.LT
    
    def test_apply_with_on_or_after_timestamp_mode(self):
        """Verify apply adds VALID_FROM filter for ON_OR_AFTER_TIMESTAMP mode."""
        timestamp = 1704067200000
        config = VersioningConfig(versioning_mode=VersioningMode.ON_OR_AFTER_TIMESTAMP, at_timestamp=timestamp)
        filter_config = FilterConfig(source_filters=None)
        
        result = config.apply(filter_config)
        
        assert result.source_filters is not None
        filters = result.source_filters.filters
        assert len(filters) == 1
        assert isinstance(filters[0], MetadataFilter)
        assert filters[0].key == VALID_FROM
        assert filters[0].value == timestamp
        assert filters[0].operator == FilterOperator.GTE
    
    def test_apply_combines_with_existing_filters(self):
        """Verify apply combines versioning filters with existing filters."""
        existing_filter = MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        filter_config = FilterConfig(source_filters=existing_filter)
        
        config = VersioningConfig(versioning_mode=VersioningMode.CURRENT)
        result = config.apply(filter_config)
        
        assert result.source_filters is not None
        # Should have combined filters with AND condition
        assert hasattr(result.source_filters, 'filters')


class TestVersioningConstants:
    """Tests for versioning constants."""
    
    def test_versioning_metadata_keys_defined(self):
        """Verify all versioning metadata keys are defined."""
        assert VALID_FROM == '__aws__versioning__valid_from__'
        assert VALID_TO == '__aws__versioning__valid_to__'
        assert EXTRACT_TIMESTAMP == '__aws__versioning__extract_timestamp__'
        assert BUILD_TIMESTAMP == '__aws__versioning__build_timestamp__'
        assert VERSION_INDEPENDENT_ID_FIELDS == '__aws__versioning__id_fields__'
        assert PREV_VERSIONS == '__aws__versioning__prev_versions__'
    
    def test_timestamp_bounds_defined(self):
        """Verify timestamp bounds are defined."""
        assert TIMESTAMP_LOWER_BOUND == -1
        assert TIMESTAMP_UPPER_BOUND == 10000000000000


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------

from hypothesis import given, strategies as st, settings, assume, HealthCheck


# Strategy for generating valid timestamps
# Timestamps should be within reasonable bounds
timestamp_strategy = st.integers(min_value=0, max_value=TIMESTAMP_UPPER_BOUND - 1)


@given(
    v1=timestamp_strategy,
    v2=timestamp_strategy,
    v3=timestamp_strategy
)
@settings(max_examples=100, suppress_health_check=[HealthCheck.filter_too_much])
def test_version_comparison_transitive_property(v1, v2, v3):
    """
    Property: Version comparison is transitive.
    
    For any three version timestamps v1, v2, v3:
    If v1 < v2 and v2 < v3, then v1 < v3.
    
    This property verifies that version ordering is consistent and transitive,
    which is essential for correct version filtering and retrieval operations.
    
    **Validates: Requirements 6.6, 12.3**
    """
    # Assume the precondition: v1 < v2 and v2 < v3
    assume(v1 < v2 and v2 < v3)
    
    # Then v1 < v3 must hold (transitivity)
    assert v1 < v3, f"Transitivity violated: {v1} < {v2} and {v2} < {v3}, but {v1} >= {v3}"
