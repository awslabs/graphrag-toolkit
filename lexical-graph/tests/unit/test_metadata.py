# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for metadata.py module.

This module tests metadata creation, serialization, and validation.
"""

import pytest
import json
from datetime import datetime, date
from graphrag_toolkit.lexical_graph.metadata import (
    format_metadata_list,
    is_datetime_key,
    format_datetime,
    type_name_for_key_value,
    formatter_for_type,
    DefaultSourceMetadataFormatter,
    FilterConfig,
    DictionaryFilter,
    to_metadata_filter
)
from llama_index.core.vector_stores.types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters
)


class TestFormatMetadataList:
    """Tests for format_metadata_list function."""
    
    def test_format_metadata_list_single_field(self):
        """Verify format_metadata_list formats single field."""
        result = format_metadata_list(['field1'])
        assert result == 'field1'
    
    def test_format_metadata_list_multiple_fields(self):
        """Verify format_metadata_list joins multiple fields with semicolon."""
        result = format_metadata_list(['field1', 'field2', 'field3'])
        assert result == 'field1;field2;field3'
    
    def test_format_metadata_list_empty(self):
        """Verify format_metadata_list handles empty list."""
        result = format_metadata_list([])
        assert result == ''


class TestIsDatetimeKey:
    """Tests for is_datetime_key function."""
    
    def test_is_datetime_key_with_date_suffix(self):
        """Verify is_datetime_key returns True for keys ending with _date."""
        assert is_datetime_key('created_date') is True
        assert is_datetime_key('modified_date') is True
    
    def test_is_datetime_key_with_datetime_suffix(self):
        """Verify is_datetime_key returns True for keys ending with _datetime."""
        assert is_datetime_key('created_datetime') is True
        assert is_datetime_key('updated_datetime') is True
    
    def test_is_datetime_key_without_suffix(self):
        """Verify is_datetime_key returns False for keys without datetime suffix."""
        assert is_datetime_key('name') is False
        assert is_datetime_key('value') is False
        assert is_datetime_key('date') is False  # Must end with suffix, not be the suffix


class TestFormatDatetime:
    """Tests for format_datetime function."""
    
    def test_format_datetime_with_datetime_object(self):
        """Verify format_datetime formats datetime object to ISO 8601."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        result = format_datetime(dt)
        assert result == '2024-01-15T10:30:45'
    
    def test_format_datetime_with_date_object(self):
        """Verify format_datetime formats date object to ISO 8601."""
        d = date(2024, 1, 15)
        result = format_datetime(d)
        assert result == '2024-01-15'
    
    def test_format_datetime_with_string(self):
        """Verify format_datetime parses and formats date string."""
        result = format_datetime('2024-01-15')
        assert '2024-01-15' in result
    
    def test_format_datetime_with_iso_string(self):
        """Verify format_datetime handles ISO 8601 string."""
        result = format_datetime('2024-01-15T10:30:45')
        assert result == '2024-01-15T10:30:45'


class TestTypeNameForKeyValue:
    """Tests for type_name_for_key_value function."""
    
    def test_type_name_for_int(self):
        """Verify type_name_for_key_value returns 'int' for integer values."""
        assert type_name_for_key_value('count', 42) == 'int'
    
    def test_type_name_for_float(self):
        """Verify type_name_for_key_value returns 'float' for float values."""
        assert type_name_for_key_value('score', 3.14) == 'float'
    
    def test_type_name_for_text(self):
        """Verify type_name_for_key_value returns 'text' for string values."""
        assert type_name_for_key_value('name', 'test') == 'text'
    
    def test_type_name_for_datetime_object(self):
        """Verify type_name_for_key_value returns 'timestamp' for datetime."""
        dt = datetime(2024, 1, 15)
        assert type_name_for_key_value('created', dt) == 'timestamp'
    
    def test_type_name_for_date_object(self):
        """Verify type_name_for_key_value returns 'timestamp' for date."""
        d = date(2024, 1, 15)
        assert type_name_for_key_value('created', d) == 'timestamp'
    
    def test_type_name_for_datetime_key_with_valid_string(self):
        """Verify type_name_for_key_value returns 'timestamp' for datetime key with parseable string."""
        assert type_name_for_key_value('created_date', '2024-01-15') == 'timestamp'
    
    def test_type_name_for_datetime_key_with_invalid_string(self):
        """Verify type_name_for_key_value returns 'text' for datetime key with unparseable string."""
        assert type_name_for_key_value('created_date', 'not a date') == 'text'
    
    def test_type_name_for_unsupported_list_raises_error(self):
        """Verify type_name_for_key_value raises ValueError for list values."""
        with pytest.raises(ValueError, match="Unsupported value type"):
            type_name_for_key_value('items', [1, 2, 3])
    
    def test_type_name_for_unsupported_dict_raises_error(self):
        """Verify type_name_for_key_value raises ValueError for dict values."""
        with pytest.raises(ValueError, match="Unsupported value type"):
            type_name_for_key_value('data', {'key': 'value'})


class TestFormatterForType:
    """Tests for formatter_for_type function."""
    
    def test_formatter_for_text(self):
        """Verify formatter_for_type returns identity function for 'text'."""
        formatter = formatter_for_type('text')
        assert formatter('hello') == 'hello'
    
    def test_formatter_for_int(self):
        """Verify formatter_for_type returns int converter for 'int'."""
        formatter = formatter_for_type('int')
        assert formatter('42') == 42
        assert formatter(42.7) == 42
    
    def test_formatter_for_float(self):
        """Verify formatter_for_type returns float converter for 'float'."""
        formatter = formatter_for_type('float')
        assert formatter('3.14') == 3.14
        assert formatter(3) == 3.0
    
    def test_formatter_for_timestamp(self):
        """Verify formatter_for_type returns datetime formatter for 'timestamp'."""
        formatter = formatter_for_type('timestamp')
        dt = datetime(2024, 1, 15, 10, 30)
        result = formatter(dt)
        assert '2024-01-15' in result
    
    def test_formatter_for_unsupported_type_raises_error(self):
        """Verify formatter_for_type raises ValueError for unsupported type."""
        with pytest.raises(ValueError, match="Unsupported type name"):
            formatter_for_type('unknown_type')


class TestDefaultSourceMetadataFormatter:
    """Tests for DefaultSourceMetadataFormatter class."""
    
    def test_format_with_simple_metadata(self):
        """Verify DefaultSourceMetadataFormatter formats simple metadata."""
        formatter = DefaultSourceMetadataFormatter()
        metadata = {'name': 'test', 'count': 42, 'score': 3.14}
        result = formatter.format(metadata)
        
        assert result['name'] == 'test'
        assert result['count'] == 42
        assert result['score'] == 3.14
    
    def test_format_with_datetime_metadata(self):
        """Verify DefaultSourceMetadataFormatter formats datetime metadata."""
        formatter = DefaultSourceMetadataFormatter()
        dt = datetime(2024, 1, 15, 10, 30)
        metadata = {'created_date': dt}
        result = formatter.format(metadata)
        
        assert '2024-01-15' in result['created_date']
    
    def test_format_preserves_invalid_values(self):
        """Verify DefaultSourceMetadataFormatter preserves values that can't be formatted."""
        formatter = DefaultSourceMetadataFormatter()
        # Lists should be preserved as-is since they raise ValueError
        metadata = {'items': [1, 2, 3]}
        result = formatter.format(metadata)
        
        assert result['items'] == [1, 2, 3]


class TestFilterConfig:
    """Tests for FilterConfig class."""
    
    def test_initialization_with_none(self):
        """Verify FilterConfig initializes with None filters."""
        config = FilterConfig(source_filters=None)
        assert config.source_filters is None
    
    def test_initialization_with_metadata_filter(self):
        """Verify FilterConfig initializes with single MetadataFilter."""
        filter = MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        config = FilterConfig(source_filters=filter)
        
        assert isinstance(config.source_filters, MetadataFilters)
        assert len(config.source_filters.filters) == 1
    
    def test_initialization_with_metadata_filters(self):
        """Verify FilterConfig initializes with MetadataFilters."""
        filters = MetadataFilters(filters=[
            MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        ])
        config = FilterConfig(source_filters=filters)
        
        assert config.source_filters == filters
    
    def test_initialization_with_list_of_filters(self):
        """Verify FilterConfig initializes with list of filters."""
        filters = [
            MetadataFilter(key='status', value='active', operator=FilterOperator.EQ),
            MetadataFilter(key='count', value=10, operator=FilterOperator.GT)
        ]
        config = FilterConfig(source_filters=filters)
        
        assert isinstance(config.source_filters, MetadataFilters)
        assert len(config.source_filters.filters) == 2
    
    def test_filter_source_metadata_dictionary_passes(self):
        """Verify filter_source_metadata_dictionary returns True for matching metadata."""
        filter = MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        config = FilterConfig(source_filters=filter)
        
        metadata = {'status': 'active', 'name': 'test'}
        assert config.filter_source_metadata_dictionary(metadata) is True
    
    def test_filter_source_metadata_dictionary_fails(self):
        """Verify filter_source_metadata_dictionary returns False for non-matching metadata."""
        filter = MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        config = FilterConfig(source_filters=filter)
        
        metadata = {'status': 'inactive', 'name': 'test'}
        assert config.filter_source_metadata_dictionary(metadata) is False


class TestDictionaryFilter:
    """Tests for DictionaryFilter class."""
    
    def test_filter_with_eq_operator(self):
        """Verify DictionaryFilter applies EQ operator correctly."""
        filters = MetadataFilters(filters=[
            MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        ])
        dict_filter = DictionaryFilter(metadata_filters=filters)
        
        assert dict_filter({'status': 'active'}) is True
        assert dict_filter({'status': 'inactive'}) is False
    
    def test_filter_with_ne_operator(self):
        """Verify DictionaryFilter applies NE operator correctly."""
        filters = MetadataFilters(filters=[
            MetadataFilter(key='status', value='inactive', operator=FilterOperator.NE)
        ])
        dict_filter = DictionaryFilter(metadata_filters=filters)
        
        assert dict_filter({'status': 'active'}) is True
        assert dict_filter({'status': 'inactive'}) is False
    
    def test_filter_with_gt_operator(self):
        """Verify DictionaryFilter applies GT operator correctly."""
        filters = MetadataFilters(filters=[
            MetadataFilter(key='count', value=10, operator=FilterOperator.GT)
        ])
        dict_filter = DictionaryFilter(metadata_filters=filters)
        
        assert dict_filter({'count': 15}) is True
        assert dict_filter({'count': 5}) is False
    
    def test_filter_with_and_condition(self):
        """Verify DictionaryFilter applies AND condition correctly."""
        filters = MetadataFilters(
            filters=[
                MetadataFilter(key='status', value='active', operator=FilterOperator.EQ),
                MetadataFilter(key='count', value=10, operator=FilterOperator.GT)
            ],
            condition=FilterCondition.AND
        )
        dict_filter = DictionaryFilter(metadata_filters=filters)
        
        assert dict_filter({'status': 'active', 'count': 15}) is True
        assert dict_filter({'status': 'active', 'count': 5}) is False
        assert dict_filter({'status': 'inactive', 'count': 15}) is False
    
    def test_filter_with_or_condition(self):
        """Verify DictionaryFilter applies OR condition correctly."""
        filters = MetadataFilters(
            filters=[
                MetadataFilter(key='status', value='active', operator=FilterOperator.EQ),
                MetadataFilter(key='count', value=10, operator=FilterOperator.GT)
            ],
            condition=FilterCondition.OR
        )
        dict_filter = DictionaryFilter(metadata_filters=filters)
        
        assert dict_filter({'status': 'active', 'count': 5}) is True
        assert dict_filter({'status': 'inactive', 'count': 15}) is True
        assert dict_filter({'status': 'inactive', 'count': 5}) is False
    
    def test_filter_with_missing_key(self):
        """Verify DictionaryFilter returns False for missing metadata keys."""
        filters = MetadataFilters(filters=[
            MetadataFilter(key='status', value='active', operator=FilterOperator.EQ)
        ])
        dict_filter = DictionaryFilter(metadata_filters=filters)
        
        assert dict_filter({'name': 'test'}) is False


class TestToMetadataFilter:
    """Tests for to_metadata_filter function."""
    
    def test_to_metadata_filter_with_filter_config(self):
        """Verify to_metadata_filter returns FilterConfig as-is."""
        config = FilterConfig(source_filters=None)
        result = to_metadata_filter(config)
        
        assert result is config
    
    def test_to_metadata_filter_with_dict(self):
        """Verify to_metadata_filter converts dict to FilterConfig."""
        filter_dict = {'status': 'active', 'count': 10}
        result = to_metadata_filter(filter_dict)
        
        assert isinstance(result, FilterConfig)
        assert isinstance(result.source_filters, MetadataFilters)
    
    def test_to_metadata_filter_with_list_of_dicts(self):
        """Verify to_metadata_filter converts list of dicts to FilterConfig with OR condition."""
        filter_list = [
            {'status': 'active'},
            {'status': 'pending'}
        ]
        result = to_metadata_filter(filter_list)
        
        assert isinstance(result, FilterConfig)
        assert isinstance(result.source_filters, MetadataFilters)
        assert result.source_filters.condition == FilterCondition.OR


# Property-based tests

from hypothesis import given, strategies as st, settings


# Define metadata strategy for property-based testing
metadata_strategy = st.dictionaries(
    keys=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_characters=['\x00'])),
    values=st.one_of(
        st.text(alphabet=st.characters(blacklist_characters=['\x00'])),
        st.integers(),
        st.floats(allow_nan=False, allow_infinity=False),
        st.booleans()
    ),
    min_size=0,
    max_size=20
)


@given(data=metadata_strategy)
@settings(max_examples=100)
def test_metadata_json_roundtrip_property(data):
    """
    Property: JSON serialization is reversible.
    
    For any metadata dictionary, converting to JSON and back should
    produce equivalent metadata.
    
    **Validates: Requirements 6.5, 12.5**
    """
    import json
    
    # Format the metadata using DefaultSourceMetadataFormatter
    formatter = DefaultSourceMetadataFormatter()
    formatted_metadata = formatter.format(data)
    
    # Serialize to JSON
    json_str = json.dumps(formatted_metadata)
    
    # Deserialize from JSON
    deserialized = json.loads(json_str)
    
    # Verify roundtrip preserves data
    # Re-format the deserialized data to ensure consistent formatting
    reformatted = formatter.format(deserialized)
    
    assert reformatted == formatted_metadata


@given(
    data=metadata_strategy,
    key=st.text(min_size=1, max_size=50, alphabet=st.characters(blacklist_characters=['\x00'])),
    value=st.one_of(st.text(alphabet=st.characters(blacklist_characters=['\x00'])), st.integers(), st.floats(allow_nan=False, allow_infinity=False))
)
@settings(max_examples=100)
def test_metadata_set_get_property(data, key, value):
    """
    Property: Set then get returns same value.
    
    For any metadata dictionary, key, and value, setting a key-value pair
    and then getting it should return the same value after formatting.
    
    **Validates: Requirements 6.5**
    """
    # Create a metadata dictionary and set a value
    metadata = dict(data)
    metadata[key] = value
    
    # Format the metadata to ensure consistent type handling
    formatter = DefaultSourceMetadataFormatter()
    formatted_metadata = formatter.format(metadata)
    
    # Retrieve the value
    retrieved = formatted_metadata.get(key)
    
    # Format the original value for comparison
    try:
        type_name = type_name_for_key_value(key, value)
        value_formatter = formatter_for_type(type_name)
        expected_value = value_formatter(value)
    except ValueError:
        # If formatting fails, expect the original value
        expected_value = value
    
    # Verify set then get returns same value
    assert retrieved == expected_value
