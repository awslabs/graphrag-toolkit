# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import abc
from typing import Callable, Any, Dict, List, Optional, Union, overload
from dateutil.parser import parse
from datetime import datetime, date

from graphrag_toolkit.lexical_graph import GraphRAGConfig

from llama_index.core.vector_stores.types import FilterCondition, FilterOperator, MetadataFilter, MetadataFilters
from llama_index.core.bridge.pydantic import BaseModel

logger = logging.getLogger(__name__)

MetadataFiltersType = Union[MetadataFilters, MetadataFilter, List[MetadataFilter]]

def format_metadata_list(fields:List[str]) -> str:
    return ';'.join(fields)

def is_datetime_key(key):
    """Determines if the given key corresponds to a datetime metadata field.

    This function checks if the provided key ends with any of the predefined suffixes
    indicating that it represents a datetime metadata field. It utilizes the
    `GraphRAGConfig.metadata_datetime_suffixes` tuple to recognize potential matches.

    Args:
        key (str): The key to check for a datetime-related suffix.

    Returns:
        bool: True if the key ends with a predefined datetime suffix, False otherwise.
    """
    return key.endswith(tuple(GraphRAGConfig.metadata_datetime_suffixes))


def format_datetime(s: Any):
    """
    Formats a date or datetime object or parses a string into an ISO 8601 formatted string.

    This function takes a datetime or date object and formats it into an ISO 8601
    string. If provided with a string, it attempts to parse the string into a
    datetime object and then formats it as an ISO 8601 string. The function ensures
    strict parsing for string inputs.

    Args:
        s: A datetime object, date object, or a string representing a date or
           datetime.

    Returns:
        str: An ISO 8601 formatted string representation of the date or datetime.
    """
    if isinstance(s, datetime) or isinstance(s, date):
        return s.isoformat()
    else:
        return parse(s, fuzzy=False).isoformat()


def type_name_for_key_value(key: str, value: Any) -> str:
    """
    Determines the type name for a given key-value pair based on the value's type or certain key-specific logic.

    The function evaluates the type of the `value` associated with a given `key` and returns a string representing the
    type. For specific cases like datetime-related keys or datetime-like values, additional logic is applied. Unsupported
    value types such as lists, dictionaries, or sets will lead to an exception.

    Args:
        key (str): The key associated with the value, which may influence the type determination for certain cases.
        value (Any): The value whose type needs to be determined.

    Returns:
        str: The inferred type name for the given value. Possible values are 'int', 'float', 'timestamp', or 'text'.

    Raises:
        ValueError: If the `value` is of an unsupported type, including lists, dictionaries, or sets.
    """
    if isinstance(value, list) or isinstance(value, dict) or isinstance(value, set):
        raise ValueError(f'Unsupported value type: {type(value)}')

    if isinstance(value, int):
        return 'int'
    elif isinstance(value, float):
        return 'float'
    else:
        if isinstance(value, datetime) or isinstance(value, date):
            return 'timestamp'
        elif is_datetime_key(key):
            try:
                parse(value, fuzzy=False)
                return 'timestamp'
            except ValueError as e:
                return 'text'
        else:
            return 'text'


def formatter_for_type(type_name: str) -> Callable[[Any], str]:
    """
    Determines and returns a specific formatter function based on the input type name. This formatter function is used
    to convert or format an input value into the desired type or representation according to the type specified.

    If the type name is not recognized or supported, an exception will be raised.

    Args:
        type_name: A string specifying the type of formatter function to return.
            Supported type names include:
            - 'text': Returns the input value as-is.
            - 'timestamp': Returns the formatted value using `format_datetime`.
            - 'int': Converts the input to an integer.
            - 'float': Converts the input to a floating-point number.

    Returns:
        Callable[[Any], str]: A function that takes an input value and formats
        it according to the specified type name.

    Raises:
        ValueError: If the type_name is not supported.
    """
    if type_name == 'text':
        return lambda x: x
    elif type_name == 'timestamp':
        return lambda x: format_datetime(x)
    elif type_name == 'int':
        return lambda x: int(x)
    elif type_name == 'float':
        return lambda x: float(x)
    else:
        raise ValueError(f'Unsupported type name: {type_name}')


class SourceMetadataFormatter(BaseModel):
    """
    Abstract base class responsible for formatting metadata.

    This class provides a blueprint for implementing custom metadata formatters.
    Its purpose is to ensure the consistent transformation or formatting of
    metadata dictionaries into desired formats. Users of this class are required
    to implement the `format` method in a subclass, where the specific logic
    for formatting metadata is defined.

    Attributes:
        None
    """
    @abc.abstractmethod
    def format(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError()


class DefaultSourceMetadataFormatter(SourceMetadataFormatter):
    """
    Formats source metadata into a standardized dictionary format.

    This class implements a formatting utility for source metadata, transforming
    input metadata into a standardized form. It maps the metadata keys and values
    to their respective types and applies appropriate formatting functions. If a
    metadata value cannot be formatted, it retains its original value.
    """
    def format(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        formatted_metadata = {}
        for k, v in metadata.items():
            try:
                type_name = type_name_for_key_value(k, v)
                formatter = formatter_for_type(type_name)
                value = formatter(v)
                formatted_metadata[k] = value
            except ValueError as e:
                formatted_metadata[k] = v
        return formatted_metadata


class FilterConfig(BaseModel):
    """
    Configuration class for filter settings.

    This class handles the configuration of filters related to metadata and provides
    functionality to filter source metadata dictionaries. It supports various types
    of metadata filter inputs and ensures proper initialization and usage of filters.

    Attributes:
        source_filters (Optional[MetadataFilters]): A collection of filters for source
            metadata, initialized based on the provided input type.
        source_metadata_dictionary_filter_fn (Callable[[Dict[str, Any]], bool]): A function
            that determines whether a source metadata dictionary passes the filtering condition.
    """
    source_filters: Optional[MetadataFilters]
    source_metadata_dictionary_filter_fn: Callable[[Dict[str, Any]], bool]

    def __init__(self, source_filters: Optional[MetadataFiltersType] = None):
        """
        Initializes an instance of the class, allowing for the configuration of source filters.

        Args:
            source_filters (Optional[MetadataFiltersType]): Optional parameter to specify the source
                filters. Can be of type `MetadataFilters`, `MetadataFilter`, a list of filters, or `None`.

        Raises:
            ValueError: If `source_filters` is not of an acceptable type.
        """
        if not source_filters:
            source_filters = None
        elif isinstance(source_filters, MetadataFilters):
            source_filters = source_filters
        elif isinstance(source_filters, MetadataFilter):
            source_filters = MetadataFilters(filters=[source_filters])
        elif isinstance(source_filters, list):
            source_filters = MetadataFilters(filters=source_filters)
        else:
            raise ValueError(f'Invalid source filters type: {type(source_filters)}')

        super().__init__(
            source_filters=source_filters,
            source_metadata_dictionary_filter_fn=DictionaryFilter(source_filters) if source_filters else lambda x: True
        )

    def filter_source_metadata_dictionary(self, d: Dict[str, Any]) -> bool:
        """
        Filters the given metadata dictionary using a filter function.

        This method applies a filter function to the input dictionary and logs
        the result of the filter operation. The result is a boolean indicating
        whether the dictionary passed the filter condition.

        Args:
            d (Dict[str, Any]): The metadata dictionary to be filtered.

        Returns:
            bool: True if the dictionary passes the filter; otherwise, False.
        """
        result = self.source_metadata_dictionary_filter_fn(d)
        if not result:
            logger.debug(f'filter result: [{str(d)}: {result}]')
        return result


class DictionaryFilter(BaseModel):
    """
    Filters metadata dictionaries based on specified filter criteria.

    This class provides mechanisms to apply a series of hierarchical metadata
    filters through recursive logic. It utilizes different filter operators
    such as equality, inequality, text matching, and value checking across
    nested metadata structures. The class is intended to be called as a function
    to determine whether the supplied metadata satisfies the provided filters.

    Attributes:
        metadata_filters (MetadataFilters): The set of filters and conditions
            to be applied on the metadata.
    """
    metadata_filters: MetadataFilters

    def __init__(self, metadata_filters: MetadataFilters):
        """
        Initializes an instance with metadata filters to process or filter specific
        metadata as per the provided configuration.

        Args:
            metadata_filters: MetadataFilters object responsible for defining the
                filtering rules or conditions for the associated metadata.
        """
        super().__init__(metadata_filters=metadata_filters)

    def _apply_filter_operator(self, operator: FilterOperator, metadata_value: Any, value: Any) -> bool:
        """
        Applies a filter operator to a metadata value and a given value to evaluate a condition.

        This method takes a filter operator, a metadata value, and a comparison value,
        and applies the operator to determine if the condition holds true. It supports
        various operators such as equality, inequality, greater than, containment,
        text matching, and more. If the metadata value is `None` or the operator is unsupported,
        the method handles these cases appropriately by returning `False` or raising an exception,
        respectively.

        Args:
            operator (FilterOperator): The filter operator to evaluate the condition.
            metadata_value (Any): The metadata value to be compared against.
            value (Any): The value to compare with the metadata value.

        Returns:
            bool: The result of applying the filter operator to the metadata value and comparison value.

        Raises:
            ValueError: If the provided filter operator is unsupported.
        """
        if metadata_value is None:
            return False
        if operator == FilterOperator.EQ:
            return metadata_value == value
        if operator == FilterOperator.NE:
            return metadata_value != value
        if operator == FilterOperator.GT:
            return metadata_value > value
        if operator == FilterOperator.GTE:
            return metadata_value >= value
        if operator == FilterOperator.LT:
            return metadata_value < value
        if operator == FilterOperator.LTE:
            return metadata_value <= value
        if operator == FilterOperator.IN:
            return metadata_value in value
        if operator == FilterOperator.NIN:
            return metadata_value not in value
        if operator == FilterOperator.CONTAINS:
            return value in metadata_value
        if operator == FilterOperator.TEXT_MATCH:
            return value.lower() in metadata_value.lower()
        if operator == FilterOperator.ALL:
            return all(val in metadata_value for val in value)
        if operator == FilterOperator.ANY:
            return any(val in metadata_value for val in value)
        raise ValueError(f'Unsupported filter operator: {operator}')

    def _apply_metadata_filters_recursive(self, metadata_filters: MetadataFilters, metadata: Dict[str, Any]) -> bool:
        """
        Applies a set of metadata filters recursively to determine whether the metadata
        satisfies the specified filter conditions. This function evaluates each filter
        based on its operator and compares the metadata values to determine whether
        the condition (AND, OR, NOT) is satisfied. Custom formatting and type handling
        are applied to ensure proper evaluation of metadata values.

        Args:
            metadata_filters (MetadataFilters): A container of metadata filters and
                a filtering condition (AND, OR, or NOT) to evaluate.
            metadata (Dict[str, Any]): A dictionary of metadata to compare against the
                provided filters.

        Returns:
            bool: True if the metadata satisfies the specified filter conditions, or
                False otherwise.

        Raises:
            ValueError: If a metadata filter condition is specified incorrectly, or if
                the metadata filter type is invalid.
        """
        results: List[bool] = []

        def get_filter_result(f: MetadataFilter, metadata: Dict[str, Any]):
            """
            Represents a filter mechanism for applying metadata filters recursively
            on a dictionary-like metadata structure. This class primarily focuses on
            validating metadata against specified filtering criteria using recursive
            approaches and type-based transformations.

            Attributes:
                No attributes are defined explicitly for this class. It functions
                through its methods for processing metadata.

            """
            metadata_value = metadata.get(f.key, None)
            if f.operator == FilterOperator.IS_EMPTY:
                return (
                        metadata_value is None
                        or metadata_value == ''
                        or metadata_value == []
                )
            else:
                type_name = type_name_for_key_value(f.key, f.value)
                formatter = formatter_for_type(type_name)
                value = formatter(f.value)
                metadata_value = formatter(metadata_value)
                return self._apply_filter_operator(
                    operator=f.operator,
                    metadata_value=metadata_value,
                    value=value  
                )

        for metadata_filter in metadata_filters.filters:
            if isinstance(metadata_filter, MetadataFilter):
                if metadata_filters.condition == FilterCondition.NOT:
                    raise ValueError(f'Expected MetadataFilters for FilterCondition.NOT, but found MetadataFilter')
                results.append(get_filter_result(metadata_filter, metadata))
            elif isinstance(metadata_filter, MetadataFilters):
                results.append(self._apply_metadata_filters_recursive(metadata_filter, metadata))
            else:
                raise ValueError(f'Invalid metadata filter type: {type(metadata_filter)}')
            
        if metadata_filters.condition == FilterCondition.NOT:
            return not all(results)
        elif metadata_filters.condition == FilterCondition.AND:
            return all(results)
        elif metadata_filters.condition == FilterCondition.OR:
            return any(results)
        else:
            raise ValueError(f'Unsupported filters condition: {metadata_filters.condition}')

    def __call__(self, metadata: Dict[str, Any]) -> bool:
        """
        Executes the metadata filters on the provided metadata.

        The method applies the set of predefined metadata filters recursively
        to determine if the provided metadata satisfies all conditions.

        Args:
            metadata (Dict[str, Any]): The metadata to be filtered and evaluated against
                the predefined metadata filters.

        Returns:
            bool: Returns True if the metadata satisfies all the filter conditions;
            otherwise, returns False.
        """
        return self._apply_metadata_filters_recursive(self.metadata_filters, metadata)
    
FilterType = Union[FilterConfig, List[Dict], Dict]

@overload
def to_metadata_filter(filter:FilterConfig) -> FilterConfig:
    ...

@overload
def to_metadata_filter(filter:List[Dict]) -> FilterConfig:
    ...

@overload
def to_metadata_filter(filter:Dict) -> FilterConfig:
    ...

def to_metadata_filter(filter:FilterType) -> FilterConfig:
    
    if isinstance(filter, FilterConfig):
        return filter
    
    def to_metadata_filters(d):
        if isinstance(d, dict):
            return MetadataFilters(
                filters = [
                    MetadataFilter(
                        key=k, 
                        value=v, 
                        operator=FilterOperator.EQ
                    ) for k,v in d.items()
                ]
            )
        else:
            raise ValueError(f'Expected dictionary, but received {type(d).__name__}')
    
    if isinstance(filter, dict):
        return FilterConfig(
            source_filters = to_metadata_filters(filter)
        )
    
    if isinstance(filter, list):
        return FilterConfig(
            source_filters = MetadataFilters(
                filters = [
                    to_metadata_filters(d)
                    for d in filter
                ],
                condition = FilterCondition.OR
            )
        )
