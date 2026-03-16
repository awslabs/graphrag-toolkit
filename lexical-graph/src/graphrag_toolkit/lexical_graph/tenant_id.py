# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from typing import Optional, Union
from llama_index.core.bridge.pydantic import BaseModel

DEFAULT_TENANT_NAME = 'default_'

class TenantId(BaseModel):
    """
    Represents a TenantId with validation logic, supporting default and custom tenant formats.

    This class provides functionality to validate and handle tenant identifiers,
    with optional formatting methods that adapt the given label, index name,
    hashable string, or ID based on whether the tenant is default or custom.
    Tenant IDs are validated to ensure they consist of lowercase letters,
    numbers, and periods (not at start/end), and are between 1 and 25
    characters in length.

    Attributes:
        value (Optional[str]): The tenant identifier. None indicates the default tenant.
    """
    value: Optional[str] = None

    def __init__(self, value: str = None):
        """ Initializes an instance of the class with a specified value.

        Validates the input value to ensure it meets specific criteria. The value must be
        a string containing between 1 and 25 characters and restricted to lowercase letters,
        numbers, and periods (but not at the start or end). If the value is invalid, a ValueError is raised.

        Args:
            value (str): Optional. A string value for initialization. The string must be
                between 1 and 25 characters, alphanumeric with optional periods (not at start/end),
                entirely in lowercase, and must not contain uppercase letters. Defaults to None.
        """
        if value is not None:
            if value.lower() == DEFAULT_TENANT_NAME:
                value = None
            elif not self._is_valid_tenant_id(value):
                raise ValueError(
                    f"Invalid TenantId: '{value}'. TenantId must be between 1-25 lowercase letters, numbers, and periods (not at start or end).")
        super().__init__(value=value)

    def _is_valid_tenant_id(self, value: str) -> bool:
        """ Validates a tenant ID format with backwards compatibility."""
        if (
            not value
            or len(value) > 25
            or any(letter.isupper() for letter in value)
        ):
            return False
        if value.startswith('.') or value.endswith('.'):
            return False
        return all(c.isalnum() or c == '.' for c in value)

    def __str__(self):
        return self.value if self.value else DEFAULT_TENANT_NAME

    def is_default_tenant(self):
        """
        Determines if the tenant is the default tenant.

        This method checks whether the `value` attribute of the instance is set to
        `None`, representing that it is the default tenant.

        Returns:
            bool: True if the tenant is marked as the default tenant (i.e., `value`
            is `None`), False otherwise.
        """
        return self.value is None

    def format_label(self, label: str):
        """
        Formats the given label by appending the instance's value with a double underscore
        when the tenant is not the default tenant. If the tenant is the default tenant,
        only the label is formatted.

        Args:
            label (str): The label to be formatted.

        Returns:
            str: The formatted label.
        """
        if self.is_default_tenant():
            return f'`{label}`'
        return f'`{label}{self.value}__`'

    def format_index_name(self, index_name: str):
        """
        Formats the provided index name by appending the tenant-specific identifier to it,
        unless the current tenant is the default tenant. This ensures that indexes are
        uniquely identifiable across different tenants.

        Args:
            index_name: The base name of the index to be formatted.

        Returns:
            str: The formatted index name with a tenant-specific suffix, if applicable.
        """
        if self.is_default_tenant():
            return index_name
        return f'{index_name}_{self.value}'

    def format_hashable(self, hashable: str):
        """
        Formats a given hashable string based on the tenant value.

        If the tenant is the default tenant, it simply returns the hashable string as is.
        Otherwise, it prefixes the hashable string with the tenant's value followed by
        '::'.

        Args:
            hashable (str): The hashable string to be formatted.

        Returns:
            str: The formatted hashable string.
        """
        if self.is_default_tenant():
            return hashable
        else:
            return f'{self.value}::{hashable}'

    def format_id(self, prefix: str, id_value: str):
        """
        Formats an identifier string by appending appropriate prefixes and delimiters
        based on tenant configuration.

        If the tenant is the default tenant, the identifier is formatted with a double
        colon delimiter. Otherwise, it is formatted with a single colon delimiter and
        includes the tenant's value.

        Args:
            prefix: A prefix for the identifier, used to categorize or differentiate
                the identifier.
            id_value: The main identifier value that will be formatted with the prefix
                and delimiters.

        Returns:
            str: The formatted identifier string based on tenant configuration.

        Raises:
            None
        """
        if self.is_default_tenant():
            return f'{prefix}::{id_value}'
        else:
            return f'{prefix}:{self.value}:{id_value}'

    def rewrite_id(self, id_value: str):
        """
        Rewrites the provided ID by appending the current tenant's value if the
        current tenant is not the default tenant. This ensures that IDs respect
        the tenant's namespace.

        Args:
            id_value (str): The original ID to be rewritten.

        Returns:
            str: The rewritten ID with the tenant's value included, or the
            original ID if the tenant is the default.
        """
        if self.is_default_tenant():
            return id_value
        else:
            id_parts = id_value.split(':')
            return f'{id_parts[0]}:{self.value}:{":".join(id_parts[2:])}'


DEFAULT_TENANT_ID = TenantId()

TenantIdType = Union[str, TenantId]


def to_tenant_id(tenant_id: Optional[TenantIdType]):
    """
    Converts the provided tenant identifier into a `TenantId` instance. If the input is None, it defaults to
    the global `DEFAULT_TENANT_ID`. If the input is already of type `TenantId`, it is returned as-is. Otherwise,
    the input is converted into a `TenantId` object using its string representation.

    Args:
        tenant_id: Optional tenant identifier. Can be None, an instance of `TenantId`, or a convertible type
            that can be passed as a string to the `TenantId` constructor.

    Returns:
        A `TenantId` instance corresponding to the input or `DEFAULT_TENANT_ID` if input is None.
    """
    if tenant_id is None:
        return DEFAULT_TENANT_ID
    if isinstance(tenant_id, TenantId):
        return tenant_id
    else:
        return TenantId(str(tenant_id))
    
