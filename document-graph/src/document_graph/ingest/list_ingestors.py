"""API to list available ingestors."""

from .ingestors_provider_registry import IngestorProviderRegistry

# Import registration modules to ensure ingestors are registered
try:
    from .row_level.register_ingestors import *
except ImportError:
    # Handle hyphenated directory names
    import importlib
    row_module = importlib.import_module('.row_level.register_ingestors', package=__package__)
    
try:
    from .column_level.register_ingestors import *
except ImportError:
    # Handle hyphenated directory names
    import importlib
    col_module = importlib.import_module('.column_level.register_ingestors', package=__package__)

# Import field_level ingestors (no register_ingestors file needed, they're in registry)
# The numeric_id_cleanup is already registered in ingestors_provider_registry.py

def list_available_ingestors():
    """
    Lists all available ingestor providers.

    This function retrieves and returns a list of all registered ingestor
    providers from the IngestorProviderRegistry.

    Returns:
        list: A list of available ingestor providers.
    """
    return IngestorProviderRegistry.list_providers()

def list_row_level_ingestors():
    """
    Lists all row-level ingestors.

    This function retrieves the list of all ingestors available in the
    IngestorProviderRegistry and filters only those that operate at the
    row level. It checks against predefined row-level ingestors and
    returns a filtered list.

    Returns:
        list[str]: A list of names of ingestors that process data at
        the row level.
    """
    all_ingestors = IngestorProviderRegistry.list_providers()
    row_level = ["skip_row", "date_range_filter"]
    return [ing for ing in all_ingestors if ing in row_level]

def list_column_level_ingestors():
    """
    Filters and retrieves column-level ingestors from a list of all available ingestors.

    This function identifies specific ingestors operating at the column level, such as
    selectors, renamers, reorders, and type converters, from the general list of
    available ingestor providers.

    Returns
    -------
    list[str]
        A list of ingestor names corresponding to column-level operations.
    """
    all_ingestors = IngestorProviderRegistry.list_providers()
    column_level = ["column_selector", "column_renamer", "column_reorder", "column_type_converter"]
    return [ing for ing in all_ingestors if ing in column_level]

def list_field_level_ingestors():
    """
    Lists all field-level ingestors available in the IngestorProviderRegistry.

    This function retrieves a list of all ingestors registered in the
    IngestorProviderRegistry and filters them to return only those that
    operate at the field level. Field-level ingestors are predefined
    and identified within the function.

    Returns:
        list: A list of ingestor names that operate at the field level.
    """
    all_ingestors = IngestorProviderRegistry.list_providers()
    field_level = ["numeric_id_cleanup"]
    return [ing for ing in all_ingestors if ing in field_level]

if __name__ == "__main__":
    print("Available Ingestors:")
    print("Row-level:", list_row_level_ingestors())
    print("Column-level:", list_column_level_ingestors())
    print("Field-level:", list_field_level_ingestors())