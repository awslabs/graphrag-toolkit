# Copyright (c) Evan Erwee. All rights reserved.

"""API to list available constructors."""

from document_graph.graph_build.constructors.constructors_provider_registry import ConstructorProviderRegistry

# Import registration module to ensure constructors are registered
from . import register_constructors

def list_available_constructors():
    """
    Lists all registered constructor providers available in the ConstructorProviderRegistry.

    Returns:
        list: A list containing all registered providers in the
              ConstructorProviderRegistry.
    """
    return ConstructorProviderRegistry.list_providers()

def list_core_constructors():
    """
    Filters and returns a list of core constructors from all available constructors.

    This function retrieves a list of all constructors from the
    ConstructorProviderRegistry and filters them to match a predefined
    list of core constructor names. The filtered list contains
    only the constructors that are considered core.

    Returns:
        list: A list of constructor names that match the predefined
        list of core constructors.
    """
    all_constructors = ConstructorProviderRegistry.list_providers()
    core = ["schema_driven", "node_constructor", "edge_constructor", "property_constructor"]
    return [const for const in all_constructors if const in core]

def list_pattern_constructors():
    """
    Lists all constructors that match specific patterns.

    This function retrieves all available constructor providers and filters
    them based on predefined patterns.

    Returns:
        list: A list of constructor providers that match the specified patterns.
    """
    all_constructors = ConstructorProviderRegistry.list_providers()
    patterns = ["one_to_many", "many_to_many"]
    return [const for const in all_constructors if const in patterns]

def list_optimization_constructors():
    """
    Filters and returns a list of constructors that match specified optimization features.

    This function retrieves all available constructors from the
    `ConstructorProviderRegistry`, compares them against a set of predefined
    optimization features, and returns only the ones that are part of the
    optimization list.

    Returns:
        list[str]: A list of constructors matching the specified optimization
        features.
    """
    all_constructors = ConstructorProviderRegistry.list_providers()
    optimizations = ["batch_constructor", "deduplication"]
    return [const for const in all_constructors if const in optimizations]

if __name__ == "__main__":
    print("Available Constructors:")
    print("Core:", list_core_constructors())
    print("Patterns:", list_pattern_constructors())
    print("Optimizations:", list_optimization_constructors())