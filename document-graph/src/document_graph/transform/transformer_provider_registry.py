# Copyright (c) Evan Erwee. All rights reserved.

"""Transformer Provider Registry module for document graph operations.

This module provides a registry system for transformer providers in the document
graph processing system. It maintains a mapping of transformer type names to their
concrete implementations, allowing for dynamic discovery and instantiation of
transformer classes. The registry supports registration, lookup, and introspection
of transformer providers, and is used by the TransformerProviderFactory and plugin
system to locate and create transformer instances.
"""

import logging
from typing import Type, Dict
from document_graph.transform.transformer_provider_base import TransformerProvider

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

class TransformerProviderRegistry:
    """Registry for transformers providers.
    
    Maintains a mapping of transformers type names to their concrete implementations.
    Used by the TransformerProviderFactory and plugin system to discover and
    instantiate transformer providers dynamically.
    
    Examples:
        >>> # Register a custom transformer provider
        >>> class MyTransformer(TransformerProvider):
        ...     def transform(self, data):
        ...         return data
        >>> 
        >>> # Register the transformer
        >>> TransformerProviderRegistry.register("my_transformer", MyTransformer)
        >>> 
        >>> # Get the transformer class
        >>> transformer_cls = TransformerProviderRegistry.get("my_transformer")
        >>> transformer_cls is MyTransformer
        True
    """
    
    _registry: Dict[str, Type[TransformerProvider]] = {}
    _lazy_registry: Dict[str, callable] = {}
    
    @classmethod
    def register(cls, type_: str, provider_cls: Type[TransformerProvider]) -> None:
        """Register a new transformers provider type.
        
        Args:
            type_: Provider type name
            provider_cls: Provider class to register
        """
        logger.debug(f"Registering transformers provider: {type_} -> {provider_cls.__name__}")
        cls._registry[type_.lower()] = provider_cls
    
    @classmethod
    def register_lazy(cls, type_: str, loader_func: callable) -> None:
        """Register a lazy-loaded transformer provider.
        
        Args:
            type_: Provider type name
            loader_func: Function that returns the provider class when called
        """
        logger.debug(f"Registering lazy transformers provider: {type_}")
        cls._lazy_registry[type_.lower()] = loader_func
    
    @classmethod
    def get(cls, type_: str) -> Type[TransformerProvider]:
        """Get a transformers provider class by type.
        
        Args:
            type_: Provider type name
            
        Returns:
            Type[TransformerProvider]: Provider class
            
        Raises:
            KeyError: If provider type is not registered
            
        Examples:
            >>> # Get a registered transformer provider
            >>> normalizer_cls = TransformerProviderRegistry.get("text_normalizer")
            >>> normalizer = normalizer_cls(config)
            >>> normalizer.transform("TEXT TO NORMALIZE")
            'text to normalize'
            >>> 
            >>> # Trying to get an unregistered provider
            >>> try:
            ...     TransformerProviderRegistry.get("nonexistent_provider")
            ... except KeyError as e:
            ...     print(f"Error: {e}")
            Error: 'No transformers provider registered for type='nonexistent_provider''
        """
        key = type_.lower()
        logger.debug(f"Looking up transformers provider: {type_}")
        
        # Check eager registry first
        if key in cls._registry:
            return cls._registry[key]
        
        # Check lazy registry
        if key in cls._lazy_registry:
            try:
                provider_cls = cls._lazy_registry[key]()
                # Cache the loaded class
                cls._registry[key] = provider_cls
                return provider_cls
            except Exception as e:
                logger.error(f"Failed to load lazy transformer {type_}: {e}")
                raise KeyError(f"Failed to load transformer '{type_}': {e}")
        
        # Provide helpful suggestions
        available = cls.list_providers()
        suggestions = [name for name in available if type_.lower() in name or name in type_.lower()]
        
        error_msg = f"No transformer '{type_}' found."
        if suggestions:
            error_msg += f" Did you mean: {', '.join(suggestions[:3])}?"
        error_msg += f" Available: {', '.join(sorted(available))}"
        
        logger.error(error_msg)
        raise KeyError(error_msg)
    
    @classmethod
    def list_providers(cls) -> list[str]:
        """List all registered provider types.
        
        Returns:
            list[str]: List of registered provider type names
            
        Examples:
            >>> # Register some providers
            >>> TransformerProviderRegistry.register("text_normalizer", TextNormalizer)
            >>> TransformerProviderRegistry.register("entity_enricher", EntityEnricher)
            >>> 
            >>> # List all registered providers
            >>> providers = TransformerProviderRegistry.list_providers()
            >>> sorted(providers)
            ['entity_enricher', 'text_normalizer']
        """
        return list(set(list(cls._registry.keys()) + list(cls._lazy_registry.keys())))
    
    @classmethod
    def describe_providers(cls) -> dict[str, dict[str, str]]:
        """Get detailed information about all registered providers.
        
        Returns:
            dict: Provider info with name, description, and category
            
        Examples:
            >>> # Get detailed information about registered providers
            >>> info = TransformerProviderRegistry.describe_providers()
            >>> 'text_normalizer' in info
            True
            >>> info['text_normalizer']['category']
            'normalizer'
            >>> info['text_normalizer']['description']
            'Normalizes text input'
        """
        info = {}
        
        # Process eager registry
        for name, provider_cls in cls._registry.items():
            info[name] = cls._get_provider_info(name, provider_cls)
        
        # Process lazy registry - create basic info without loading
        for name in cls._lazy_registry.keys():
            if name not in info:  # Don't override if already loaded
                # Create basic info based on name patterns without loading
                category = 'transformer'
                type_name = 'transformer'
                
                if 'normalize' in name:
                    category = 'normalizer'
                    type_name = 'normalizer'
                elif 'enricher' in name:
                    category = 'enricher'
                    type_name = 'enricher'
                elif name in ['column_pruner', 'row_filter']:
                    category = 'filter'
                    type_name = 'filter_transformer'
                elif name in ['infer_edges', 'row_to_node']:
                    category = 'graph'
                    type_name = 'graph_transformer'
                elif name in ['pii_redactor', 'json_to_rows', 'text_chunker']:
                    category = 'document'
                    type_name = 'document_transformer'
                elif name in ['regex_cleaner', 'embedded_json', 'standardize_enum', 'comma_split', 'json_array_expander', 'json_flattener', 'comma_flattener']:
                    category = 'field'
                    type_name = 'field_transformer'
                
                info[name] = {
                    'name': name,
                    'description': f'{category.title()} transformer for {name.replace("_", " ")}',
                    'category': category,
                    'type': type_name,
                    'class': 'LazyLoaded'
                }
        
        return info
    
    @classmethod
    def _get_provider_info(cls, name: str, provider_cls) -> dict[str, str]:
        """Extract provider information from a provider class."""
        # Extract description from docstring
        doc = provider_cls.__doc__ or "No description available"
        description = doc.split('\n')[0].strip() if doc else "No description"
        
        # Determine category and type from class name and module patterns
        class_name = provider_cls.__name__.lower()
        module_name = provider_cls.__module__.lower()
        
        if 'normalize' in class_name or 'normalizers' in module_name:
            category = 'normalizer'
            type_name = 'normalizer'
        elif 'enrich' in class_name or 'enrichers' in module_name:
            category = 'enricher'
            type_name = 'enricher'
        elif 'filter' in class_name or 'prune' in class_name or 'filter_transformers' in module_name:
            category = 'filter'
            type_name = 'filter_transformer'
        elif 'graph' in class_name or 'node' in class_name or 'edge' in class_name or 'graph_transformers' in module_name:
            category = 'graph'
            type_name = 'graph_transformer'
        elif 'document_transformers' in module_name:
            category = 'document'
            type_name = 'document_transformer'
        elif 'field_transformers' in module_name:
            category = 'field'
            type_name = 'field_transformer'
        elif 'truncators' in module_name:
            category = 'truncator'
            type_name = 'truncator'
        else:
            category = 'transformer'
            type_name = 'transformer'
        
        return {
            'name': name,
            'description': description,
            'category': category,
            'type': type_name,
            'class': provider_cls.__name__
        }


# Initialize registry instance
transformer_provider_registry = TransformerProviderRegistry()