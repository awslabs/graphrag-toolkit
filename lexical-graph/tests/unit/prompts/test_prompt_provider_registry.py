# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for PromptProviderRegistry.

This module tests the global registry for managing and retrieving
named PromptProvider instances.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_registry import PromptProviderRegistry
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider


class TestPromptProviderRegistryRegister:
    """Tests for register class method."""
    
    def setup_method(self):
        """Clear registry before each test."""
        PromptProviderRegistry._registry = {}
        PromptProviderRegistry._default_provider_name = None
    
    def test_register_provider_with_name(self):
        """Verify register stores provider with given name."""
        mock_provider = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("test-provider", mock_provider)
        
        assert "test-provider" in PromptProviderRegistry._registry
        assert PromptProviderRegistry._registry["test-provider"] == mock_provider
    
    def test_register_sets_first_provider_as_default(self):
        """Verify first registered provider becomes default."""
        mock_provider = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("first-provider", mock_provider)
        
        assert PromptProviderRegistry._default_provider_name == "first-provider"
    
    def test_register_with_default_flag_sets_as_default(self):
        """Verify register with default=True sets provider as default."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        PromptProviderRegistry.register("provider2", mock_provider2, default=True)
        
        assert PromptProviderRegistry._default_provider_name == "provider2"
    
    def test_register_multiple_providers(self):
        """Verify multiple providers can be registered."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        mock_provider3 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        PromptProviderRegistry.register("provider2", mock_provider2)
        PromptProviderRegistry.register("provider3", mock_provider3)
        
        assert len(PromptProviderRegistry._registry) == 3
        assert "provider1" in PromptProviderRegistry._registry
        assert "provider2" in PromptProviderRegistry._registry
        assert "provider3" in PromptProviderRegistry._registry
    
    def test_register_overwrites_existing_provider(self):
        """Verify registering same name overwrites existing provider."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("test", mock_provider1)
        PromptProviderRegistry.register("test", mock_provider2)
        
        assert PromptProviderRegistry._registry["test"] == mock_provider2
    
    def test_register_without_default_flag_keeps_existing_default(self):
        """Verify register without default flag keeps existing default."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        PromptProviderRegistry.register("provider2", mock_provider2, default=False)
        
        assert PromptProviderRegistry._default_provider_name == "provider1"


class TestPromptProviderRegistryGet:
    """Tests for get class method."""
    
    def setup_method(self):
        """Clear registry before each test."""
        PromptProviderRegistry._registry = {}
        PromptProviderRegistry._default_provider_name = None
    
    def test_get_returns_provider_by_name(self):
        """Verify get returns provider for given name."""
        mock_provider = Mock(spec=PromptProvider)
        PromptProviderRegistry.register("test-provider", mock_provider)
        
        result = PromptProviderRegistry.get("test-provider")
        
        assert result == mock_provider
    
    def test_get_returns_none_for_nonexistent_name(self):
        """Verify get returns None for nonexistent provider name."""
        result = PromptProviderRegistry.get("nonexistent")
        
        assert result is None
    
    def test_get_without_name_returns_default_provider(self):
        """Verify get without name returns default provider."""
        mock_provider = Mock(spec=PromptProvider)
        PromptProviderRegistry.register("default-provider", mock_provider)
        
        result = PromptProviderRegistry.get()
        
        assert result == mock_provider
    
    def test_get_with_none_returns_default_provider(self):
        """Verify get with None returns default provider."""
        mock_provider = Mock(spec=PromptProvider)
        PromptProviderRegistry.register("default-provider", mock_provider)
        
        result = PromptProviderRegistry.get(None)
        
        assert result == mock_provider
    
    def test_get_returns_none_when_no_default_set(self):
        """Verify get returns None when no default provider set."""
        result = PromptProviderRegistry.get()
        
        assert result is None
    
    def test_get_returns_correct_provider_from_multiple(self):
        """Verify get returns correct provider when multiple registered."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        mock_provider3 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        PromptProviderRegistry.register("provider2", mock_provider2)
        PromptProviderRegistry.register("provider3", mock_provider3)
        
        result = PromptProviderRegistry.get("provider2")
        
        assert result == mock_provider2
    
    def test_get_default_returns_explicitly_set_default(self):
        """Verify get returns explicitly set default provider."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        PromptProviderRegistry.register("provider2", mock_provider2, default=True)
        
        result = PromptProviderRegistry.get()
        
        assert result == mock_provider2


class TestPromptProviderRegistryListRegistered:
    """Tests for list_registered class method."""
    
    def setup_method(self):
        """Clear registry before each test."""
        PromptProviderRegistry._registry = {}
        PromptProviderRegistry._default_provider_name = None
    
    def test_list_registered_returns_empty_dict_when_empty(self):
        """Verify list_registered returns empty dict when no providers."""
        result = PromptProviderRegistry.list_registered()
        
        assert result == {}
        assert isinstance(result, dict)
    
    def test_list_registered_returns_all_providers(self):
        """Verify list_registered returns all registered providers."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        mock_provider3 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        PromptProviderRegistry.register("provider2", mock_provider2)
        PromptProviderRegistry.register("provider3", mock_provider3)
        
        result = PromptProviderRegistry.list_registered()
        
        assert len(result) == 3
        assert result["provider1"] == mock_provider1
        assert result["provider2"] == mock_provider2
        assert result["provider3"] == mock_provider3
    
    def test_list_registered_returns_copy_of_registry(self):
        """Verify list_registered returns copy, not reference."""
        mock_provider = Mock(spec=PromptProvider)
        PromptProviderRegistry.register("test", mock_provider)
        
        result = PromptProviderRegistry.list_registered()
        
        # Modify returned dict
        result["new-provider"] = Mock(spec=PromptProvider)
        
        # Original registry should be unchanged
        assert "new-provider" not in PromptProviderRegistry._registry
    
    def test_list_registered_includes_all_names(self):
        """Verify list_registered includes all provider names."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("aws-prod", mock_provider1)
        PromptProviderRegistry.register("local-dev", mock_provider2)
        
        result = PromptProviderRegistry.list_registered()
        
        assert "aws-prod" in result
        assert "local-dev" in result


class TestPromptProviderRegistryIntegration:
    """Integration tests for PromptProviderRegistry."""
    
    def setup_method(self):
        """Clear registry before each test."""
        PromptProviderRegistry._registry = {}
        PromptProviderRegistry._default_provider_name = None
    
    def test_complete_workflow(self):
        """Verify complete workflow of register, get, and list."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        
        # Register providers
        PromptProviderRegistry.register("prod", mock_provider1)
        PromptProviderRegistry.register("dev", mock_provider2, default=True)
        
        # Get by name
        prod_provider = PromptProviderRegistry.get("prod")
        assert prod_provider == mock_provider1
        
        # Get default
        default_provider = PromptProviderRegistry.get()
        assert default_provider == mock_provider2
        
        # List all
        all_providers = PromptProviderRegistry.list_registered()
        assert len(all_providers) == 2
        assert all_providers["prod"] == mock_provider1
        assert all_providers["dev"] == mock_provider2
    
    def test_registry_is_global(self):
        """Verify registry is shared across all accesses."""
        mock_provider = Mock(spec=PromptProvider)
        
        # Register in one place
        PromptProviderRegistry.register("global-test", mock_provider)
        
        # Should be accessible elsewhere
        result = PromptProviderRegistry.get("global-test")
        assert result == mock_provider
        
        # Should appear in list
        all_providers = PromptProviderRegistry.list_registered()
        assert "global-test" in all_providers
    
    def test_can_update_default_provider(self):
        """Verify default provider can be updated."""
        mock_provider1 = Mock(spec=PromptProvider)
        mock_provider2 = Mock(spec=PromptProvider)
        mock_provider3 = Mock(spec=PromptProvider)
        
        PromptProviderRegistry.register("provider1", mock_provider1)
        assert PromptProviderRegistry.get() == mock_provider1
        
        PromptProviderRegistry.register("provider2", mock_provider2, default=True)
        assert PromptProviderRegistry.get() == mock_provider2
        
        PromptProviderRegistry.register("provider3", mock_provider3, default=True)
        assert PromptProviderRegistry.get() == mock_provider3
    
    def test_registry_supports_different_provider_types(self):
        """Verify registry can store different provider implementations."""
        # Create mock providers with different behaviors
        static_provider = Mock(spec=PromptProvider)
        static_provider.get_system_prompt.return_value = "static system"
        
        bedrock_provider = Mock(spec=PromptProvider)
        bedrock_provider.get_system_prompt.return_value = "bedrock system"
        
        file_provider = Mock(spec=PromptProvider)
        file_provider.get_system_prompt.return_value = "file system"
        
        # Register all types
        PromptProviderRegistry.register("static", static_provider)
        PromptProviderRegistry.register("bedrock", bedrock_provider)
        PromptProviderRegistry.register("file", file_provider)
        
        # Verify each can be retrieved and used
        assert PromptProviderRegistry.get("static").get_system_prompt() == "static system"
        assert PromptProviderRegistry.get("bedrock").get_system_prompt() == "bedrock system"
        assert PromptProviderRegistry.get("file").get_system_prompt() == "file system"


class TestPromptProviderRegistryClassMethods:
    """Tests for registry class method behavior."""
    
    def setup_method(self):
        """Clear registry before each test."""
        PromptProviderRegistry._registry = {}
        PromptProviderRegistry._default_provider_name = None
    
    def test_register_is_class_method(self):
        """Verify register is a class method."""
        assert isinstance(PromptProviderRegistry.__dict__['register'], classmethod)
    
    def test_get_is_class_method(self):
        """Verify get is a class method."""
        assert isinstance(PromptProviderRegistry.__dict__['get'], classmethod)
    
    def test_list_registered_is_class_method(self):
        """Verify list_registered is a class method."""
        assert isinstance(PromptProviderRegistry.__dict__['list_registered'], classmethod)
    
    def test_can_call_without_instance(self):
        """Verify methods can be called without registry instance."""
        mock_provider = Mock(spec=PromptProvider)
        
        # Should work without creating registry instance
        PromptProviderRegistry.register("test", mock_provider)
        result = PromptProviderRegistry.get("test")
        all_providers = PromptProviderRegistry.list_registered()
        
        assert result == mock_provider
        assert "test" in all_providers
