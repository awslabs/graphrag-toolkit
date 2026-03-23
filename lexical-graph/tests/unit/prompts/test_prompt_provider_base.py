# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for PromptProvider base class.

This module tests the abstract base class for prompt providers,
ensuring proper interface definition and abstract method enforcement.
"""

import pytest
from abc import ABC
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider


class TestPromptProviderBase:
    """Tests for PromptProvider abstract base class."""
    
    def test_is_abstract_base_class(self):
        """Verify PromptProvider is an abstract base class."""
        assert issubclass(PromptProvider, ABC)
    
    def test_cannot_instantiate_directly(self):
        """Verify PromptProvider cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            PromptProvider()
    
    def test_has_get_system_prompt_abstract_method(self):
        """Verify get_system_prompt is an abstract method."""
        assert hasattr(PromptProvider, 'get_system_prompt')
        assert getattr(PromptProvider.get_system_prompt, '__isabstractmethod__', False)
    
    def test_has_get_user_prompt_abstract_method(self):
        """Verify get_user_prompt is an abstract method."""
        assert hasattr(PromptProvider, 'get_user_prompt')
        assert getattr(PromptProvider.get_user_prompt, '__isabstractmethod__', False)
    
    def test_concrete_implementation_requires_both_methods(self):
        """Verify concrete implementations must implement both abstract methods."""
        # Missing both methods
        with pytest.raises(TypeError):
            class IncompleteProvider(PromptProvider):
                pass
            IncompleteProvider()
        
        # Missing get_user_prompt
        with pytest.raises(TypeError):
            class PartialProvider(PromptProvider):
                def get_system_prompt(self):
                    return "system"
            PartialProvider()
    
    def test_concrete_implementation_with_both_methods(self):
        """Verify concrete implementation works when both methods are implemented."""
        class ConcreteProvider(PromptProvider):
            def get_system_prompt(self):
                return "system prompt"
            
            def get_user_prompt(self):
                return "user prompt"
        
        provider = ConcreteProvider()
        assert provider.get_system_prompt() == "system prompt"
        assert provider.get_user_prompt() == "user prompt"
    
    def test_subclass_can_add_additional_methods(self):
        """Verify subclasses can add additional methods beyond the interface."""
        class ExtendedProvider(PromptProvider):
            def get_system_prompt(self):
                return "system"
            
            def get_user_prompt(self):
                return "user"
            
            def get_custom_prompt(self):
                return "custom"
        
        provider = ExtendedProvider()
        assert provider.get_custom_prompt() == "custom"
    
    def test_subclass_can_override_with_parameters(self):
        """Verify subclasses can override methods with different signatures."""
        class ParameterizedProvider(PromptProvider):
            def get_system_prompt(self, context=None):
                return f"system: {context}" if context else "system"
            
            def get_user_prompt(self, query=None):
                return f"user: {query}" if query else "user"
        
        provider = ParameterizedProvider()
        assert provider.get_system_prompt() == "system"
        assert provider.get_system_prompt("test") == "system: test"
        assert provider.get_user_prompt() == "user"
        assert provider.get_user_prompt("query") == "user: query"
