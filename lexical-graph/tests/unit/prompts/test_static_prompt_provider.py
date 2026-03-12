# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for StaticPromptProvider.

This module tests the static prompt provider which returns predefined
system and user prompts without external dependencies.
"""

import pytest
from unittest.mock import patch
from graphrag_toolkit.lexical_graph.prompts.static_prompt_provider import StaticPromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider


class TestStaticPromptProviderInitialization:
    """Tests for StaticPromptProvider initialization."""
    
    def test_initialization_succeeds(self):
        """Verify StaticPromptProvider initializes without errors."""
        provider = StaticPromptProvider()
        assert provider is not None
    
    def test_is_prompt_provider_subclass(self):
        """Verify StaticPromptProvider is a PromptProvider subclass."""
        assert issubclass(StaticPromptProvider, PromptProvider)
    
    def test_has_system_prompt_attribute(self):
        """Verify provider has _system_prompt attribute after initialization."""
        provider = StaticPromptProvider()
        assert hasattr(provider, '_system_prompt')
        assert isinstance(provider._system_prompt, str)
    
    def test_has_user_prompt_attribute(self):
        """Verify provider has _user_prompt attribute after initialization."""
        provider = StaticPromptProvider()
        assert hasattr(provider, '_user_prompt')
        assert isinstance(provider._user_prompt, str)
    
    def test_prompts_are_not_empty(self):
        """Verify initialized prompts are not empty strings."""
        provider = StaticPromptProvider()
        assert len(provider._system_prompt) > 0
        assert len(provider._user_prompt) > 0


class TestStaticPromptProviderGetSystemPrompt:
    """Tests for get_system_prompt method."""
    
    def test_returns_string(self):
        """Verify get_system_prompt returns a string."""
        provider = StaticPromptProvider()
        result = provider.get_system_prompt()
        assert isinstance(result, str)
    
    def test_returns_non_empty_string(self):
        """Verify get_system_prompt returns non-empty string."""
        provider = StaticPromptProvider()
        result = provider.get_system_prompt()
        assert len(result) > 0
    
    def test_returns_consistent_value(self):
        """Verify get_system_prompt returns same value on multiple calls."""
        provider = StaticPromptProvider()
        result1 = provider.get_system_prompt()
        result2 = provider.get_system_prompt()
        assert result1 == result2
    
    def test_returns_same_value_across_instances(self):
        """Verify different instances return the same system prompt."""
        provider1 = StaticPromptProvider()
        provider2 = StaticPromptProvider()
        assert provider1.get_system_prompt() == provider2.get_system_prompt()


class TestStaticPromptProviderGetUserPrompt:
    """Tests for get_user_prompt method."""
    
    def test_returns_string(self):
        """Verify get_user_prompt returns a string."""
        provider = StaticPromptProvider()
        result = provider.get_user_prompt()
        assert isinstance(result, str)
    
    def test_returns_non_empty_string(self):
        """Verify get_user_prompt returns non-empty string."""
        provider = StaticPromptProvider()
        result = provider.get_user_prompt()
        assert len(result) > 0
    
    def test_returns_consistent_value(self):
        """Verify get_user_prompt returns same value on multiple calls."""
        provider = StaticPromptProvider()
        result1 = provider.get_user_prompt()
        result2 = provider.get_user_prompt()
        assert result1 == result2
    
    def test_returns_same_value_across_instances(self):
        """Verify different instances return the same user prompt."""
        provider1 = StaticPromptProvider()
        provider2 = StaticPromptProvider()
        assert provider1.get_user_prompt() == provider2.get_user_prompt()
    
    def test_handles_aws_template_placeholder(self):
        """Verify get_user_prompt handles AWS template placeholder."""
        provider = StaticPromptProvider()
        # Temporarily modify the user prompt to include placeholder
        original_prompt = provider._user_prompt
        provider._user_prompt = "Test prompt with {aws_template_structure} placeholder"
        
        result = provider.get_user_prompt()
        
        # Should replace placeholder with message
        assert '{aws_template_structure}' not in result
        assert 'not available in static provider' in result
        
        # Restore original
        provider._user_prompt = original_prompt
    
    def test_no_placeholder_returns_unchanged(self):
        """Verify get_user_prompt returns unchanged prompt without placeholder."""
        provider = StaticPromptProvider()
        # Ensure no placeholder in original
        if '{aws_template_structure}' not in provider._user_prompt:
            result = provider.get_user_prompt()
            assert result == provider._user_prompt


class TestStaticPromptProviderIntegration:
    """Integration tests for StaticPromptProvider."""
    
    def test_can_be_used_as_prompt_provider(self):
        """Verify StaticPromptProvider can be used as PromptProvider interface."""
        provider: PromptProvider = StaticPromptProvider()
        
        system_prompt = provider.get_system_prompt()
        user_prompt = provider.get_user_prompt()
        
        assert isinstance(system_prompt, str)
        assert isinstance(user_prompt, str)
        assert len(system_prompt) > 0
        assert len(user_prompt) > 0
    
    def test_prompts_are_different(self):
        """Verify system and user prompts are different."""
        provider = StaticPromptProvider()
        system_prompt = provider.get_system_prompt()
        user_prompt = provider.get_user_prompt()
        
        # They should be different prompts
        assert system_prompt != user_prompt
    
    def test_no_external_dependencies(self):
        """Verify StaticPromptProvider works without external dependencies."""
        # Should work without any mocking or patching
        provider = StaticPromptProvider()
        system_prompt = provider.get_system_prompt()
        user_prompt = provider.get_user_prompt()
        
        assert system_prompt is not None
        assert user_prompt is not None
