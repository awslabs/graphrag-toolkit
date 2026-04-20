# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for FilePromptProvider.

This module tests the file-based prompt provider which loads prompts
from local filesystem files.
"""

import pytest
import os
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.prompts.file_prompt_provider import FilePromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import FilePromptProviderConfig
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider


class TestFilePromptProviderInitialization:
    """Tests for FilePromptProvider initialization."""
    
    def test_initialization_with_valid_directory(self, tmp_path):
        """Verify FilePromptProvider initializes with valid directory."""
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        assert provider.config == config
        assert provider.system_prompt_file == "system_prompt.txt"
        assert provider.user_prompt_file == "user_prompt.txt"
    
    def test_initialization_with_custom_filenames(self, tmp_path):
        """Verify FilePromptProvider accepts custom prompt filenames."""
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(
            config,
            system_prompt_file="custom_system.txt",
            user_prompt_file="custom_user.txt"
        )
        
        assert provider.system_prompt_file == "custom_system.txt"
        assert provider.user_prompt_file == "custom_user.txt"
    
    def test_initialization_with_nonexistent_directory_raises_error(self):
        """Verify NotADirectoryError raised for nonexistent directory."""
        config = FilePromptProviderConfig(base_path="/nonexistent/path")
        
        with pytest.raises(NotADirectoryError, match="Invalid or non-existent directory"):
            FilePromptProvider(config)
    
    def test_initialization_with_file_instead_of_directory_raises_error(self, tmp_path):
        """Verify NotADirectoryError raised when path is a file."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")
        
        config = FilePromptProviderConfig(base_path=str(file_path))
        
        with pytest.raises(NotADirectoryError, match="Invalid or non-existent directory"):
            FilePromptProvider(config)
    
    def test_is_prompt_provider_subclass(self):
        """Verify FilePromptProvider is a PromptProvider subclass."""
        assert issubclass(FilePromptProvider, PromptProvider)


class TestFilePromptProviderLoadPrompt:
    """Tests for _load_prompt private method."""
    
    def test_load_existing_file(self, tmp_path):
        """Verify _load_prompt loads existing file content."""
        prompt_file = tmp_path / "test_prompt.txt"
        prompt_file.write_text("Test prompt content")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider._load_prompt("test_prompt.txt")
        assert result == "Test prompt content"
    
    def test_load_file_strips_trailing_whitespace(self, tmp_path):
        """Verify _load_prompt strips trailing whitespace."""
        prompt_file = tmp_path / "test_prompt.txt"
        prompt_file.write_text("Test prompt\n\n\n")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider._load_prompt("test_prompt.txt")
        assert result == "Test prompt"
    
    def test_load_nonexistent_file_raises_error(self, tmp_path):
        """Verify FileNotFoundError raised for nonexistent file."""
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        with pytest.raises(FileNotFoundError, match="Prompt file not found"):
            provider._load_prompt("nonexistent.txt")
    
    def test_load_file_with_utf8_encoding(self, tmp_path):
        """Verify _load_prompt handles UTF-8 encoded files."""
        prompt_file = tmp_path / "test_prompt.txt"
        prompt_file.write_text("Test with émojis 🚀 and spëcial çhars", encoding="utf-8")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider._load_prompt("test_prompt.txt")
        assert "émojis 🚀" in result
        assert "spëcial çhars" in result
    
    def test_load_empty_file(self, tmp_path):
        """Verify _load_prompt handles empty files."""
        prompt_file = tmp_path / "empty.txt"
        prompt_file.write_text("")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider._load_prompt("empty.txt")
        assert result == ""
    
    def test_load_file_with_multiline_content(self, tmp_path):
        """Verify _load_prompt preserves multiline content."""
        content = "Line 1\nLine 2\nLine 3"
        prompt_file = tmp_path / "multiline.txt"
        prompt_file.write_text(content)
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider._load_prompt("multiline.txt")
        assert result == content


class TestFilePromptProviderGetSystemPrompt:
    """Tests for get_system_prompt method."""
    
    def test_returns_system_prompt_content(self, tmp_path):
        """Verify get_system_prompt returns system prompt file content."""
        system_file = tmp_path / "system_prompt.txt"
        system_file.write_text("System prompt content")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider.get_system_prompt()
        assert result == "System prompt content"
    
    def test_uses_custom_system_filename(self, tmp_path):
        """Verify get_system_prompt uses custom filename."""
        custom_file = tmp_path / "custom_system.txt"
        custom_file.write_text("Custom system prompt")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config, system_prompt_file="custom_system.txt")
        
        result = provider.get_system_prompt()
        assert result == "Custom system prompt"
    
    def test_raises_error_when_file_missing(self, tmp_path):
        """Verify FileNotFoundError raised when system prompt file missing."""
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        with pytest.raises(FileNotFoundError):
            provider.get_system_prompt()


class TestFilePromptProviderGetUserPrompt:
    """Tests for get_user_prompt method."""
    
    def test_returns_user_prompt_content(self, tmp_path):
        """Verify get_user_prompt returns user prompt file content."""
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("User prompt content")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider.get_user_prompt()
        assert result == "User prompt content"
    
    def test_uses_custom_user_filename(self, tmp_path):
        """Verify get_user_prompt uses custom filename."""
        custom_file = tmp_path / "custom_user.txt"
        custom_file.write_text("Custom user prompt")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config, user_prompt_file="custom_user.txt")
        
        result = provider.get_user_prompt()
        assert result == "Custom user prompt"
    
    def test_raises_error_when_file_missing(self, tmp_path):
        """Verify FileNotFoundError raised when user prompt file missing."""
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        with pytest.raises(FileNotFoundError):
            provider.get_user_prompt()
    
    def test_substitutes_aws_template_when_present(self, tmp_path):
        """Verify get_user_prompt substitutes AWS template placeholder."""
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("Prompt with {aws_template_structure} placeholder")
        
        template_file = tmp_path / "template.json"
        template_data = {"key": "value", "nested": {"field": "data"}}
        template_file.write_text(json.dumps(template_data))
        
        config = FilePromptProviderConfig(
            base_path=str(tmp_path),
            aws_template_file="template.json"
        )
        provider = FilePromptProvider(config)
        
        result = provider.get_user_prompt()
        
        assert '{aws_template_structure}' not in result
        assert '"key": "value"' in result
        assert '"nested"' in result
    
    def test_handles_missing_aws_template_file(self, tmp_path):
        """Verify get_user_prompt handles missing AWS template file."""
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("Prompt with {aws_template_structure} placeholder")
        
        config = FilePromptProviderConfig(
            base_path=str(tmp_path),
            aws_template_file="nonexistent.json"
        )
        provider = FilePromptProvider(config)
        
        result = provider.get_user_prompt()
        
        assert '{aws_template_structure}' not in result
        assert 'template file not found' in result
    
    def test_no_substitution_without_placeholder(self, tmp_path):
        """Verify get_user_prompt returns unchanged when no placeholder."""
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("Simple user prompt")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider.get_user_prompt()
        assert result == "Simple user prompt"
    
    def test_no_substitution_when_template_file_not_configured(self, tmp_path):
        """Verify no substitution when aws_template_file not configured."""
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("Prompt with {aws_template_structure} placeholder")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        result = provider.get_user_prompt()
        
        # Should replace with fallback message
        assert '{aws_template_structure}' not in result
        assert 'template file not found' in result


class TestFilePromptProviderIntegration:
    """Integration tests for FilePromptProvider."""
    
    def test_complete_workflow(self, tmp_path):
        """Verify complete workflow with both prompts."""
        system_file = tmp_path / "system_prompt.txt"
        system_file.write_text("System: You are a helpful assistant.")
        
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("User: Answer the question: {question}")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider = FilePromptProvider(config)
        
        system_prompt = provider.get_system_prompt()
        user_prompt = provider.get_user_prompt()
        
        assert "helpful assistant" in system_prompt
        assert "Answer the question" in user_prompt
    
    def test_can_be_used_as_prompt_provider_interface(self, tmp_path):
        """Verify FilePromptProvider implements PromptProvider interface."""
        system_file = tmp_path / "system_prompt.txt"
        system_file.write_text("System prompt")
        
        user_file = tmp_path / "user_prompt.txt"
        user_file.write_text("User prompt")
        
        config = FilePromptProviderConfig(base_path=str(tmp_path))
        provider: PromptProvider = FilePromptProvider(config)
        
        assert isinstance(provider.get_system_prompt(), str)
        assert isinstance(provider.get_user_prompt(), str)
