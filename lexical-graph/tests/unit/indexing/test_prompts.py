# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for indexing prompts module.

Tests verify that prompt templates load correctly, variable substitution
works as expected, and prompt validation catches errors. These prompts
are used for LLM-based entity extraction and other indexing operations.

Validates: Requirements 3.7, 11.1, 11.2, 11.3, 11.4
"""

import pytest
from graphrag_toolkit.lexical_graph.indexing.prompts import (
    EXTRACT_PROPOSITIONS_PROMPT,
    EXTRACT_TOPICS_PROMPT,
    DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
    RANK_ENTITY_CLASSIFICATIONS_PROMPT
)


class TestPromptTemplateLoading:
    """Tests for prompt template loading and accessibility."""

    def test_extract_propositions_prompt_defined(self):
        """Verify EXTRACT_PROPOSITIONS_PROMPT is defined and non-empty."""
        assert EXTRACT_PROPOSITIONS_PROMPT is not None
        assert isinstance(EXTRACT_PROPOSITIONS_PROMPT, str)
        assert len(EXTRACT_PROPOSITIONS_PROMPT) > 0

    def test_extract_topics_prompt_defined(self):
        """Verify EXTRACT_TOPICS_PROMPT is defined and non-empty."""
        assert EXTRACT_TOPICS_PROMPT is not None
        assert isinstance(EXTRACT_TOPICS_PROMPT, str)
        assert len(EXTRACT_TOPICS_PROMPT) > 0

    def test_domain_entity_classifications_prompt_defined(self):
        """Verify DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT is defined and non-empty."""
        assert DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT is not None
        assert isinstance(DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT, str)
        assert len(DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT) > 0

    def test_rank_entity_classifications_prompt_defined(self):
        """Verify RANK_ENTITY_CLASSIFICATIONS_PROMPT is defined and non-empty."""
        assert RANK_ENTITY_CLASSIFICATIONS_PROMPT is not None
        assert isinstance(RANK_ENTITY_CLASSIFICATIONS_PROMPT, str)
        assert len(RANK_ENTITY_CLASSIFICATIONS_PROMPT) > 0

    def test_all_prompts_are_strings(self):
        """Verify all prompt templates are string types."""
        prompts = [
            EXTRACT_PROPOSITIONS_PROMPT,
            EXTRACT_TOPICS_PROMPT,
            DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            RANK_ENTITY_CLASSIFICATIONS_PROMPT
        ]
        for prompt in prompts:
            assert isinstance(prompt, str), "All prompts should be strings"

    def test_prompts_have_substantial_content(self):
        """Verify prompts contain substantial instructional content."""
        prompts = [
            EXTRACT_PROPOSITIONS_PROMPT,
            EXTRACT_TOPICS_PROMPT,
            DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            RANK_ENTITY_CLASSIFICATIONS_PROMPT
        ]
        for prompt in prompts:
            # Prompts should be substantial (at least 100 characters)
            assert len(prompt) > 100, f"Prompt should contain substantial content"

    def test_prompts_contain_instructions(self):
        """Verify prompts contain instructional keywords."""
        # Check that prompts contain common instruction keywords
        instruction_keywords = ['extract', 'identify', 'format', 'ensure', 'provide']
        
        prompts = {
            'EXTRACT_PROPOSITIONS_PROMPT': EXTRACT_PROPOSITIONS_PROMPT,
            'EXTRACT_TOPICS_PROMPT': EXTRACT_TOPICS_PROMPT,
            'DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT': DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            'RANK_ENTITY_CLASSIFICATIONS_PROMPT': RANK_ENTITY_CLASSIFICATIONS_PROMPT
        }
        
        for prompt_name, prompt_text in prompts.items():
            prompt_lower = prompt_text.lower()
            has_instruction = any(keyword in prompt_lower for keyword in instruction_keywords)
            assert has_instruction, f"{prompt_name} should contain instructional keywords"


class TestPromptVariableSubstitution:
    """Tests for prompt variable substitution functionality."""

    def test_extract_propositions_prompt_has_placeholders(self):
        """Verify EXTRACT_PROPOSITIONS_PROMPT contains expected placeholders."""
        assert '{source_info}' in EXTRACT_PROPOSITIONS_PROMPT
        assert '{text}' in EXTRACT_PROPOSITIONS_PROMPT

    def test_extract_topics_prompt_has_placeholders(self):
        """Verify EXTRACT_TOPICS_PROMPT contains expected placeholders."""
        assert '{text}' in EXTRACT_TOPICS_PROMPT
        assert '{preferred_topics}' in EXTRACT_TOPICS_PROMPT
        assert '{preferred_entity_classifications}' in EXTRACT_TOPICS_PROMPT

    def test_domain_entity_classifications_prompt_has_placeholders(self):
        """Verify DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT contains expected placeholders."""
        assert '{text_chunks}' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        assert '{existing_classifications}' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT

    def test_rank_entity_classifications_prompt_has_placeholders(self):
        """Verify RANK_ENTITY_CLASSIFICATIONS_PROMPT contains expected placeholders."""
        assert '{classifications}' in RANK_ENTITY_CLASSIFICATIONS_PROMPT

    def test_extract_propositions_substitution(self):
        """Verify EXTRACT_PROPOSITIONS_PROMPT variable substitution works correctly."""
        source_info = "Document: GraphRAG Overview"
        text = "GraphRAG combines knowledge graphs with retrieval."
        
        formatted_prompt = EXTRACT_PROPOSITIONS_PROMPT.format(
            source_info=source_info,
            text=text
        )
        
        assert source_info in formatted_prompt
        assert text in formatted_prompt
        assert '{source_info}' not in formatted_prompt
        assert '{text}' not in formatted_prompt

    def test_extract_topics_substitution(self):
        """Verify EXTRACT_TOPICS_PROMPT variable substitution works correctly."""
        text = "GraphRAG is a framework."
        preferred_topics = "Technology, Software"
        preferred_entity_classifications = "Company, Product"
        
        formatted_prompt = EXTRACT_TOPICS_PROMPT.format(
            text=text,
            preferred_topics=preferred_topics,
            preferred_entity_classifications=preferred_entity_classifications
        )
        
        assert text in formatted_prompt
        assert preferred_topics in formatted_prompt
        assert preferred_entity_classifications in formatted_prompt
        assert '{text}' not in formatted_prompt
        assert '{preferred_topics}' not in formatted_prompt
        assert '{preferred_entity_classifications}' not in formatted_prompt

    def test_domain_entity_classifications_substitution(self):
        """Verify DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT variable substitution works correctly."""
        text_chunks = "Sample text chunk 1\nSample text chunk 2"
        existing_classifications = "Person, Location, Organization"
        
        formatted_prompt = DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT.format(
            text_chunks=text_chunks,
            existing_classifications=existing_classifications
        )
        
        assert text_chunks in formatted_prompt
        assert existing_classifications in formatted_prompt
        assert '{text_chunks}' not in formatted_prompt
        assert '{existing_classifications}' not in formatted_prompt

    def test_rank_entity_classifications_substitution(self):
        """Verify RANK_ENTITY_CLASSIFICATIONS_PROMPT variable substitution works correctly."""
        classifications = "Person\nLocation\nOrganization\nProduct"
        
        formatted_prompt = RANK_ENTITY_CLASSIFICATIONS_PROMPT.format(
            classifications=classifications
        )
        
        assert classifications in formatted_prompt
        assert '{classifications}' not in formatted_prompt

    def test_substitution_with_empty_values(self):
        """Verify prompt substitution handles empty values correctly."""
        # Empty values should still work (no KeyError)
        formatted_prompt = EXTRACT_PROPOSITIONS_PROMPT.format(
            source_info="",
            text=""
        )
        
        assert isinstance(formatted_prompt, str)
        assert '{source_info}' not in formatted_prompt
        assert '{text}' not in formatted_prompt

    def test_substitution_with_special_characters(self):
        """Verify prompt substitution handles special characters correctly."""
        source_info = "Document: Test & Validation <important>"
        text = "Text with 'quotes' and \"double quotes\" and {braces}"
        
        formatted_prompt = EXTRACT_PROPOSITIONS_PROMPT.format(
            source_info=source_info,
            text=text
        )
        
        assert source_info in formatted_prompt
        assert text in formatted_prompt

    def test_substitution_with_multiline_text(self):
        """Verify prompt substitution handles multiline text correctly."""
        text = """Line 1 of text
Line 2 of text
Line 3 of text"""
        
        formatted_prompt = EXTRACT_PROPOSITIONS_PROMPT.format(
            source_info="Test document",
            text=text
        )
        
        assert text in formatted_prompt
        assert formatted_prompt.count('\n') >= text.count('\n')


class TestPromptValidation:
    """Tests for prompt validation and error detection."""

    def test_extract_propositions_missing_placeholder_raises_error(self):
        """Verify KeyError raised when required placeholder is missing."""
        with pytest.raises(KeyError):
            # Missing 'text' placeholder
            EXTRACT_PROPOSITIONS_PROMPT.format(source_info="test")

    def test_extract_topics_missing_placeholder_raises_error(self):
        """Verify KeyError raised when required placeholder is missing."""
        with pytest.raises(KeyError):
            # Missing 'preferred_topics' and 'preferred_entity_classifications'
            EXTRACT_TOPICS_PROMPT.format(text="test")

    def test_domain_entity_classifications_missing_placeholder_raises_error(self):
        """Verify KeyError raised when required placeholder is missing."""
        with pytest.raises(KeyError):
            # Missing 'existing_classifications'
            DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT.format(text_chunks="test")

    def test_rank_entity_classifications_missing_placeholder_raises_error(self):
        """Verify KeyError raised when required placeholder is missing."""
        with pytest.raises(KeyError):
            # Missing 'classifications'
            RANK_ENTITY_CLASSIFICATIONS_PROMPT.format()

    def test_prompts_have_no_unintended_placeholders(self):
        """Verify prompts don't contain unintended placeholder patterns."""
        prompts = {
            'EXTRACT_PROPOSITIONS_PROMPT': EXTRACT_PROPOSITIONS_PROMPT,
            'EXTRACT_TOPICS_PROMPT': EXTRACT_TOPICS_PROMPT,
            'DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT': DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            'RANK_ENTITY_CLASSIFICATIONS_PROMPT': RANK_ENTITY_CLASSIFICATIONS_PROMPT
        }
        
        # Define expected placeholders for each prompt
        expected_placeholders = {
            'EXTRACT_PROPOSITIONS_PROMPT': ['{source_info}', '{text}'],
            'EXTRACT_TOPICS_PROMPT': ['{text}', '{preferred_topics}', '{preferred_entity_classifications}'],
            'DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT': ['{text_chunks}', '{existing_classifications}'],
            'RANK_ENTITY_CLASSIFICATIONS_PROMPT': ['{classifications}']
        }
        
        for prompt_name, prompt_text in prompts.items():
            # Find all placeholders in the prompt
            import re
            found_placeholders = set(re.findall(r'\{[^}]+\}', prompt_text))
            expected = set(expected_placeholders[prompt_name])
            
            # Verify only expected placeholders are present
            unexpected = found_placeholders - expected
            assert len(unexpected) == 0, \
                f"{prompt_name} contains unexpected placeholders: {unexpected}"

    def test_prompts_use_xml_tags_correctly(self):
        """Verify prompts use XML-style tags for structured sections."""
        # EXTRACT_PROPOSITIONS_PROMPT should have XML tags
        assert '<sourceInformation>' in EXTRACT_PROPOSITIONS_PROMPT
        assert '</sourceInformation>' in EXTRACT_PROPOSITIONS_PROMPT
        assert '<text>' in EXTRACT_PROPOSITIONS_PROMPT
        assert '</text>' in EXTRACT_PROPOSITIONS_PROMPT
        
        # EXTRACT_TOPICS_PROMPT should have XML tags
        assert '<propositions>' in EXTRACT_TOPICS_PROMPT
        assert '</propositions>' in EXTRACT_TOPICS_PROMPT
        assert '<preferredTopics>' in EXTRACT_TOPICS_PROMPT
        assert '</preferredTopics>' in EXTRACT_TOPICS_PROMPT
        assert '<preferredEntityClassifications>' in EXTRACT_TOPICS_PROMPT
        assert '</preferredEntityClassifications>' in EXTRACT_TOPICS_PROMPT
        
        # DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT should have XML tags
        assert '<chunks>' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        assert '</chunks>' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        assert '<existing_classifications>' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        assert '</existing_classifications>' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        assert '<entity_classifications>' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        assert '</entity_classifications>' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        
        # RANK_ENTITY_CLASSIFICATIONS_PROMPT should have XML tags
        assert '<classifications>' in RANK_ENTITY_CLASSIFICATIONS_PROMPT
        assert '</classifications>' in RANK_ENTITY_CLASSIFICATIONS_PROMPT
        assert '<entity_classifications>' in RANK_ENTITY_CLASSIFICATIONS_PROMPT
        assert '</entity_classifications>' in RANK_ENTITY_CLASSIFICATIONS_PROMPT

    def test_xml_tags_are_balanced(self):
        """Verify XML-style tags are properly balanced in prompts."""
        prompts = {
            'EXTRACT_PROPOSITIONS_PROMPT': EXTRACT_PROPOSITIONS_PROMPT,
            'EXTRACT_TOPICS_PROMPT': EXTRACT_TOPICS_PROMPT,
            'DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT': DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            'RANK_ENTITY_CLASSIFICATIONS_PROMPT': RANK_ENTITY_CLASSIFICATIONS_PROMPT
        }
        
        for prompt_name, prompt_text in prompts.items():
            # Find all XML tags
            import re
            opening_tags = re.findall(r'<([^/>]+)>', prompt_text)
            closing_tags = re.findall(r'</([^>]+)>', prompt_text)
            
            # Count occurrences of each tag
            from collections import Counter
            opening_counts = Counter(opening_tags)
            closing_counts = Counter(closing_tags)
            
            # Verify each opening tag has a matching closing tag
            for tag in opening_counts:
                assert tag in closing_counts, \
                    f"{prompt_name}: Opening tag <{tag}> has no matching closing tag"
                assert opening_counts[tag] == closing_counts[tag], \
                    f"{prompt_name}: Tag <{tag}> count mismatch (open: {opening_counts[tag]}, close: {closing_counts[tag]})"

    def test_prompts_contain_output_format_instructions(self):
        """Verify prompts contain output format instructions."""
        # EXTRACT_PROPOSITIONS_PROMPT should specify output format
        assert 'Output Format:' in EXTRACT_PROPOSITIONS_PROMPT or \
               'Response Format:' in EXTRACT_PROPOSITIONS_PROMPT
        
        # EXTRACT_TOPICS_PROMPT should specify response format
        assert 'Response Format:' in EXTRACT_TOPICS_PROMPT
        
        # DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT should specify expected format
        assert 'Expected format:' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT
        
        # RANK_ENTITY_CLASSIFICATIONS_PROMPT should specify expected format
        assert 'Expected format:' in RANK_ENTITY_CLASSIFICATIONS_PROMPT


# Parametrized tests for efficiency

@pytest.mark.parametrize("prompt_name,prompt_value", [
    ("EXTRACT_PROPOSITIONS_PROMPT", EXTRACT_PROPOSITIONS_PROMPT),
    ("EXTRACT_TOPICS_PROMPT", EXTRACT_TOPICS_PROMPT),
    ("DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT", DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT),
    ("RANK_ENTITY_CLASSIFICATIONS_PROMPT", RANK_ENTITY_CLASSIFICATIONS_PROMPT),
])
def test_prompt_is_non_empty_string(prompt_name, prompt_value):
    """
    Parametrized test verifying each prompt is a non-empty string.
    
    Tests that each prompt constant is defined as a non-empty string.
    """
    assert isinstance(prompt_value, str), \
        f"{prompt_name} should be a string"
    assert len(prompt_value) > 0, \
        f"{prompt_name} should not be empty"


@pytest.mark.parametrize("prompt_name,prompt_value,expected_placeholders", [
    ("EXTRACT_PROPOSITIONS_PROMPT", EXTRACT_PROPOSITIONS_PROMPT, ['{source_info}', '{text}']),
    ("EXTRACT_TOPICS_PROMPT", EXTRACT_TOPICS_PROMPT, ['{text}', '{preferred_topics}', '{preferred_entity_classifications}']),
    ("DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT", DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT, ['{text_chunks}', '{existing_classifications}']),
    ("RANK_ENTITY_CLASSIFICATIONS_PROMPT", RANK_ENTITY_CLASSIFICATIONS_PROMPT, ['{classifications}']),
])
def test_prompt_contains_expected_placeholders(prompt_name, prompt_value, expected_placeholders):
    """
    Parametrized test verifying each prompt contains its expected placeholders.
    
    Tests that each prompt has the correct variable placeholders for substitution.
    """
    for placeholder in expected_placeholders:
        assert placeholder in prompt_value, \
            f"{prompt_name} should contain placeholder {placeholder}"


class TestPromptIntegration:
    """Integration tests verifying prompts work together correctly."""

    def test_all_prompts_are_unique(self):
        """Verify all prompt templates have unique content."""
        prompts = [
            EXTRACT_PROPOSITIONS_PROMPT,
            EXTRACT_TOPICS_PROMPT,
            DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            RANK_ENTITY_CLASSIFICATIONS_PROMPT
        ]
        
        # Check that no two prompts are identical
        for i, prompt1 in enumerate(prompts):
            for j, prompt2 in enumerate(prompts):
                if i != j:
                    assert prompt1 != prompt2, \
                        f"Prompts at indices {i} and {j} should be unique"

    def test_prompts_follow_consistent_style(self):
        """Verify prompts follow consistent instructional style."""
        prompts = [
            EXTRACT_PROPOSITIONS_PROMPT,
            EXTRACT_TOPICS_PROMPT,
            DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT,
            RANK_ENTITY_CLASSIFICATIONS_PROMPT
        ]
        
        # All prompts should contain structured instructions
        for prompt in prompts:
            # Should contain numbered or bulleted instructions
            has_structure = any(marker in prompt for marker in ['1.', '2.', '-', '*'])
            assert has_structure, "Prompt should contain structured instructions"

    def test_entity_classification_prompts_are_related(self):
        """Verify entity classification prompts are thematically related."""
        # Both classification prompts should mention 'entity' and 'classification'
        assert 'entity' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT.lower()
        assert 'classification' in DOMAIN_ENTITY_CLASSIFICATIONS_PROMPT.lower()
        assert 'entity' in RANK_ENTITY_CLASSIFICATIONS_PROMPT.lower()
        assert 'classification' in RANK_ENTITY_CLASSIFICATIONS_PROMPT.lower()

    def test_extraction_prompts_mention_knowledge_graph(self):
        """Verify extraction prompts reference knowledge graph context."""
        # Extraction prompts should mention knowledge graph
        assert 'knowledge graph' in EXTRACT_PROPOSITIONS_PROMPT.lower()
        assert 'knowledge graph' in EXTRACT_TOPICS_PROMPT.lower()
