# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import json
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import parse_extracted_topics_json
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection
from graphrag_toolkit.lexical_graph.indexing.prompts import EXTRACT_TOPICS_JSON_PROMPT


class TestParseExtractedTopicsJson:
    """Tests for parse_extracted_topics_json function."""

    def test_valid_json(self):
        """Verify valid JSON is parsed into TopicCollection."""
        data = {
            "topics": [{
                "value": "Test Topic",
                "entities": [{"value": "John", "classification": "Person"}],
                "statements": [{
                    "value": "John works at Acme",
                    "facts": [{
                        "subject": {"value": "John", "classification": "Person"},
                        "predicate": {"value": "WORKS_FOR"},
                        "object": {"value": "Acme", "classification": "Company"},
                    }]
                }]
            }]
        }
        tc, garbage = parse_extracted_topics_json(json.dumps(data))
        assert len(garbage) == 0
        assert len(tc.topics) == 1
        assert tc.topics[0].value == "Test Topic"
        assert len(tc.topics[0].entities) == 1
        assert tc.topics[0].entities[0].value == "John"
        assert len(tc.topics[0].statements[0].facts) == 1

    def test_json_with_code_fences(self):
        """Verify JSON wrapped in markdown code fences is parsed."""
        data = {"topics": [{"value": "Topic", "entities": [], "statements": []}]}
        raw = f"```json\n{json.dumps(data)}\n```"
        tc, garbage = parse_extracted_topics_json(raw)
        assert len(garbage) == 0
        assert len(tc.topics) == 1

    def test_empty_topics(self):
        """Verify empty topics list is valid."""
        tc, garbage = parse_extracted_topics_json('{"topics": []}')
        assert len(garbage) == 0
        assert len(tc.topics) == 0

    def test_invalid_json_returns_error(self):
        """Verify invalid JSON returns empty TopicCollection with error."""
        tc, garbage = parse_extracted_topics_json('not json at all')
        assert len(tc.topics) == 0
        assert len(garbage) == 1
        assert 'JSON_PARSE_ERROR' in garbage[0]

    def test_malformed_structure_returns_error(self):
        """Verify JSON with wrong structure returns error."""
        tc, garbage = parse_extracted_topics_json('{"wrong_key": []}')
        assert len(tc.topics) == 0

    def test_whitespace_handling(self):
        """Verify leading/trailing whitespace is handled."""
        data = {"topics": [{"value": "Topic", "entities": [], "statements": []}]}
        raw = f"  \n{json.dumps(data)}\n  "
        tc, garbage = parse_extracted_topics_json(raw)
        assert len(garbage) == 0
        assert len(tc.topics) == 1


class TestExtractTopicsJsonPrompt:
    """Tests for EXTRACT_TOPICS_JSON_PROMPT template."""

    def test_prompt_has_required_placeholders(self):
        """Verify prompt contains all required template variables."""
        assert '{text}' in EXTRACT_TOPICS_JSON_PROMPT
        assert '{preferred_topics}' in EXTRACT_TOPICS_JSON_PROMPT
        assert '{preferred_entity_classifications}' in EXTRACT_TOPICS_JSON_PROMPT
        assert '{schema_constraints}' in EXTRACT_TOPICS_JSON_PROMPT

    def test_prompt_requests_json_output(self):
        """Verify prompt instructs JSON output."""
        assert 'JSON' in EXTRACT_TOPICS_JSON_PROMPT
        assert '"topics"' in EXTRACT_TOPICS_JSON_PROMPT
