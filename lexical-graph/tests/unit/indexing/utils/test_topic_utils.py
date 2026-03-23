# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import (
    format_text,
    format_list,
    clean,
    format_value,
    format_classification,
    strip_full_stop,
    remove_parenthetical_content,
    remove_articles,
    parse_extracted_topics
)
from graphrag_toolkit.lexical_graph.indexing.model import TopicCollection, Topic, Statement, Entity, Fact, Relation
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_TOPIC, LOCAL_ENTITY_CLASSIFICATION


class TestFormatText:
    """Tests for format_text function."""
    
    def test_format_text_with_string(self):
        """Verify format_text returns string unchanged."""
        text = "Simple text"
        result = format_text(text)
        assert result == "Simple text"
    
    def test_format_text_with_list(self):
        """Verify format_text joins list with newlines."""
        text_list = ["Line 1", "Line 2", "Line 3"]
        result = format_text(text_list)
        assert result == "Line 1\nLine 2\nLine 3"
    
    def test_format_text_with_empty_list(self):
        """Verify format_text handles empty list."""
        result = format_text([])
        assert result == ""


class TestFormatList:
    """Tests for format_list function."""
    
    def test_format_list_joins_with_newlines(self):
        """Verify format_list joins values with newlines."""
        values = ["Value 1", "Value 2", "Value 3"]
        result = format_list(values)
        assert result == "Value 1\nValue 2\nValue 3"
    
    def test_format_list_empty(self):
        """Verify format_list handles empty list."""
        result = format_list([])
        assert result == ""


class TestClean:
    """Tests for clean function."""
    
    def test_clean_removes_articles_and_parentheses(self):
        """Verify clean removes articles and parenthetical content."""
        text = "The company (founded 2020)"
        result = clean(text)
        assert result == "company"
    
    def test_clean_handles_underscores(self):
        """Verify clean replaces underscores with spaces."""
        text = "entity_name_here"
        result = clean(text)
        assert result == "entity name here"
    
    def test_clean_removes_article_a(self):
        """Verify clean removes article 'a'."""
        text = "a company"
        result = clean(text)
        assert result == "company"
    
    def test_clean_removes_article_an(self):
        """Verify clean removes article 'an'."""
        text = "an organization"
        result = clean(text)
        assert result == "organization"


class TestFormatValue:
    """Tests for format_value function."""
    
    def test_format_value_replaces_underscores(self):
        """Verify format_value replaces underscores with spaces."""
        value = "entity_name"
        result = format_value(value)
        assert result == "entity name"
    
    def test_format_value_handles_none(self):
        """Verify format_value handles None."""
        result = format_value(None)
        assert result == ""
    
    def test_format_value_handles_empty_string(self):
        """Verify format_value handles empty string."""
        result = format_value("")
        assert result == ""


class TestFormatClassification:
    """Tests for format_classification function."""
    
    def test_format_classification_title_cases(self):
        """Verify format_classification converts to title case."""
        classification = "person_entity"
        result = format_classification(classification)
        assert result == "Person Entity"
    
    def test_format_classification_handles_none(self):
        """Verify format_classification handles None."""
        result = format_classification(None)
        assert result == ""


class TestStripFullStop:
    """Tests for strip_full_stop function."""
    
    def test_strip_full_stop_removes_period(self):
        """Verify strip_full_stop removes trailing period."""
        text = "Sentence."
        result = strip_full_stop(text)
        assert result == "Sentence"
    
    def test_strip_full_stop_preserves_no_period(self):
        """Verify strip_full_stop preserves text without period."""
        text = "Sentence"
        result = strip_full_stop(text)
        assert result == "Sentence"
    
    def test_strip_full_stop_handles_none(self):
        """Verify strip_full_stop handles None."""
        result = strip_full_stop(None)
        assert result is None


class TestRemoveParentheticalContent:
    """Tests for remove_parenthetical_content function."""
    
    def test_remove_parenthetical_content_removes_parens(self):
        """Verify remove_parenthetical_content removes content in parentheses."""
        text = "Company (founded 2020)"
        result = remove_parenthetical_content(text)
        assert result == "Company"
    
    def test_remove_parenthetical_content_multiple_parens(self):
        """Verify remove_parenthetical_content handles multiple parentheses."""
        text = "Company (founded 2020) in City (USA)"
        result = remove_parenthetical_content(text)
        # The regex removes all content in parentheses
        # Note: The implementation removes all parenthetical content
        assert result == "Company"
    
    def test_remove_parenthetical_content_no_parens(self):
        """Verify remove_parenthetical_content preserves text without parentheses."""
        text = "Company Name"
        result = remove_parenthetical_content(text)
        assert result == "Company Name"


class TestRemoveArticles:
    """Tests for remove_articles function."""
    
    def test_remove_articles_removes_the(self):
        """Verify remove_articles removes 'the'."""
        text = "the company"
        result = remove_articles(text)
        assert result == "company"
    
    def test_remove_articles_removes_a(self):
        """Verify remove_articles removes 'a'."""
        text = "a person"
        result = remove_articles(text)
        assert result == "person"
    
    def test_remove_articles_removes_an(self):
        """Verify remove_articles removes 'an'."""
        text = "an entity"
        result = remove_articles(text)
        assert result == "entity"
    
    def test_remove_articles_case_insensitive(self):
        """Verify remove_articles is case insensitive."""
        text = "The Company"
        result = remove_articles(text)
        assert result == "Company"
    
    def test_remove_articles_preserves_no_article(self):
        """Verify remove_articles preserves text without articles."""
        text = "company"
        result = remove_articles(text)
        assert result == "company"


class TestParseExtractedTopics:
    """Tests for parse_extracted_topics function."""
    
    def test_parse_simple_topic(self):
        """Verify parsing of simple topic structure."""
        raw_text = """topic: Technology
entities:
GraphRAG|Technology
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics) == 1
        assert topics.topics[0].value == "Technology"
        assert len(topics.topics[0].entities) == 1
        assert topics.topics[0].entities[0].value == "GraphRAG"
    
    def test_parse_topic_with_proposition(self):
        """Verify parsing of topic with proposition."""
        raw_text = """topic: AI Systems
entities:
Machine Learning|Technology
proposition: Machine learning enables AI systems
Machine Learning|enables|AI systems
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics) == 1
        assert len(topics.topics[0].statements) == 1
        assert topics.topics[0].statements[0].value == "Machine learning enables AI systems"
        assert len(topics.topics[0].statements[0].facts) == 1
    
    def test_parse_multiple_topics(self):
        """Verify parsing of multiple topics."""
        raw_text = """topic: Topic 1
entities:
Entity1|Type1

topic: Topic 2
entities:
Entity2|Type2
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics) == 2
        assert topics.topics[0].value == "Topic 1"
        assert topics.topics[1].value == "Topic 2"
    
    def test_parse_entity_with_classification(self):
        """Verify parsing of entity with classification."""
        raw_text = """topic: Companies
entities:
Amazon|Organization
Microsoft|Organization
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics[0].entities) == 2
        assert topics.topics[0].entities[0].value == "Amazon"
        assert topics.topics[0].entities[0].classification == "Organization"
        assert topics.topics[0].entities[1].value == "Microsoft"
    
    def test_parse_fact_triple(self):
        """Verify parsing of fact triple (subject|predicate|object)."""
        raw_text = """topic: Relationships
entities:
Alice|Person
Bob|Person
proposition: Alice knows Bob
Alice|knows|Bob
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        statement = topics.topics[0].statements[0]
        assert len(statement.facts) == 1
        fact = statement.facts[0]
        assert fact.subject.value == "Alice"
        assert fact.predicate.value == "knows"
        assert fact.object.value == "Bob"
    
    def test_parse_handles_unparseable_lines(self):
        """Verify parsing collects unparseable lines as garbage."""
        raw_text = """topic: Test
entities:
InvalidEntity
proposition: Test statement
InvalidFact
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(garbage) > 0
        assert any("UNPARSEABLE" in g for g in garbage)
    
    def test_parse_empty_text(self):
        """Verify parsing handles empty text."""
        raw_text = ""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics) == 0
    
    def test_parse_strips_full_stop_from_topic(self):
        """Verify parsing strips full stop from topic value."""
        raw_text = """topic: Technology.
entities:
AI|Technology
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert topics.topics[0].value == "Technology"
    
    def test_parse_formats_entity_value(self):
        """Verify parsing formats entity values (removes underscores)."""
        raw_text = """topic: Test
entities:
entity_name|Type
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert topics.topics[0].entities[0].value == "entity name"
    
    def test_parse_handles_missing_object_entity(self):
        """Verify parsing handles fact when object entity not in entity list."""
        raw_text = """topic: Test
entities:
Alice|Person
proposition: Alice works at Company
Alice|works at|Company
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        statement = topics.topics[0].statements[0]
        fact = statement.facts[0]
        assert fact.subject.value == "Alice"
        assert fact.complement.value == "Company"
        assert fact.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    
    def test_parse_handles_missing_subject_entity(self):
        """Verify parsing handles fact when subject entity not in entity list."""
        raw_text = """topic: Test
entities:
Company|Organization
proposition: Person works at Company
Person|works at|Company
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        statement = topics.topics[0].statements[0]
        # When subject is not in entities, it creates local entity and adds to details
        assert len(statement.details) > 0 or len(statement.facts) > 0
    
    def test_parse_skips_empty_lines(self):
        """Verify parsing skips empty lines."""
        raw_text = """topic: Test

entities:

Entity1|Type1

"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics) == 1
        assert len(topics.topics[0].entities) == 1
    
    def test_parse_handles_unicode_content(self):
        """Verify parsing handles Unicode content."""
        raw_text = """topic: 技术
entities:
人工智能|Technology
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert topics.topics[0].value == "技术"
        assert topics.topics[0].entities[0].value == "人工智能"
    
    def test_parse_multiple_propositions_in_topic(self):
        """Verify parsing handles multiple propositions in one topic."""
        raw_text = """topic: Technology
entities:
AI|Technology
ML|Technology
proposition: AI uses ML
AI|uses|ML
proposition: ML enables AI
ML|enables|AI
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        assert len(topics.topics[0].statements) == 2
        assert topics.topics[0].statements[0].value == "AI uses ML"
        assert topics.topics[0].statements[1].value == "ML enables AI"
    
    def test_parse_removes_parenthetical_content_from_entities(self):
        """Verify parsing removes parenthetical content from entity values."""
        raw_text = """topic: Test
entities:
Company (Inc)|Organization
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        # Entity value should have parenthetical content removed
        assert "Inc" not in topics.topics[0].entities[0].value
    
    def test_parse_removes_articles_from_entities(self):
        """Verify parsing removes articles from entity values."""
        raw_text = """topic: Test
entities:
the Company|Organization
"""
        
        topics, garbage = parse_extracted_topics(raw_text)
        
        # Article should be removed
        assert topics.topics[0].entities[0].value == "Company"
