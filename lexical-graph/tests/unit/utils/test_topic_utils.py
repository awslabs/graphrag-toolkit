"""Tests for topic_utils.py — string normalisation helpers and the LLM output parser.

String normalisation pipeline
------------------------------
  format_value             : replaces underscores with spaces (tested in test_format_helpers.py)
  remove_parenthetical_content : strips everything between the first '(' and last ')' on a line
  remove_articles          : strips leading English articles ('a ', 'an ', 'the ') case-insensitively
  clean                    : composed pipeline — format_value -> remove_parenthetical_content -> remove_articles

parse_extracted_topics
-----------------------
Parses the multi-section text format produced by the LLM topic-extraction prompt:

  topic: <name>
  entities:
  <value>|<classification>
  ...
  proposition: <statement text>
  <subject>|<predicate>|<object>
  ...

Returns (TopicCollection, garbage_lines).

Key behaviours tested here
---------------------------
- Empty topics (no entities or statements) are excluded from the result.
- Duplicate entities within a topic are deduplicated by their cleaned value.
- A relationship line whose subject is not in the entity dict -> both subject and
  complement get LOCAL_ENTITY_CLASSIFICATION; the line is also appended to details.
- A relationship line whose object is not in the entity dict -> complement gets
  LOCAL_ENTITY_CLASSIFICATION while the subject retains its known entity.
- An entity line without exactly one '|' separator -> appended to garbage.
- A relationship segment that does not have exactly three '|'-delimited parts but
  has non-empty text -> appended to the current statement's details (not garbage).
- A topic: line value that contains extra colons loses those colons because the code
  joins split segments with '' (empty string). This is a known limitation.
- Lines outside any recognised state -> appended to garbage.
"""

import pytest

from graphrag_toolkit.lexical_graph.indexing.utils.topic_utils import (
    remove_parenthetical_content,
    remove_articles,
    clean,
    parse_extracted_topics,
)
from graphrag_toolkit.lexical_graph.indexing.constants import (
    LOCAL_ENTITY_CLASSIFICATION,
    DEFAULT_TOPIC,
)


# ---------------------------------------------------------------------------
# remove_parenthetical_content
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("input_val,expected", [
    ("Amazon (company)", "Amazon"),          # single parenthetical at end
    ("A (B) C (D) E", "A E"),               # greedy: first '(' to last ')' consumed
    ("No parens", "No parens"),             # no-op
    ("A (B (C)) D", "A D"),                 # nested parens handled by greedy match
    ("(leading) text", "text"),             # leading parenthetical stripped
    ("", ""),                               # empty string
])
def test_remove_parenthetical_content(input_val, expected):
    assert remove_parenthetical_content(input_val) == expected


def test_remove_parenthetical_content_double_space_collapsed():
    """After removing parenthetical content, double spaces are collapsed to single."""
    result = remove_parenthetical_content("word (paren) end")
    assert "  " not in result


# ---------------------------------------------------------------------------
# remove_articles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("input_val,expected", [
    ("The White House", "White House"),     # "The" removed, case preserved in remainder
    ("An example", "example"),             # "An" removed
    ("A test", "test"),                    # "A" removed
    ("Anthem", "Anthem"),                  # "An" without trailing space — no match
    ("the lower", "lower"),               # lowercase "the"
    ("THE UPPER", "UPPER"),               # uppercase "THE"
    ("a", "a"),                           # bare "a" with no trailing space — no match
    ("", ""),                             # empty string
])
def test_remove_articles(input_val, expected):
    assert remove_articles(input_val) == expected


def test_remove_articles_only_first_article_stripped():
    """Only the leading article is stripped; inner occurrences are preserved."""
    result = remove_articles("The cat and the dog")
    assert result == "cat and the dog"


# ---------------------------------------------------------------------------
# clean (composed pipeline)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("input_val,expected", [
    ("the_Amazon_(river)", "Amazon"),       # underscore->space, parens stripped, article stripped
    ("A_simple_test", "simple test"),       # underscore->space, article stripped
    ("plain", "plain"),                     # no transformations needed
])
def test_clean(input_val, expected):
    assert clean(input_val) == expected


# ---------------------------------------------------------------------------
# parse_extracted_topics — single well-formed topic
# ---------------------------------------------------------------------------


SINGLE_TOPIC_INPUT = """\
topic: Climate Change
entities:
CO2|Gas
Earth|Planet
proposition: CO2 causes warming
CO2|causes warming|Earth"""


def test_parse_single_topic_count():
    topics, garbage = parse_extracted_topics(SINGLE_TOPIC_INPUT)
    assert len(topics.topics) == 1


def test_parse_single_topic_value():
    topics, _ = parse_extracted_topics(SINGLE_TOPIC_INPUT)
    assert topics.topics[0].value == "Climate Change"


def test_parse_single_topic_entities():
    topics, _ = parse_extracted_topics(SINGLE_TOPIC_INPUT)
    assert len(topics.topics[0].entities) == 2


def test_parse_single_topic_statements():
    topics, _ = parse_extracted_topics(SINGLE_TOPIC_INPUT)
    assert len(topics.topics[0].statements) == 1


def test_parse_single_topic_facts():
    topics, _ = parse_extracted_topics(SINGLE_TOPIC_INPUT)
    assert len(topics.topics[0].statements[0].facts) == 1


def test_parse_single_topic_no_garbage():
    _, garbage = parse_extracted_topics(SINGLE_TOPIC_INPUT)
    assert not garbage


# ---------------------------------------------------------------------------
# parse_extracted_topics — multiple topics
# ---------------------------------------------------------------------------


MULTI_TOPIC_INPUT = """\
topic: Climate
entities:
CO2|Gas
proposition: CO2 warms
CO2|warms|CO2
topic: Economy
entities:
GDP|Metric
proposition: GDP grows
GDP|grows|GDP"""


def test_parse_multiple_topics_count():
    topics, _ = parse_extracted_topics(MULTI_TOPIC_INPUT)
    assert len(topics.topics) == 2


def test_parse_multiple_topics_values():
    topics, _ = parse_extracted_topics(MULTI_TOPIC_INPUT)
    assert topics.topics[0].value == "Climate"
    assert topics.topics[1].value == "Economy"


# ---------------------------------------------------------------------------
# parse_extracted_topics — empty topics excluded
# ---------------------------------------------------------------------------


EMPTY_TOPIC_INPUT = """\
topic: Empty
topic: Real
entities:
Item|Thing
proposition: Item exists
Item|exists|Item"""


def test_parse_empty_topic_excluded():
    """A topic with no entities and no statements is not added to the result."""
    topics, _ = parse_extracted_topics(EMPTY_TOPIC_INPUT)
    assert len(topics.topics) == 1
    assert topics.topics[0].value == "Real"


# ---------------------------------------------------------------------------
# parse_extracted_topics — entity deduplication
# ---------------------------------------------------------------------------


DEDUP_ENTITY_INPUT = """\
topic: Test
entities:
Amazon|Company
Amazon|Company
proposition: Amazon operates
Amazon|operates|Amazon"""


def test_parse_entity_deduplication():
    """Duplicate entity lines (same cleaned value) are stored only once."""
    topics, _ = parse_extracted_topics(DEDUP_ENTITY_INPUT)
    assert len(topics.topics[0].entities) == 1


# ---------------------------------------------------------------------------
# parse_extracted_topics — unknown subject -> both sides become local entities
# ---------------------------------------------------------------------------


UNKNOWN_SUBJECT_INPUT = """\
topic: Test
entities:
KnownEntity|Type
proposition: Something happens
UnknownSubject|does|KnownEntity"""


def test_parse_unknown_subject_subject_is_local_entity():
    """When neither subject nor object is in the entity dict, the subject gets
    LOCAL_ENTITY_CLASSIFICATION."""
    topics, _ = parse_extracted_topics(UNKNOWN_SUBJECT_INPUT)
    fact = topics.topics[0].statements[0].facts[0]
    assert fact.subject.classification == LOCAL_ENTITY_CLASSIFICATION


def test_parse_unknown_subject_complement_is_local_entity():
    """When the subject is unknown, the complement also gets LOCAL_ENTITY_CLASSIFICATION
    (the code falls into the else-branch that creates two local entities)."""
    topics, _ = parse_extracted_topics(UNKNOWN_SUBJECT_INPUT)
    fact = topics.topics[0].statements[0].facts[0]
    assert fact.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    assert fact.complement.value == "KnownEntity"


def test_parse_unknown_subject_adds_detail():
    """The else-branch also appends the raw line to statement.details."""
    topics, _ = parse_extracted_topics(UNKNOWN_SUBJECT_INPUT)
    stmt = topics.topics[0].statements[0]
    assert len(stmt.details) == 1


# ---------------------------------------------------------------------------
# parse_extracted_topics — unknown object -> complement created
# ---------------------------------------------------------------------------


UNKNOWN_OBJECT_INPUT = """\
topic: Test
entities:
KnownEntity|Type
proposition: Known does something
KnownEntity|relates to|UnknownObject"""


def test_parse_unknown_object_subject_is_known():
    """Subject is in the entity dict -> uses the known entity."""
    topics, _ = parse_extracted_topics(UNKNOWN_OBJECT_INPUT)
    fact = topics.topics[0].statements[0].facts[0]
    assert fact.subject.value == "KnownEntity"


def test_parse_unknown_object_complement_is_local_entity():
    """Object not in entity dict -> complement gets LOCAL_ENTITY_CLASSIFICATION."""
    topics, _ = parse_extracted_topics(UNKNOWN_OBJECT_INPUT)
    fact = topics.topics[0].statements[0].facts[0]
    assert fact.complement.classification == LOCAL_ENTITY_CLASSIFICATION
    assert fact.complement.value == "UnknownObject"


def test_parse_unknown_object_fact_object_is_none():
    """When the complement path is used, fact.object remains None."""
    topics, _ = parse_extracted_topics(UNKNOWN_OBJECT_INPUT)
    fact = topics.topics[0].statements[0].facts[0]
    assert fact.object is None


# ---------------------------------------------------------------------------
# parse_extracted_topics — unparseable entity line -> garbage
# ---------------------------------------------------------------------------


BAD_ENTITY_INPUT = """\
topic: Test
entities:
bad entity line without pipe
Good|Entity
proposition: Good exists
Good|exists|Good"""


def test_parse_unparseable_entity_goes_to_garbage():
    """An entity line without exactly one '|' is appended to garbage."""
    _, garbage = parse_extracted_topics(BAD_ENTITY_INPUT)
    assert any("UNPARSEABLE ENTITY" in g for g in garbage)


def test_parse_unparseable_entity_valid_entity_still_parsed():
    """The bad line does not prevent the valid entity on the next line from being parsed."""
    topics, _ = parse_extracted_topics(BAD_ENTITY_INPUT)
    assert len(topics.topics[0].entities) == 1


# ---------------------------------------------------------------------------
# parse_extracted_topics — unparseable relationship line -> detail
# ---------------------------------------------------------------------------


BAD_RELATIONSHIP_INPUT = """\
topic: Test
entities:
A|Type
proposition: Some statement
only two segments here"""


def test_parse_unparseable_relationship_becomes_detail():
    """A relationship line without three '|'-delimited segments but with non-empty
    text is appended to statement.details (not to garbage)."""
    topics, _ = parse_extracted_topics(BAD_RELATIONSHIP_INPUT)
    stmt = topics.topics[0].statements[0]
    assert len(stmt.details) == 1


def test_parse_unparseable_relationship_not_in_garbage():
    """Non-empty single-segment lines in relationship-extraction state go to details,
    NOT to garbage."""
    _, garbage = parse_extracted_topics(BAD_RELATIONSHIP_INPUT)
    assert not any("UNPARSEABLE" in g for g in garbage)


# ---------------------------------------------------------------------------
# parse_extracted_topics — colon in topic value
# ---------------------------------------------------------------------------


COLON_TOPIC_INPUT = """\
topic: Key: Value Pair
entities:
Item|Thing
proposition: Item exists
Item|exists|Item"""


def test_parse_colon_in_topic_value():
    """The topic parser splits on ':' and joins with '' (empty string), so any colons
    inside the topic value after the first ':' are lost.

    Known limitation: 'topic: Key: Value Pair' is stored as 'Key Value Pair'
    (not 'Key: Value Pair').
    """
    topics, _ = parse_extracted_topics(COLON_TOPIC_INPUT)
    # The code does ''.join(line.split(':')[1:]).strip(), which drops the inner colon.
    assert topics.topics[0].value == "Key Value Pair"


# ---------------------------------------------------------------------------
# parse_extracted_topics — empty input
# ---------------------------------------------------------------------------


def test_parse_empty_input():
    topics, garbage = parse_extracted_topics("")
    assert len(topics.topics) == 0
    assert len(garbage) == 0


# ---------------------------------------------------------------------------
# parse_extracted_topics — only garbage lines
# ---------------------------------------------------------------------------


def test_parse_only_garbage():
    """Lines outside any recognised state (no topic/entities/proposition header yet)
    are classified as garbage."""
    topics, garbage = parse_extracted_topics("random nonsense\nmore junk")
    assert len(topics.topics) == 0
    assert len(garbage) == 2


# ---------------------------------------------------------------------------
# parse_extracted_topics — entities/proposition before any topic: header
# ---------------------------------------------------------------------------


PROPOSITION_BEFORE_TOPIC = """\
entities:
Item|Thing
proposition: Item exists
Item|exists|Item"""


def test_parse_proposition_before_topic_uses_default():
    """When no 'topic:' line precedes the content, the implicit default topic
    ('context') is used."""
    topics, _ = parse_extracted_topics(PROPOSITION_BEFORE_TOPIC)
    assert len(topics.topics) == 1
    assert topics.topics[0].value == DEFAULT_TOPIC
