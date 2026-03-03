"""Tests for IdGenerator (id_generator.py).

IdGenerator produces stable, deterministic IDs for every node type in the graph:
sources, chunks, entities, topics, statements, and facts. All IDs are MD5 hashes
of normalized strings so the same real-world content always maps to the same node,
enabling deduplication without a central registry.

ID hierarchy
------------
  source_id  = aws::<hash(text)[:8]>:<hash(metadata)[:4]>
  chunk_id   = <source_id>:<hash(text + metadata)[:8]>   ← child of source
  topic_id   = hash("topic::<source_id>::<topic>")        ← child of source
  statement_id = hash("statement::<topic_id>::<stmt>")    ← child of topic
  fact_id    = hash("fact::<value>")                      ← global (no parent)
  entity_id  = hash("entity::<value>[:: <class>]")        ← global

Tenant isolation
----------------
Source IDs are hashed from raw content with no tenant prefix — they are NOT
tenant-scoped by design. Tenant context is injected later via rewrite_id_for_tenant.
All other node types go through format_hashable, which prepends the tenant prefix
before hashing, so they are fully isolated between tenants.

Known issues documented here
-----------------------------
- create_chunk_id has no delimiter between text and metadata before hashing,
  so boundary-shifted inputs that concatenate to the same string will collide.
- IdGenerator.__init__ uses `value or config_default` semantics, which means
  explicitly passing include_classification_in_entity_id=False is silently ignored
  because False is falsy. The workaround is to set the field directly after construction.
"""

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re

import pytest
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator


# --- create_source_id ---


def test_create_source_id_format(default_id_gen):
    """Source ID matches the expected 'aws::<8 hex>:<4 hex>' pattern.

    The first segment is the first 8 chars of hash(text), the second is
    the first 4 chars of hash(metadata). Fixed lengths keep IDs compact.
    """
    result = default_id_gen.create_source_id("text", "meta")
    assert re.match(r"aws::[a-f0-9]{8}:[a-f0-9]{4}$", result)


def test_create_source_id_deterministic(default_id_gen):
    """Same text + metadata always produces the same source ID."""
    assert default_id_gen.create_source_id("text", "meta") == default_id_gen.create_source_id("text", "meta")


def test_create_source_id_different_text(default_id_gen):
    """Changing the text changes the source ID (first hash segment)."""
    assert default_id_gen.create_source_id("text_a", "meta") != default_id_gen.create_source_id("text_b", "meta")


def test_create_source_id_different_metadata(default_id_gen):
    """Changing the metadata changes the source ID (second hash segment)."""
    assert default_id_gen.create_source_id("text", "meta_a") != default_id_gen.create_source_id("text", "meta_b")


# --- create_chunk_id ---


def test_create_chunk_id_hierarchical(default_id_gen):
    """Chunk ID starts with its parent source ID, encoding the parent-child relationship."""
    source_id = default_id_gen.create_source_id("text", "meta")
    chunk_id = default_id_gen.create_chunk_id(source_id, "chunk text", "chunk meta")
    assert chunk_id.startswith(source_id + ":")


def test_create_chunk_id_deterministic(default_id_gen):
    """Same inputs always produce the same chunk ID."""
    source_id = default_id_gen.create_source_id("text", "meta")
    id1 = default_id_gen.create_chunk_id(source_id, "chunk", "meta")
    id2 = default_id_gen.create_chunk_id(source_id, "chunk", "meta")
    assert id1 == id2


def test_create_chunk_id_concatenation_boundary(default_id_gen):
    """Known behavior: text and metadata are concatenated with no delimiter before
    hashing. Inputs whose concatenations are identical — e.g. ('foo', 'bar') and
    ('foob', 'ar') both produce 'foobar' — hash to the same value and collide.

    This is a documented limitation, not a bug we are fixing here.
    """
    source_id = default_id_gen.create_source_id("text", "meta")
    id1 = default_id_gen.create_chunk_id(source_id, "foo", "bar")
    id2 = default_id_gen.create_chunk_id(source_id, "foob", "ar")
    assert id1 == id2  # both hash "foobar"


# --- create_entity_id ---


def test_create_entity_id_classification_matters_when_enabled(default_id_gen):
    """With classification enabled, 'Amazon/Company' and 'Amazon/River' are distinct nodes."""
    assert default_id_gen.create_entity_id("Amazon", "Company") != default_id_gen.create_entity_id("Amazon", "River")


def test_create_entity_id_classification_ignored_when_disabled(default_tenant):
    """With classification disabled, entity identity depends only on value — so
    'Amazon/Company' and 'Amazon/River' collapse to the same node.
    """
    gen = IdGenerator(tenant_id=default_tenant, include_classification_in_entity_id=False)
    assert gen.create_entity_id("Amazon", "Company") == gen.create_entity_id("Amazon", "River")


def test_create_entity_id_case_insensitive(default_id_gen):
    """Entity values are lowercased before hashing, so 'Amazon' and 'amazon' are the same node."""
    assert default_id_gen.create_entity_id("Amazon", "Company") == default_id_gen.create_entity_id("amazon", "Company")


def test_create_entity_id_space_normalization(default_id_gen):
    """Spaces are replaced with underscores before hashing, so 'New York' and 'new york' collide."""
    assert default_id_gen.create_entity_id("New York", "Location") == default_id_gen.create_entity_id("new york", "Location")


# --- create_topic_id ---


def test_create_topic_id_source_scoping(default_id_gen):
    """Topics are scoped to their source: the same topic name under two different
    sources produces two different topic IDs, avoiding cross-document contamination.
    """
    assert default_id_gen.create_topic_id("source_a", "Climate") != default_id_gen.create_topic_id("source_b", "Climate")


def test_create_topic_id_deterministic(default_id_gen):
    """Same source + topic always produces the same topic ID."""
    assert default_id_gen.create_topic_id("source", "Climate") == default_id_gen.create_topic_id("source", "Climate")


# --- create_statement_id ---


def test_create_statement_id_topic_scoping(default_id_gen):
    """Statements are scoped to their parent topic: the same statement text under two
    different topics produces different statement IDs.
    """
    id1 = default_id_gen.create_statement_id("topic_a", "CO2 warms Earth")
    id2 = default_id_gen.create_statement_id("topic_b", "CO2 warms Earth")
    assert id1 != id2


# --- create_fact_id ---


def test_create_fact_id_global_dedup(default_id_gen):
    """Facts are globally deduplicated: the same fact text always maps to the same ID
    regardless of which document or topic it appeared in.
    """
    assert default_id_gen.create_fact_id("CO2 causes warming") == default_id_gen.create_fact_id("CO2 causes warming")


def test_create_fact_id_different_values(default_id_gen):
    """Different fact text produces different fact IDs."""
    assert default_id_gen.create_fact_id("fact A") != default_id_gen.create_fact_id("fact B")


# --- rewrite_id_for_tenant ---


def test_rewrite_id_for_tenant_default_passthrough(default_id_gen):
    """For the default tenant the ID is returned unchanged — no prefix is inserted."""
    original = "aws::abc:def"
    assert default_id_gen.rewrite_id_for_tenant(original) == original


def test_rewrite_id_for_tenant_custom_insertion(custom_id_gen):
    """For a custom tenant the tenant name is inserted after the first segment.

    'aws::abc:def' becomes 'aws:acme:abc:def' — the '::' separator is replaced
    by ':acme:' to slot the tenant between the prefix and the ID body.
    """
    assert custom_id_gen.rewrite_id_for_tenant("aws::abc:def") == "aws:acme:abc:def"


# --- Tenant isolation ---


def test_source_id_tenant_isolation(default_id_gen, custom_id_gen):
    """Source IDs are NOT tenant-scoped by design.

    create_source_id hashes raw content directly (no format_hashable call),
    so the same document produces the same source ID across all tenants.
    Tenant context is applied separately via rewrite_id_for_tenant.
    """
    id1 = default_id_gen.create_source_id("text", "meta")
    id2 = custom_id_gen.create_source_id("text", "meta")
    assert id1 == id2


def test_entity_id_tenant_isolation(default_id_gen, custom_id_gen):
    """Entity IDs are tenant-scoped: the same entity produces different IDs per tenant,
    preventing entities from different tenants from merging in a shared graph store.
    """
    id1 = default_id_gen.create_entity_id("Amazon", "Company")
    id2 = custom_id_gen.create_entity_id("Amazon", "Company")
    assert id1 != id2


def test_topic_id_tenant_isolation(default_id_gen, custom_id_gen):
    """Topic IDs are tenant-scoped."""
    id1 = default_id_gen.create_topic_id("source", "Climate")
    id2 = custom_id_gen.create_topic_id("source", "Climate")
    assert id1 != id2


def test_fact_id_tenant_isolation(default_id_gen, custom_id_gen):
    """Fact IDs are tenant-scoped, even though facts are otherwise globally deduplicated
    within a single tenant.
    """
    id1 = default_id_gen.create_fact_id("some fact")
    id2 = custom_id_gen.create_fact_id("some fact")
    assert id1 != id2


# ============================================================================
# Tests for create_chunk_id with backward compatible mode (no delimiter)
# ============================================================================


class TestCreateChunkIdBackwardCompatible:
    """Tests for IdGenerator.create_chunk_id method in backward compatible mode (no delimiter)."""

    def test_create_chunk_id_basic(self, default_id_gen):
        """Test basic chunk ID creation."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        chunk_id = default_id_gen.create_chunk_id(source_id, text, metadata)

        assert chunk_id.startswith(source_id + ":")
        assert len(chunk_id) == len(source_id) + 1 + 8  # source_id:8_char_hash

    def test_create_chunk_id_deterministic(self, default_id_gen):
        """Test that same inputs produce same chunk ID."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen.create_chunk_id(source_id, text, metadata)
        id2 = default_id_gen.create_chunk_id(source_id, text, metadata)

        assert id1 == id2

    def test_create_chunk_id_boundary_collision_exists(self, default_id_gen):
        """
        Test that boundary collisions can occur in backward compatible mode.

        In the old behavior (without delimiter), different (text, metadata) pairs
        with same concatenation will collide. This is expected for backward compatibility.
        """
        source_id = "aws::12345678:abcd"

        # These WILL collide without a delimiter: "hello" + "world" = "helloworld"
        id1 = default_id_gen.create_chunk_id(source_id, "hello", "world")

        # This will also produce "helloworld" without delimiter
        id2 = default_id_gen.create_chunk_id(source_id, "hell", "oworld")

        # In backward compatible mode, they are the same (boundary collision exists)
        assert id1 == id2, (
            "In backward compatible mode (without delimiter), boundary collisions are expected. "
            "Enable use_chunk_id_delimiter=True for collision-resistant hashing."
        )

    def test_create_chunk_id_empty_strings(self, default_id_gen):
        """Test chunk ID creation with empty strings in backward compatible mode."""
        source_id = "aws::12345678:abcd"

        # Empty text
        id1 = default_id_gen.create_chunk_id(source_id, "", "metadata")
        assert id1.startswith(source_id + ":")

        # Empty metadata
        id2 = default_id_gen.create_chunk_id(source_id, "text", "")
        assert id2.startswith(source_id + ":")

        # In backward compatible mode, ("", "metadata") and ("", "metadata") concatenate differently
        # than ("text", ""), so they should be different
        assert id1 != id2

    def test_create_chunk_id_different_source_ids(self, default_id_gen):
        """Test that different source IDs produce different chunk IDs."""
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen.create_chunk_id("source1", text, metadata)
        id2 = default_id_gen.create_chunk_id("source2", text, metadata)

        assert id1 != id2
        assert id1.startswith("source1:")
        assert id2.startswith("source2:")


# ============================================================================
# Tests for create_chunk_id with delimiter enabled (collision-resistant mode)
# ============================================================================


class TestCreateChunkIdWithDelimiter:
    """Tests for IdGenerator.create_chunk_id method with delimiter enabled (collision-resistant mode)."""

    def test_create_chunk_id_basic(self, default_id_gen_with_delimiter):
        """Test basic chunk ID creation with delimiter."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        chunk_id = default_id_gen_with_delimiter.create_chunk_id(source_id, text, metadata)

        assert chunk_id.startswith(source_id + ":")
        assert len(chunk_id) == len(source_id) + 1 + 8  # source_id:8_char_hash

    def test_create_chunk_id_deterministic(self, default_id_gen_with_delimiter):
        """Test that same inputs produce same chunk ID."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen_with_delimiter.create_chunk_id(source_id, text, metadata)
        id2 = default_id_gen_with_delimiter.create_chunk_id(source_id, text, metadata)

        assert id1 == id2

    def test_create_chunk_id_no_boundary_collision(self, default_id_gen_with_delimiter):
        """
        Test that different (text, metadata) pairs with same concatenation don't collide.

        This is a regression test for issue #107:
        Previously, ("hello", "world") and ("hell", "oworld") would both hash
        "helloworld" and produce identical IDs. With the delimiter fix, they
        should produce different IDs.
        """
        source_id = "aws::12345678:abcd"

        # These would collide without a delimiter: "hello" + "world" = "helloworld"
        id1 = default_id_gen_with_delimiter.create_chunk_id(source_id, "hello", "world")

        # This would also produce "helloworld" without delimiter
        id2 = default_id_gen_with_delimiter.create_chunk_id(source_id, "hell", "oworld")

        # With the fix, they should be different
        assert id1 != id2, (
            "Chunk IDs should differ for ('hello', 'world') vs ('hell', 'oworld'). "
            "Boundary collision detected - this means the delimiter fix is not working."
        )

    def test_create_chunk_id_boundary_collision_more_cases(self, default_id_gen_with_delimiter):
        """Test additional boundary collision cases with delimiter enabled."""
        source_id = "aws::12345678:abcd"

        # Test various boundary shift patterns
        test_cases = [
            (("abc", "def"), ("ab", "cdef")),
            (("abc", "def"), ("abcd", "ef")),
            (("", "abcdef"), ("abc", "def")),
            (("abcdef", ""), ("abc", "def")),
            (("a", "bcdef"), ("abcde", "f")),
        ]

        for (text1, meta1), (text2, meta2) in test_cases:
            id1 = default_id_gen_with_delimiter.create_chunk_id(source_id, text1, meta1)
            id2 = default_id_gen_with_delimiter.create_chunk_id(source_id, text2, meta2)
            assert id1 != id2, (
                f"Boundary collision: ({text1!r}, {meta1!r}) vs ({text2!r}, {meta2!r})"
            )

    def test_create_chunk_id_empty_strings(self, default_id_gen_with_delimiter):
        """Test chunk ID creation with empty strings with delimiter."""
        source_id = "aws::12345678:abcd"

        # Empty text
        id1 = default_id_gen_with_delimiter.create_chunk_id(source_id, "", "metadata")
        assert id1.startswith(source_id + ":")

        # Empty metadata
        id2 = default_id_gen_with_delimiter.create_chunk_id(source_id, "text", "")
        assert id2.startswith(source_id + ":")

        # Both should be different (delimiter separates them)
        assert id1 != id2

    def test_create_chunk_id_different_source_ids(self, default_id_gen_with_delimiter):
        """Test that different source IDs produce different chunk IDs."""
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen_with_delimiter.create_chunk_id("source1", text, metadata)
        id2 = default_id_gen_with_delimiter.create_chunk_id("source2", text, metadata)

        assert id1 != id2
        assert id1.startswith("source1:")
        assert id2.startswith("source2:")


# ============================================================================
# Tests comparing behavior between delimiter and non-delimiter modes
# ============================================================================


class TestDelimiterModeComparison:
    """Tests comparing behavior between delimiter and non-delimiter modes."""

    def test_same_inputs_different_modes_different_ids(self, default_id_gen, default_id_gen_with_delimiter):
        """
        Test that the same inputs produce different IDs in different modes.

        This ensures that enabling the delimiter actually changes the hash output,
        which is necessary for fixing boundary collisions.
        """
        source_id = "aws::12345678:abcd"
        text = "hello"
        metadata = "world"

        id_without_delimiter = default_id_gen.create_chunk_id(source_id, text, metadata)
        id_with_delimiter = default_id_gen_with_delimiter.create_chunk_id(source_id, text, metadata)

        # Different modes should produce different IDs for the same input
        assert id_without_delimiter != id_with_delimiter, (
            "Enabling delimiter should change the hash output for the same input. "
            "This difference is expected and ensures collision-resistant hashing."
        )


# ============================================================================
# Additional tests for create_source_id
# ============================================================================


class TestCreateSourceId:
    """Tests for IdGenerator.create_source_id method."""

    def test_create_source_id_format(self, default_id_gen):
        """Test source ID format."""
        text = "Hello world"
        metadata = "test_metadata"

        source_id = default_id_gen.create_source_id(text, metadata)

        assert source_id.startswith("aws::")
        parts = source_id.split(":")
        assert len(parts) == 4  # "aws", "", "hash1", "hash2"

    def test_create_source_id_deterministic(self, default_id_gen):
        """Test that same inputs produce same source ID."""
        text = "Hello world"
        metadata = "test_metadata"

        id1 = default_id_gen.create_source_id(text, metadata)
        id2 = default_id_gen.create_source_id(text, metadata)

        assert id1 == id2


# ============================================================================
# Additional tests for tenant isolation
# ============================================================================


class TestTenantIsolation:
    """Tests for tenant isolation in ID generation."""

    def test_chunk_id_tenant_isolation(self, default_id_gen, custom_id_gen):
        """Test that chunk IDs from different tenants can be distinguished via rewrite."""
        source_id = "aws::12345678:abcd"
        text = "Hello world"
        metadata = "test_metadata"

        # Chunk IDs themselves are the same (tenant affects rewrite_id_for_tenant)
        default_chunk_id = default_id_gen.create_chunk_id(source_id, text, metadata)
        custom_chunk_id = custom_id_gen.create_chunk_id(source_id, text, metadata)

        # The raw chunk IDs are the same
        assert default_chunk_id == custom_chunk_id

        # But when rewritten for tenant, they differ
        default_rewritten = default_id_gen.rewrite_id_for_tenant(default_chunk_id)
        custom_rewritten = custom_id_gen.rewrite_id_for_tenant(custom_chunk_id)

        assert default_rewritten != custom_rewritten
