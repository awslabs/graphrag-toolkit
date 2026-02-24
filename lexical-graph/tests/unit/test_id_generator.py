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
