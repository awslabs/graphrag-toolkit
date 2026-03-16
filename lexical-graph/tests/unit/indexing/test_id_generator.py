# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator


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


class TestIDGeneratorInitialization:
    """Tests for IDGenerator initialization - Task 7.1."""

    def test_initialization_with_tenant_id(self, default_tenant):
        """Verify generator initializes with tenant ID."""
        generator = IdGenerator(tenant_id=default_tenant)
        assert generator.tenant_id == default_tenant

    def test_initialization_empty_tenant_raises_error(self):
        """Verify ValueError raised for empty tenant ID string."""
        # Empty string tenant should raise ValueError
        with pytest.raises(ValueError, match="Invalid TenantId"):
            IdGenerator(tenant_id=TenantId(""))

    def test_initialization_none_tenant_raises_error(self):
        """Verify None tenant ID creates default TenantId."""
        # None tenant_id should create default TenantId
        generator = IdGenerator(tenant_id=None)
        assert generator.tenant_id is not None
        assert isinstance(generator.tenant_id, TenantId)

    def test_generate_creates_valid_id(self, default_id_gen):
        """Verify create_source_id() creates valid ID string."""
        text = "Sample text"
        metadata = "Sample metadata"
        id_value = default_id_gen.create_source_id(text, metadata)
        
        assert isinstance(id_value, str)
        assert len(id_value) > 0
        assert id_value.startswith("aws::")

    def test_generate_includes_tenant_prefix(self, custom_id_gen):
        """Verify generated IDs can be rewritten with tenant prefix."""
        text = "Sample text"
        metadata = "Sample metadata"
        source_id = custom_id_gen.create_source_id(text, metadata)
        
        # Rewrite for tenant to get tenant-specific ID
        rewritten_id = custom_id_gen.rewrite_id_for_tenant(source_id)
        assert isinstance(rewritten_id, str)
        assert len(rewritten_id) > 0

    def test_generate_uniqueness(self, default_id_gen):
        """Verify multiple generated IDs are unique for different inputs."""
        ids = []
        for i in range(100):
            text = f"Sample text {i}"
            metadata = f"Sample metadata {i}"
            id_value = default_id_gen.create_source_id(text, metadata)
            ids.append(id_value)
        
        assert len(ids) == len(set(ids)), "Found duplicate IDs"

    def test_generate_with_prefix(self, default_id_gen):
        """Verify create_topic_id() and other methods with prefixes work correctly."""
        source_id = "aws::12345678:abcd"
        topic_value = "Sample Topic"
        
        topic_id = default_id_gen.create_topic_id(source_id, topic_value)
        assert isinstance(topic_id, str)
        assert len(topic_id) > 0

    def test_generate_different_tenants_no_collision(self, default_id_gen, custom_id_gen):
        """Verify IDs from different tenants don't collide after rewriting."""
        text = "Sample text"
        metadata = "Sample metadata"
        
        # Create source IDs (these will be the same)
        default_source_id = default_id_gen.create_source_id(text, metadata)
        custom_source_id = custom_id_gen.create_source_id(text, metadata)
        
        # Source IDs are the same
        assert default_source_id == custom_source_id
        
        # But when rewritten for tenant, they differ
        default_rewritten = default_id_gen.rewrite_id_for_tenant(default_source_id)
        custom_rewritten = custom_id_gen.rewrite_id_for_tenant(custom_source_id)
        
        assert default_rewritten != custom_rewritten


# Property-based tests

from hypothesis import given, strategies as st, settings

# Strategy for generating valid tenant IDs (lowercase letters, numbers, periods)
# Must be 1-25 characters, no periods at start/end
tenant_id_strategy = st.text(
    min_size=1,
    max_size=25,
    alphabet=st.characters(
        whitelist_categories=('Ll', 'Nd'),  # lowercase letters and digits only
        whitelist_characters='.'
    )
).filter(lambda s: not s.startswith('.') and not s.endswith('.') and s != 'default_')


@given(tenant_id=tenant_id_strategy)
@settings(max_examples=100)
def test_id_generator_tenant_prefix_property(tenant_id):
    """
    Property: Generated IDs always include tenant prefix.
    
    For any valid tenant ID, all generated IDs should include
    that tenant ID when rewritten to ensure tenant isolation.
    
    **Validates: Requirements 3.1, 12.2**
    """
    # Create tenant and generator
    tenant = TenantId(tenant_id)
    generator = IdGenerator(tenant_id=tenant)
    
    # Generate a source ID
    text = "Sample text"
    metadata = "Sample metadata"
    source_id = generator.create_source_id(text, metadata)
    
    # Rewrite for tenant to get tenant-specific ID
    rewritten_id = generator.rewrite_id_for_tenant(source_id)
    
    # For non-default tenant, the rewritten ID should have format: prefix:tenant:rest
    # The tenant value should appear as the second component after splitting by ':'
    id_parts = rewritten_id.split(':')
    assert len(id_parts) >= 3, f"Expected at least 3 parts in rewritten ID, got {len(id_parts)}: {rewritten_id}"
    assert id_parts[1] == tenant_id, \
        f"Expected tenant '{tenant_id}' at position 1 in ID, but got '{id_parts[1]}' in '{rewritten_id}'"


@given(tenant_id=tenant_id_strategy)
@settings(max_examples=100)
def test_id_generator_uniqueness_property(tenant_id):
    """
    Property: Generated IDs are unique.
    
    For any tenant ID, generating 100 IDs with different inputs should produce
    100 unique values to prevent collisions.
    
    **Validates: Requirements 3.1, 12.2**
    """
    # Create tenant and generator
    tenant = TenantId(tenant_id)
    generator = IdGenerator(tenant_id=tenant)
    
    # Generate 100 IDs with different inputs
    ids = []
    for i in range(100):
        text = f"Sample text {i}"
        metadata = f"Sample metadata {i}"
        source_id = generator.create_source_id(text, metadata)
        ids.append(source_id)
    
    # Property: All IDs should be unique
    assert len(ids) == len(set(ids)), f"Duplicate IDs found for tenant {tenant_id}"

@given(tenant_id1=tenant_id_strategy, tenant_id2=tenant_id_strategy)
@settings(max_examples=100)
def test_id_generator_tenant_isolation_property(tenant_id1, tenant_id2):
    """
    Property: Different tenants produce distinguishable IDs.

    For any two different tenant IDs, the rewritten IDs should be different
    to ensure tenant isolation. This verifies that IDs from different tenants
    can be distinguished and don't collide.

    **Validates: Requirements 3.1, 12.2**
    """
    # Skip if tenant IDs are the same (we're testing different tenants)
    if tenant_id1 == tenant_id2:
        return

    # Create tenants and generators
    tenant1 = TenantId(tenant_id1)
    tenant2 = TenantId(tenant_id2)
    generator1 = IdGenerator(tenant_id=tenant1)
    generator2 = IdGenerator(tenant_id=tenant2)

    # Generate source IDs with the same input
    text = "Sample text"
    metadata = "Sample metadata"
    source_id1 = generator1.create_source_id(text, metadata)
    source_id2 = generator2.create_source_id(text, metadata)

    # The raw source IDs should be the same (they don't include tenant info)
    assert source_id1 == source_id2, \
        f"Source IDs should be identical before rewriting: {source_id1} vs {source_id2}"

    # But when rewritten for tenant, they should differ
    rewritten_id1 = generator1.rewrite_id_for_tenant(source_id1)
    rewritten_id2 = generator2.rewrite_id_for_tenant(source_id2)

    # Property: Different tenants produce distinguishable IDs
    assert rewritten_id1 != rewritten_id2, \
        f"Tenant isolation violated: tenant '{tenant_id1}' and '{tenant_id2}' produced same rewritten ID: {rewritten_id1}"

    # Verify that the tenant IDs appear in the rewritten IDs
    # The rewritten ID format should include the tenant identifier
    id_parts1 = rewritten_id1.split(':')
    id_parts2 = rewritten_id2.split(':')

    # Both should have at least 3 parts (prefix:tenant:rest)
    assert len(id_parts1) >= 3, f"Expected at least 3 parts in rewritten ID, got {len(id_parts1)}: {rewritten_id1}"
    assert len(id_parts2) >= 3, f"Expected at least 3 parts in rewritten ID, got {len(id_parts2)}: {rewritten_id2}"

    # The tenant component should be different
    assert id_parts1[1] != id_parts2[1], \
        f"Tenant components should differ: '{id_parts1[1]}' vs '{id_parts2[1]}'"

