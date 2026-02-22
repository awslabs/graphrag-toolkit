"""Tests for TenantId and to_tenant_id (tenant_id.py).

TenantId wraps a tenant name string with strict validation and formatting helpers.
The default tenant (value=None) uses simpler formatting than a custom tenant,
allowing the same graph store to host multiple isolated tenants without collision.

Validation rules
----------------
  - Length 1–25 characters
  - All lowercase
  - Alphanumeric characters and periods allowed (not at start or end)
  - The literal string "default_" (case-insensitive) is treated as the default
    tenant and stored as value=None instead of being validated
"""

import pytest

from graphrag_toolkit.lexical_graph.tenant_id import TenantId, to_tenant_id


# ---------------------------------------------------------------------------
# __init__ validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["acme", "tenant1", "a.b.c", "x"])
def test_tenant_id_valid(value):
    t = TenantId(value)
    assert t.value == value


@pytest.mark.parametrize("value", [
    "UPPER",        # uppercase letters not allowed
    "a" * 26,       # 26 chars > 25-char limit
    ".start",       # leading period not allowed
    "end.",         # trailing period not allowed
    "has space",    # spaces not allowed
    "special!",     # special characters not allowed
])
def test_tenant_id_invalid_raises(value):
    with pytest.raises(ValueError, match="Invalid TenantId"):
        TenantId(value)


def test_tenant_id_default_none():
    """No argument -> default tenant with value=None."""
    assert TenantId().value is None


def test_tenant_id_default_string_normalized():
    """The sentinel string 'default_' normalizes to the default tenant (value=None)."""
    assert TenantId("default_").value is None


def test_tenant_id_25_chars_valid():
    """Exactly 25 lowercase characters is the maximum valid length."""
    value = "a" * 25
    assert TenantId(value).value == value


# ---------------------------------------------------------------------------
# is_default_tenant
# ---------------------------------------------------------------------------


def test_is_default_tenant_true(default_tenant):
    assert default_tenant.is_default_tenant() is True


def test_is_default_tenant_false(custom_tenant):
    assert custom_tenant.is_default_tenant() is False


# ---------------------------------------------------------------------------
# format_label
# ---------------------------------------------------------------------------


def test_format_label_default(default_tenant):
    assert default_tenant.format_label("Topic") == "`Topic`"


def test_format_label_custom(custom_tenant):
    assert custom_tenant.format_label("Topic") == "`Topicacme__`"


# ---------------------------------------------------------------------------
# format_index_name
# ---------------------------------------------------------------------------


def test_format_index_name_default(default_tenant):
    assert default_tenant.format_index_name("my_index") == "my_index"


def test_format_index_name_custom(custom_tenant):
    assert custom_tenant.format_index_name("my_index") == "my_index_acme"


# ---------------------------------------------------------------------------
# format_hashable
# ---------------------------------------------------------------------------


def test_format_hashable_default(default_tenant):
    """Default tenant: hashable string is returned as-is."""
    assert default_tenant.format_hashable("topic::foo") == "topic::foo"


def test_format_hashable_custom(custom_tenant):
    """Custom tenant: tenant name is prepended with '::' separator."""
    assert custom_tenant.format_hashable("topic::foo") == "acme::topic::foo"


# ---------------------------------------------------------------------------
# format_id
# ---------------------------------------------------------------------------


def test_format_id_default(default_tenant):
    """Default tenant: uses '::' double-colon separator."""
    assert default_tenant.format_id("aws", "abc123") == "aws::abc123"


def test_format_id_custom(custom_tenant):
    """Custom tenant: uses single-colon separators with tenant name in the middle."""
    assert custom_tenant.format_id("aws", "abc123") == "aws:acme:abc123"


# ---------------------------------------------------------------------------
# rewrite_id
# ---------------------------------------------------------------------------


def test_rewrite_id_default(default_tenant):
    """Default tenant: ID is returned unchanged."""
    assert default_tenant.rewrite_id("aws::abc:def") == "aws::abc:def"


def test_rewrite_id_custom(custom_tenant):
    """Custom tenant: tenant name is inserted after the prefix segment.

    'aws::abc:def' splits as ['aws', '', 'abc', 'def']. Rejoining with the
    tenant name gives 'aws:acme:abc:def'.
    """
    assert custom_tenant.rewrite_id("aws::abc:def") == "aws:acme:abc:def"


# ---------------------------------------------------------------------------
# to_tenant_id
# ---------------------------------------------------------------------------


def test_to_tenant_id_none():
    """None -> returns the global DEFAULT_TENANT_ID (default tenant)."""
    result = to_tenant_id(None)
    assert result.is_default_tenant()


def test_to_tenant_id_passthrough():
    """An existing TenantId instance is returned as-is (identity check)."""
    t = TenantId("acme")
    assert to_tenant_id(t) is t


def test_to_tenant_id_from_string():
    """A plain string is wrapped in a new TenantId."""
    result = to_tenant_id("acme")
    assert isinstance(result, TenantId)
    assert result.value == "acme"
