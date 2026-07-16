# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tenant Operations — lifecycle management for CPG tenants in Neptune."""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


async def delete_tenant(tenant_id: str, graph_store) -> int:
    """Purge ALL nodes and edges for a tenant from Neptune.

    WARNING: This deletes everything for the tenant across all domains.
    For domain-scoped deletion (preserving non-CPG data), use delete_domain().

    Args:
        tenant_id: The tenant scope to delete
        graph_store: Neptune graph store with execute_query method

    Returns:
        Number of nodes deleted (approximate — Neptune doesn't return count)
    """
    query = "MATCH (n {tenant_id: $tenant_id}) DETACH DELETE n"
    try:
        await asyncio.to_thread(graph_store.execute_query, query, {"tenant_id": tenant_id})
        logger.info(f"Tenant purged (all domains): {tenant_id}")
        return -1
    except Exception as e:
        logger.warning(f"Tenant purge failed for {tenant_id}: {e}")
        raise


async def delete_domain(tenant_id: str, domain: str, graph_store) -> int:
    """Delete only nodes with a specific domain label for a tenant.

    This preserves nodes from other domains (QA, Perf, Security, Lexical)
    while replacing the specified domain (e.g., CPG).

    The client's requirement: "When a code change triggers a CPG reload,
    the update must only delete/update CPG-labeled nodes and not touch
    other app-level data."

    Args:
        tenant_id: The tenant (app) scope
        domain: The domain label to delete (e.g., "cpg", "lexical", "qa")
        graph_store: Neptune graph store with execute_query method

    Returns:
        Number of nodes deleted (approximate)
    """
    query = "MATCH (n {tenant_id: $tenant_id, domain: $domain}) DETACH DELETE n"
    try:
        await asyncio.to_thread(
            graph_store.execute_query, query, {"tenant_id": tenant_id, "domain": domain}
        )
        logger.info(f"Domain purged: tenant={tenant_id}, domain={domain}")
        return -1
    except Exception as e:
        logger.warning(f"Domain purge failed for {tenant_id}/{domain}: {e}")
        raise


async def list_domains(tenant_id: str, graph_store) -> list[str]:
    """List all distinct domain labels for a tenant.

    Useful for inspecting what data exists before a scoped delete.

    Args:
        tenant_id: The tenant scope
        graph_store: Neptune graph store with execute_query method

    Returns:
        List of domain strings (e.g., ["cpg", "lexical", "qa"])
    """
    query = "MATCH (n {tenant_id: $tenant_id}) RETURN DISTINCT n.domain AS domain"
    try:
        result = await asyncio.to_thread(
            graph_store.execute_query, query, {"tenant_id": tenant_id}
        )
        return [r["domain"] for r in result if r.get("domain")]
    except Exception as e:
        logger.warning(f"List domains failed for {tenant_id}: {e}")
        return []
