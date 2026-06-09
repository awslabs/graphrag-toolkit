# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit_contrib.lexical_graph.storage.graph.falkordb.falkordb_graph_store import (
    FalkorDBDatabaseClient,
)


class _Target:
    def __init__(self) -> None:
        self.create_calls = 0

    def _rewrite_query(self, statement: str) -> str:
        return statement.replace("`__Entity__`", "`__Entity__tenant1.c63ad0__`")

    def execute_query_with_retry(self, query: str, parameters: dict) -> list[dict]:
        if query == "CALL db.indexes()":
            return [{"label": "__Entity__tenant1.c63ad0__", "properties": ["entityId"]}]
        self.create_calls += 1
        return []


def _new_client() -> FalkorDBDatabaseClient:
    return FalkorDBDatabaseClient.__new__(FalkorDBDatabaseClient)


def test_statement_spec_parses_create_index_statement() -> None:
    client = _new_client()
    spec = client._statement_spec("CREATE INDEX FOR (n:`__Entity__tenant1.c63ad0__`) ON (n.entityId)")
    assert spec == ("__Entity__tenant1.c63ad0__", "entityId")


def test_row_label_preserves_tenant_scoped_label() -> None:
    client = _new_client()
    label = client._row_label({"label": "__Entity__tenant1.c63ad0__"})
    assert label == "__Entity__tenant1.c63ad0__"


def test_init_skips_create_when_index_already_exists() -> None:
    client = _new_client()
    target = _Target()

    client._index_statements = lambda: ("CREATE INDEX FOR (n:`__Entity__`) ON (n.entityId)",)  # type: ignore[method-assign]
    client.init(target)

    assert target.create_calls == 0
