# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify lexical-graph binds filter values, metadata keys, and id lists at the
PGVector store SQL sinks against a live Postgres+pgvector engine.

The PGIndex sinks (top_k filters, delete_embeddings, update_versioning) build SQL
from user-supplied filter values, metadata keys, and node ids. Before the fix these
were interpolated; a crafted value or key could break out of the WHERE clause. The
test drives the real index from VectorStoreFactory with injection payloads and
confirms they stay inert, plus a red-state proof that the pre-fix interpolation
would break out on the same engine.

Requires a Postgres-backed VECTOR_STORE (postgres:// or postgresql://), i.e. a
'*-postgresql' env-type. Skips on any other vector store.
"""

import os
import json
import unittest
from typing import Dict, Any

import numpy as np

import graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes as pvi
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from llama_index.core.schema import QueryBundle
from llama_index.core.vector_stores.types import (
    FilterCondition, FilterOperator, MetadataFilter, MetadataFilters,
)

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler


VALUE_PAYLOAD = "' OR '1'='1"
KEY_PAYLOAD = "category' OR '1'='1"
ID_PAYLOAD = "x') OR '1'='1"

VERSIONING_TIMESTAMP = 999

# Two seed rows with known metadata. A working filter resolves 'keep1' only; an
# injected filter that breaks out of the WHERE clause would return both.
SEED_ROWS = [
    ('pgvec-keep1', {'source': {'metadata': {'category': 'tech', 'count': 5}, 'versioning': {}}}),
    ('pgvec-keep2', {'source': {'metadata': {'category': 'science', 'count': 1}, 'versioning': {}}}),
]
SEED_IDS = [cid for cid, _ in SEED_ROWS]


class LexicalGraphPGVectorInjectionSafety(IntegrationTestBase):
    """Drive the real PGVector sinks with SQL-injection payloads and confirm the
    bound parameters keep them inert: injected filter values and metadata keys
    match nothing, injected ids delete/update nothing, plus a red-state proof
    that the pre-fix interpolation would break out on the same engine.
    """

    @property
    def description(self):
        return 'PGVector store binds filter values, keys, and id lists at the SQL sinks'

    def _is_pgvector(self):
        vector_store = os.environ.get('VECTOR_STORE', '')
        return vector_store.startswith('postgres://') or vector_store.startswith('postgresql://')

    def _embedding(self, index, i):
        """A distinct unit vector of the index's dimensionality for seed row i."""
        vec = [0.0] * index.dimensions
        vec[i % index.dimensions] = 1.0
        return np.array(vec)

    def _reset(self, index):
        """Truncate the chunk table and reseed the two known rows."""
        dbconn = index._get_connection()
        cur = dbconn.cursor()
        try:
            cur.execute(f'TRUNCATE {index.schema_name}.{index.underlying_index_name()};')  # nosec B608 - internal identifiers, not user input
            for i, (cid, meta) in enumerate(SEED_ROWS):
                cur.execute(
                    f'INSERT INTO {index.schema_name}.{index.underlying_index_name()} '  # nosec B608 - internal identifiers, not user input
                    f'({index.index_name}Id, value, metadata, embedding) VALUES (%s, %s, %s, %s);',
                    (cid, cid, json.dumps(meta), self._embedding(index, i)),
                )
        finally:
            cur.close()
            dbconn.close()

    def _row_count(self, index, where=''):
        dbconn = index._get_connection()
        cur = dbconn.cursor()
        try:
            cur.execute(f'SELECT count(*) FROM {index.schema_name}.{index.underlying_index_name()} {where};')  # nosec B608 - internal identifiers plus a literal WHERE fragment, not user input
            return cur.fetchone()[0]
        finally:
            cur.close()
            dbconn.close()

    def _eq(self, key, value):
        return FilterConfig(source_filters=MetadataFilters(
            filters=[MetadataFilter(key=key, value=value, operator=FilterOperator.EQ)],
            condition=FilterCondition.AND))

    def _top_k_categories(self, index, filter_config):
        """Run top_k with a fixed query embedding and return the matched categories."""
        bundle = QueryBundle(query_str='q', embedding=list(self._embedding(index, 0)))
        original = pvi.to_embedded_query
        pvi.to_embedded_query = lambda qb, model: bundle
        try:
            results = index.top_k(bundle, top_k=10, filter_config=filter_config)
        finally:
            pvi.to_embedded_query = original
        return [r['source']['metadata']['category'] for r in results]

    def _redstate_bypass_count(self, index):
        """Run the pre-fix interpolated filter value directly, proving the payload
        breaks out on this engine when it is not bound."""
        dbconn = index._get_connection()
        cur = dbconn.cursor()
        try:
            old_where = "(((metadata->'source'->'metadata'->>'category')::text = '%s'))" % VALUE_PAYLOAD
            cur.execute(
                f"SELECT {index.index_name}Id FROM {index.schema_name}.{index.underlying_index_name()} WHERE {old_where};"  # nosec B608 - deliberate red-state demonstration of the pre-fix interpolation
            )
            return len(cur.fetchall())
        finally:
            cur.close()
            dbconn.close()

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        if not self._is_pgvector():
            handler.skip()
            return

        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:

            index = vector_store.get_index('chunk')
            index.enable_for_versioning(ids=SEED_IDS)

            # --- Legitimate filter resolves the matching row ---
            self._reset(index)
            legit_categories = self._top_k_categories(index, self._eq('category', 'tech'))

            # --- Injected filter value must match nothing ---
            self._reset(index)
            injected_value_categories = self._top_k_categories(index, self._eq('category', VALUE_PAYLOAD))

            # --- Injected metadata key must resolve to a missing property, not break out ---
            self._reset(index)
            injected_key_categories = self._top_k_categories(index, self._eq(KEY_PAYLOAD, 'tech'))

            # --- Injected id must not delete other rows ---
            self._reset(index)
            index.delete_embeddings(ids=[ID_PAYLOAD])
            rows_after_delete = self._row_count(index)

            # --- Injected id must not version other rows ---
            self._reset(index)
            index.update_versioning(versioning_timestamp=VERSIONING_TIMESTAMP, ids=[ID_PAYLOAD])
            rows_versioned = self._row_count(index, f'WHERE valid_to = {VERSIONING_TIMESTAMP}')

            # --- Red-state proof: the un-bound interpolation breaks out on this engine ---
            self._reset(index)
            redstate_bypass_count = self._redstate_bypass_count(index)

            self._reset(index)

        handler.add_output('legit_categories', legit_categories)
        handler.add_output('injected_value_categories', injected_value_categories)
        handler.add_output('injected_key_categories', injected_key_categories)
        handler.add_output('rows_after_delete', rows_after_delete)
        handler.add_output('rows_versioned', rows_versioned)
        handler.add_output('redstate_bypass_count', redstate_bypass_count)

        class PGVectorInjectionAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._legit_categories = legit_categories
                cls._injected_value_categories = injected_value_categories
                cls._injected_key_categories = injected_key_categories
                cls._rows_after_delete = rows_after_delete
                cls._rows_versioned = rows_versioned
                cls._redstate_bypass_count = redstate_bypass_count

            def test_legit_text_filter_resolves(self):
                """A legitimate equality filter resolves only the matching row"""
                self.assertEqual(self._legit_categories, ['tech'])

            def test_injected_filter_value_matches_nothing(self):
                """The injected filter value is bound, so it matches no rows"""
                self.assertEqual(self._injected_value_categories, [])

            def test_injected_filter_key_matches_nothing(self):
                """The injected metadata key is bound into the JSON path, so it
                resolves to a missing property rather than breaking out"""
                self.assertEqual(self._injected_key_categories, [])

            def test_injected_id_does_not_delete(self):
                """delete_embeddings binds the id list; both seed rows survive"""
                self.assertEqual(self._rows_after_delete, 2)

            def test_injected_id_does_not_update_versioning(self):
                """update_versioning binds the id list; no rows are versioned"""
                self.assertEqual(self._rows_versioned, 0)

            def test_pre_fix_interpolation_would_break_out(self):
                """Red-state: the old interpolated filter value bypasses on the
                same engine, so the inert results above are the fix working, not
                a lenient engine"""
                self.assertEqual(self._redstate_bypass_count, 2)

        handler.run_assertions(PGVectorInjectionAssertions)
