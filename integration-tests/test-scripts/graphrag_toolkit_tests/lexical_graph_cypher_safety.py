# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify lexical-graph escapes Cypher labels at the EntityGraphBuilder
domain-label sink against a live Neptune graph (Analytics or Database).

`label_from` passes `__...__` classifications through unescaped, so a backtick
in one can close the identifier and inject Cypher. The test drives the real
EntityGraphBuilder.build() with a malicious classification and checks a seeded
canary survives. Requires env var GRAPH_STORE (neptune-graph:// or neptune-db://).
"""

import os
import unittest
from typing import Dict, Any

from llama_index.core.schema import TextNode

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
    NeptuneAnalyticsClient,
    NeptuneDatabaseClient,
)
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import label_from
from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import (
    EntityGraphBuilder,
)
from graphrag_toolkit.lexical_graph.indexing.model import Entity, Fact, Relation


CANARY_LABEL = '__LexGraphInjCanary__'

# Wrapped in __...__ so label_from passes them through verbatim. Backtick closes
# the identifier; the newline variant also targets the trailing `//` comment.
BACKTICK_CLASSIFICATION = (
    f'__Evil` WITH e MATCH (c:`{CANARY_LABEL}`) DETACH DELETE c //__'
)
NEWLINE_CLASSIFICATION = (
    f'__Evil`\nMATCH (c:`{CANARY_LABEL}`) DETACH DELETE c //__'
)

BACKTICK_ENTITY_ID = 'lexgraph-evil-backtick'
NEWLINE_ENTITY_ID = 'lexgraph-evil-newline'
REDSTATE_ENTITY_ID = 'lexgraph-evil-redstate'
_ENTITY_IDS = [BACKTICK_ENTITY_ID, NEWLINE_ENTITY_ID, REDSTATE_ENTITY_ID]


class LexicalGraphLabelInjectionSafety(IntegrationTestBase):
    """Drive the real domain-label sink with malicious classifications and
    confirm no injected clause executes: backtick breakout, newline comment
    termination, plus a red-state proof that the canary mechanism works.
    """

    @property
    def description(self):
        return 'EntityGraphBuilder escapes Cypher labels at the domain-label sink'

    def _make_graph_store(self):
        graph_store_id = os.environ['GRAPH_STORE']

        if graph_store_id.startswith('neptune-graph://'):
            graph_identifier = graph_store_id[len('neptune-graph://'):]
            return 'analytics', NeptuneAnalyticsClient(graph_id=graph_identifier)

        if graph_store_id.startswith('neptune-db://'):
            endpoint = graph_store_id[len('neptune-db://'):]
            if not endpoint.startswith('https://'):
                endpoint = f'https://{endpoint}'
            return 'db', NeptuneDatabaseClient(endpoint_url=endpoint)

        raise ValueError(
            f"Invalid graph store id. Expected 'neptune-graph://' or "
            f"'neptune-db://', but received {graph_store_id}."
        )

    def _canary_count(self, graph_store):
        rows = graph_store.execute_query_with_retry(
            f'MATCH (c:`{CANARY_LABEL}`) RETURN count(c) AS n', {}
        )
        return rows[0]['n'] if rows else 0

    def _seed_canary(self, graph_store):
        graph_store.execute_query_with_retry(
            f'MERGE (c:`{CANARY_LABEL}` {{probe: $probe}})', {'probe': 'canary'}
        )

    def _cleanup(self, graph_store):
        graph_store.execute_query_with_retry(
            f'MATCH (c:`{CANARY_LABEL}`) DETACH DELETE c', {}
        )
        graph_store.execute_query_with_retry(
            'MATCH (n:`__Entity__`) WHERE id(n) IN $ids DETACH DELETE n',
            {'ids': _ENTITY_IDS},
        )

    def _fact_node(self, entity_id, classification):
        """A node whose subject classification carries the injection payload."""
        fact = Fact(
            subject=Entity(
                entityId=entity_id, value='evil entity', classification=classification
            ),
            predicate=Relation(value='RELATES_TO'),
            object=Entity(
                entityId=f'{entity_id}-obj', value='benign object',
                classification='__Thing__',
            ),
        )
        return TextNode(text='injection probe', metadata={'fact': fact.model_dump()})

    def _build(self, graph_store, entity_id, classification):
        EntityGraphBuilder().build(
            self._fact_node(entity_id, classification),
            graph_store,
            include_domain_labels=True,
            include_local_entities=False,
        )

    def _run_vulnerable_query(self, graph_store, entity_id, classification):
        """Run the pre-patch (un-escaped) construction to prove the canary
        mechanism can detect a breakout."""
        e_label = label_from(classification)
        query = (
            f"MERGE (e:`__Entity__`{{{graph_store.node_id('entityId')}: '{entity_id}'}}) "
            f"SET e :`{e_label}` // awsqid:{entity_id}-{e_label}"
        )
        graph_store.execute_query_with_retry(query, {})

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        engine, graph_store = self._make_graph_store()

        # --- Test 1: Backtick breakout (real builder) ---
        self._cleanup(graph_store)
        self._seed_canary(graph_store)
        canary_before_backtick = self._canary_count(graph_store)

        build_error_backtick = None
        try:
            self._build(graph_store, BACKTICK_ENTITY_ID, BACKTICK_CLASSIFICATION)
        except Exception as e:
            build_error_backtick = e

        canary_after_backtick = self._canary_count(graph_store)

        # --- Test 2: Newline comment termination (real builder) ---
        self._cleanup(graph_store)
        self._seed_canary(graph_store)
        canary_before_newline = self._canary_count(graph_store)

        build_error_newline = None
        try:
            self._build(graph_store, NEWLINE_ENTITY_ID, NEWLINE_CLASSIFICATION)
        except Exception as e:
            build_error_newline = e

        canary_after_newline = self._canary_count(graph_store)

        # --- Test 3: Red-state proof (un-escaped construction deletes canary) ---
        self._cleanup(graph_store)
        self._seed_canary(graph_store)
        canary_before_vuln = self._canary_count(graph_store)

        vuln_error = None
        try:
            self._run_vulnerable_query(
                graph_store, REDSTATE_ENTITY_ID, BACKTICK_CLASSIFICATION
            )
        except Exception as e:
            # An engine may reject the malformed query outright, which is also a
            # safe outcome: the injected clause never executes.
            vuln_error = e

        canary_after_vuln = self._canary_count(graph_store)

        self._cleanup(graph_store)

        handler.add_output('engine', engine)
        handler.add_output('canary_before_backtick', canary_before_backtick)
        handler.add_output('canary_after_backtick', canary_after_backtick)
        handler.add_output('canary_before_newline', canary_before_newline)
        handler.add_output('canary_after_newline', canary_after_newline)
        handler.add_output('canary_before_vuln', canary_before_vuln)
        handler.add_output('canary_after_vuln', canary_after_vuln)
        handler.add_output(
            'build_error_backtick',
            str(build_error_backtick) if build_error_backtick else None,
        )
        handler.add_output(
            'build_error_newline',
            str(build_error_newline) if build_error_newline else None,
        )
        handler.add_output('vuln_error', str(vuln_error) if vuln_error else None)

        class LabelInjectionAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._canary_before_backtick = canary_before_backtick
                cls._canary_after_backtick = canary_after_backtick
                cls._canary_before_newline = canary_before_newline
                cls._canary_after_newline = canary_after_newline
                cls._canary_before_vuln = canary_before_vuln
                cls._canary_after_vuln = canary_after_vuln
                cls._build_error_backtick = build_error_backtick
                cls._build_error_newline = build_error_newline
                cls._vuln_error = vuln_error

            def test_canary_seeded_for_backtick(self):
                """Canary exists before the backtick-breakout build runs"""
                self.assertEqual(self._canary_before_backtick, 1)

            def test_build_backtick_does_not_error(self):
                """Builder escapes the backtick label and runs without raising"""
                self.assertIsNone(self._build_error_backtick)

            def test_canary_survives_backtick_breakout(self):
                """Canary still present: the escaped label blocked the injection"""
                self.assertEqual(self._canary_after_backtick, 1)

            def test_canary_seeded_for_newline(self):
                """Canary exists before the newline-termination build runs"""
                self.assertEqual(self._canary_before_newline, 1)

            def test_build_newline_does_not_error(self):
                """Builder strips the newline from the comment and runs cleanly"""
                self.assertIsNone(self._build_error_newline)

            def test_canary_survives_newline_termination(self):
                """Canary still present: newline stripping blocked the injection"""
                self.assertEqual(self._canary_after_newline, 1)

            def test_canary_seeded_for_redstate(self):
                """Canary exists before the red-state proof runs"""
                self.assertEqual(self._canary_before_vuln, 1)

            def test_vulnerable_query_deletes_canary_or_is_rejected(self):
                """The un-escaped construction either deletes the canary (proving
                this test would catch a regression) or the engine rejects it
                (also safe)."""
                canary_deleted = self._canary_after_vuln == 0
                engine_rejected = self._vuln_error is not None
                self.assertTrue(
                    canary_deleted or engine_rejected,
                    'Expected the un-escaped query to delete the canary or be '
                    'rejected by the engine, but neither happened.',
                )

        handler.run_assertions(LabelInjectionAssertions)
