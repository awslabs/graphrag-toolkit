# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.byokg_rag.graphstore import (
    NeptuneAnalyticsGraphStore,
    NeptuneDBGraphStore,
)
from graphrag_toolkit.byokg_rag.graphstore.neptune import _escape_cypher_label
from graphrag_toolkit.byokg_rag.graph_retrievers.graph_retrievers import GraphQueryRetriever


class CypherSafetyCheck(IntegrationTestBase):
    """Integration test verifying that the GraphQueryRetriever blocks
    obfuscated write queries against a live Neptune Analytics graph.

    This test confirms that:
    1. Comment-split keywords (CRE/**/ATE) are blocked
    2. CALL/APOC procedures are blocked
    3. Unicode lookalike keywords are blocked
    4. The block_graph_modification flag works correctly
    5. Legitimate read queries still execute successfully

    Note: Neither Neptune Graph (Analytics) nor Neptune DB support a read-only
    query execution parameter. Write protection relies on application-level
    Cypher validation. For stronger server-side guarantees, restrict the IAM
    role to neptune-graph:ReadDataViaQuery or neptune-db:ReadDataViaQuery.
    """

    @property
    def description(self):
        return 'Verify Cypher safety checks block obfuscated write queries'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        region = os.environ['AWS_REGION_NAME']
        graph_store_id = os.environ['GRAPH_STORE']

        if not graph_store_id.startswith('neptune-graph://'):
            raise ValueError(
                f"Invalid graph store id. Expected Neptune graph beginning "
                f"'neptune-graph://', but received {graph_store_id}."
            )

        graph_identifier = graph_store_id[16:]
        graph_store = NeptuneAnalyticsGraphStore(
            graph_identifier=graph_identifier, region=region
        )

        retriever = GraphQueryRetriever(
            graph_store=graph_store, block_graph_modification=True
        )

        # Run a legitimate read query to confirm connectivity
        read_result = retriever.retrieve(
            "MATCH (n) RETURN n LIMIT 1", return_answers=True
        )
        read_context, read_answers = read_result

        # Collect results for bypass attempts
        bypass_results = {}

        # Comment bypass attempts
        comment_queries = [
            "CRE/**/ATE (n:MaliciousNode {name: 'should_not_exist'})",
            "ME/**/RGE (n:MaliciousNode {name: 'should_not_exist'})",
            "MATCH (n) DE/**/LETE n",
        ]
        for q in comment_queries:
            result = retriever.retrieve(q)
            bypass_results[f"comment:{q[:30]}"] = (
                "Cannot execute query" in result[0]
            )

        # CALL/APOC bypass attempts
        call_queries = [
            "CALL apoc.create.node(['MaliciousNode'], {name: 'evil'})",
            "CALL { CREATE (n:MaliciousNode) RETURN n }",
        ]
        for q in call_queries:
            result = retriever.retrieve(q)
            bypass_results[f"call:{q[:30]}"] = (
                "Cannot execute query" in result[0]
            )

        # Unicode bypass attempts
        unicode_create = ''.join(
            chr(ord(c) + 0xFEE0) if c.isalpha() else c for c in 'CREATE'
        )
        unicode_queries = [
            f"{unicode_create} (n:MaliciousNode {{name: 'evil'}})",
        ]
        for q in unicode_queries:
            result = retriever.retrieve(q)
            bypass_results[f"unicode:{q[:30]}"] = (
                "Cannot execute query" in result[0]
            )

        # Flag bypass - when False, queries should execute (read-only at driver level)
        retriever_no_block = GraphQueryRetriever(
            graph_store=graph_store, block_graph_modification=False
        )
        flag_safe_result = retriever_no_block.is_query_safe(
            "CREATE (n:MaliciousNode)"
        )

        # Verify no MaliciousNode was created
        verify_result = graph_store.execute_query(
            "MATCH (n:MaliciousNode) RETURN count(n) as cnt"
        )
        malicious_count = verify_result[0]['cnt'] if verify_result else 0

        class CypherSafetyAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._read_context = read_context
                cls._read_answers = read_answers
                cls._bypass_results = bypass_results
                cls._flag_safe_result = flag_safe_result
                cls._malicious_count = malicious_count

            def test_read_query_succeeds(self):
                """Legitimate read query executes successfully"""
                self.assertTrue(len(self._read_context) > 0)
                self.assertNotIn("Cannot execute query", self._read_context[0])
                self.assertNotIn("Error", self._read_context[0])

            def test_comment_bypass_blocked(self):
                """Comment-split keywords (CRE/**/ATE) are blocked"""
                comment_blocked = [
                    v for k, v in self._bypass_results.items()
                    if k.startswith("comment:")
                ]
                self.assertTrue(all(comment_blocked))

            def test_call_bypass_blocked(self):
                """CALL/APOC procedures are blocked"""
                call_blocked = [
                    v for k, v in self._bypass_results.items()
                    if k.startswith("call:")
                ]
                self.assertTrue(all(call_blocked))

            def test_unicode_bypass_blocked(self):
                """Unicode lookalike keywords are blocked"""
                unicode_blocked = [
                    v for k, v in self._bypass_results.items()
                    if k.startswith("unicode:")
                ]
                self.assertTrue(all(unicode_blocked))

            def test_flag_disables_check(self):
                """block_graph_modification=False skips safety check"""
                self.assertTrue(self._flag_safe_result)

            def test_no_malicious_nodes_created(self):
                """No MaliciousNode was created in the graph"""
                self.assertEqual(self._malicious_count, 0)

        handler.run_assertions(CypherSafetyAssertions)


# A node label and relationship type carrying a Cypher breakout payload. If a
# sink interpolates one into a backtick-quoted identifier without escaping, the
# appended clause deletes the canary; with escaping the canary survives.
CANARY_LABEL = '__CypherSafetyCanary__'
CANARY_ID = 'cypher-safety-canary'
MALICIOUS_NODE_LABEL = (
    'EvilNode`) MATCH (c:`__CypherSafetyCanary__`) DETACH DELETE c //'
)
MALICIOUS_EDGE_TYPE = (
    'EvilEdge`]->() MATCH (c:`__CypherSafetyCanary__`) DETACH DELETE c //'
)
_TMP_IDS = [CANARY_ID, 'evil-node', 'evil-edge-a', 'evil-edge-b']


class BYOKGCypherInjectionSafety(IntegrationTestBase):
    """Insert a backtick-bearing label into a live Neptune graph, run the
    schema-discovery path, and confirm no injected clause executes."""

    @property
    def description(self):
        return 'Schema discovery escapes backticks in dynamic Cypher labels'

    def _make_graph_store(self):
        region = os.environ['AWS_REGION_NAME']
        graph_store_id = os.environ['GRAPH_STORE']

        if graph_store_id.startswith('neptune-graph://'):
            graph_identifier = graph_store_id[len('neptune-graph://'):]
            store = NeptuneAnalyticsGraphStore(
                graph_identifier=graph_identifier, region=region
            )
            return 'analytics', store

        if graph_store_id.startswith('neptune-db://'):
            endpoint = graph_store_id[len('neptune-db://'):]
            if not endpoint.startswith('https://'):
                endpoint = f'https://{endpoint}'
            # Neptune DB serves the data-plane API on port 8182; append it if absent
            if endpoint.count(':') < 2:
                endpoint = f'{endpoint}:8182'
            return 'db', NeptuneDBGraphStore(endpoint_url=endpoint, region=region)

        raise ValueError(
            "Invalid graph store id. Expected 'neptune-graph://' or "
            f"'neptune-db://', but received {graph_store_id}."
        )

    def _canary_count(self, graph_store):
        rows = graph_store.execute_query(
            f'MATCH (c:`{CANARY_LABEL}`) RETURN count(c) AS n'
        )
        return rows[0]['n'] if rows else 0

    def _seed_payload(self, graph_store):
        # The label/type are escaped here on the write path so the payload is
        # stored as data; the read path (under test) is what must re-escape it.
        graph_store.execute_query(
            f'CREATE (c:`{CANARY_LABEL}` {{id: $id}})',
            parameters={'id': CANARY_ID},
        )
        graph_store.execute_query(
            f'CREATE (n:`{_escape_cypher_label(MALICIOUS_NODE_LABEL)}` {{id: $id}})',
            parameters={'id': 'evil-node'},
        )
        graph_store.execute_query(
            f'CREATE (a:`__CypherSafetyTmp__` {{id: $aid}})'
            f'-[:`{_escape_cypher_label(MALICIOUS_EDGE_TYPE)}`]->'
            f'(b:`__CypherSafetyTmp__` {{id: $bid}})',
            parameters={'aid': 'evil-edge-a', 'bid': 'evil-edge-b'},
        )

    def _exercise_sinks(self, engine, graph_store):
        """Drive the escaped schema-discovery sinks for this engine and return
        the labels the path observed."""
        if engine == 'db':
            # get_schema() -> _refresh_schema() -> _get_node_properties /
            # _get_edge_properties / _get_triples, each interpolating a label
            # read back from the graph (including the payload labels seeded above).
            schema = graph_store.get_schema()
            return list(schema.get('nodeLabels', []))

        # Analytics: pg_schema() does not interpolate labels, so the payload
        # flows in as the node_type argument to the shared sinks instead.
        graph_store.nodes(node_type=MALICIOUS_NODE_LABEL)
        graph_store.get_node_text_for_embedding_input(
            node_embedding_text_props={MALICIOUS_NODE_LABEL: ['name']},
            group_by_node_label=True,
        )
        return [MALICIOUS_NODE_LABEL]

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        engine, graph_store = self._make_graph_store()

        self._seed_payload(graph_store)
        canary_before = self._canary_count(graph_store)

        schema_error = None
        discovered_labels = []
        try:
            discovered_labels = self._exercise_sinks(engine, graph_store)
        except Exception as e:
            schema_error = e

        canary_after = self._canary_count(graph_store)

        # Best-effort cleanup; matching by id is label-agnostic.
        try:
            graph_store.execute_query(
                'MATCH (n) WHERE n.id IN $ids DETACH DELETE n',
                parameters={'ids': _TMP_IDS},
            )
        except Exception as e:
            handler.add_exception(e)

        handler.add_output('engine', engine)
        handler.add_output('canary_before', canary_before)
        handler.add_output('canary_after', canary_after)
        handler.add_output(
            'schema_error', str(schema_error) if schema_error else None
        )
        handler.add_output('discovered_labels', discovered_labels)

        class CypherInjectionSafetyAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._engine = engine
                cls._canary_before = canary_before
                cls._canary_after = canary_after
                cls._schema_error = schema_error
                cls._discovered_labels = discovered_labels

            def test_setup_created_canary(self):
                """Canary node exists before schema discovery runs"""
                self.assertEqual(self._canary_before, 1)

            def test_schema_discovery_did_not_error(self):
                """Schema discovery completes without raising"""
                self.assertIsNone(self._schema_error)

            def test_canary_survives_injection_payload(self):
                """Canary still present: no injected DETACH DELETE executed"""
                self.assertEqual(self._canary_after, 1)

            def test_payload_label_flowed_through_discovery(self):
                """The backtick-bearing label was processed as data, not code"""
                self.assertIn(MALICIOUS_NODE_LABEL, self._discovered_labels)

        handler.run_assertions(CypherInjectionSafetyAssertions)


# A breakout s3_path aimed at the CALL neptune.load(source: '...') sink. Its
# quote/space/backtick/')' fall outside the allowlist, so it is rejected before
# reaching the sink; if the validator were dropped, it would delete the canary.
MALICIOUS_S3_PATH = (
    "s3://b/k', region:'x'}) "
    f"MATCH (c:`{CANARY_LABEL}`) DETACH DELETE c //"
)


class BYOKGS3PathInjectionSafety(IntegrationTestBase):
    """Call read_from_csv with an s3_path carrying a Cypher breakout payload
    and confirm the validator rejects it before any CALL neptune.load() or
    bulk-loader job runs, leaving a seeded canary node untouched."""

    @property
    def description(self):
        return 'read_from_csv rejects a breakout s3_path before the load sink'

    def _make_graph_store(self):
        region = os.environ['AWS_REGION_NAME']
        graph_store_id = os.environ['GRAPH_STORE']

        if graph_store_id.startswith('neptune-graph://'):
            graph_identifier = graph_store_id[len('neptune-graph://'):]
            store = NeptuneAnalyticsGraphStore(
                graph_identifier=graph_identifier, region=region
            )
            return 'analytics', store

        if graph_store_id.startswith('neptune-db://'):
            endpoint = graph_store_id[len('neptune-db://'):]
            if not endpoint.startswith('https://'):
                endpoint = f'https://{endpoint}'
            # Neptune DB serves the data-plane API on port 8182; append it if absent
            if endpoint.count(':') < 2:
                endpoint = f'{endpoint}:8182'
            return 'db', NeptuneDBGraphStore(endpoint_url=endpoint, region=region)

        raise ValueError(
            "Invalid graph store id. Expected 'neptune-graph://' or "
            f"'neptune-db://', but received {graph_store_id}."
        )

    def _canary_count(self, graph_store):
        rows = graph_store.execute_query(
            f'MATCH (c:`{CANARY_LABEL}`) RETURN count(c) AS n'
        )
        return rows[0]['n'] if rows else 0

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        engine, graph_store = self._make_graph_store()

        # Canary survival proves no injection reached the analytics Cypher sink;
        # on db the operative proof is the validator raising before the load job.
        graph_store.execute_query(
            f'CREATE (c:`{CANARY_LABEL}` {{id: $id}})',
            parameters={'id': CANARY_ID},
        )
        canary_before = self._canary_count(graph_store)

        rejected = False
        rejection_message = None
        unexpected_error = None
        try:
            graph_store.read_from_csv(s3_path=MALICIOUS_S3_PATH)
        except ValueError as e:
            rejected = True
            rejection_message = str(e)
        except Exception as e:
            # Non-ValueError => payload slipped past the validator to a sink.
            unexpected_error = e

        canary_after = self._canary_count(graph_store)

        # Best-effort cleanup by id.
        try:
            graph_store.execute_query(
                'MATCH (n) WHERE n.id IN $ids DETACH DELETE n',
                parameters={'ids': [CANARY_ID]},
            )
        except Exception as e:
            handler.add_exception(e)

        handler.add_output('engine', engine)
        handler.add_output('canary_before', canary_before)
        handler.add_output('canary_after', canary_after)
        handler.add_output('rejected', rejected)
        handler.add_output('rejection_message', rejection_message)
        handler.add_output(
            'unexpected_error',
            str(unexpected_error) if unexpected_error else None,
        )

        class S3PathInjectionSafetyAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._engine = engine
                cls._canary_before = canary_before
                cls._canary_after = canary_after
                cls._rejected = rejected
                cls._rejection_message = rejection_message
                cls._unexpected_error = unexpected_error

            def test_setup_created_canary(self):
                """Canary node exists before read_from_csv runs"""
                self.assertEqual(self._canary_before, 1)

            def test_no_unexpected_error(self):
                """read_from_csv failed only via the s3_path validator, not a
                downstream sink"""
                self.assertIsNone(self._unexpected_error)

            def test_breakout_s3_path_rejected(self):
                """Malicious s3_path raised ValueError before the load sink"""
                self.assertTrue(self._rejected)
                self.assertIn(
                    'Invalid s3_path', self._rejection_message or ''
                )

            def test_canary_survives_injection_payload(self):
                """Canary still present: no injected DETACH DELETE executed"""
                self.assertEqual(self._canary_after, 1)

        handler.run_assertions(S3PathInjectionSafetyAssertions)


# A property name carrying a Cypher breakout payload. get_nodes and
# get_one_hop_edges interpolate node_type_to_property_mapping values into
# n.`{prop}`; without escaping the embedded backtick closes the identifier and
# the appended clause deletes the canary. With escaping the whole payload is an
# inert property name and the canary survives.
PROP_CANARY_LABEL = '__ByokgPropInjCanary__'
PROP_CANARY_ID = 'byokg-prop-inj-canary'
MALICIOUS_PROPERTY = (
    'id` OR true WITH n MATCH (c:`__ByokgPropInjCanary__`) DETACH DELETE c //'
)


class BYOKGPropertyNameInjectionSafety(IntegrationTestBase):
    """Drive get_nodes / get_one_hop_edges with a backtick-bearing property name
    against a live Neptune graph and confirm the escaped property cannot break
    out and delete a canary node."""

    @property
    def description(self):
        return 'get_nodes and get_one_hop_edges escape interpolated property names'

    def _make_graph_store(self):
        region = os.environ['AWS_REGION_NAME']
        graph_store_id = os.environ['GRAPH_STORE']

        if graph_store_id.startswith('neptune-graph://'):
            graph_identifier = graph_store_id[len('neptune-graph://'):]
            return NeptuneAnalyticsGraphStore(
                graph_identifier=graph_identifier, region=region
            )
        if graph_store_id.startswith('neptune-db://'):
            endpoint = graph_store_id[len('neptune-db://'):]
            if not endpoint.startswith('https://'):
                endpoint = f'https://{endpoint}'
            # Neptune DB serves the data-plane API on port 8182; append it if absent
            if endpoint.count(':') < 2:
                endpoint = f'{endpoint}:8182'
            return NeptuneDBGraphStore(endpoint_url=endpoint, region=region)

        raise ValueError(
            "Invalid graph store id. Expected 'neptune-graph://' or "
            f"'neptune-db://', but received {graph_store_id}."
        )

    def _canary_count(self, graph_store):
        rows = graph_store.execute_query(
            f'MATCH (c:`{PROP_CANARY_LABEL}`) RETURN count(c) AS n'
        )
        return rows[0]['n'] if rows else 0

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        graph_store = self._make_graph_store()

        graph_store.execute_query(
            f'CREATE (c:`{PROP_CANARY_LABEL}` {{id: $id}})',
            parameters={'id': PROP_CANARY_ID},
        )
        canary_before = self._canary_count(graph_store)

        # Route the payload through the property-name sinks under test.
        graph_store.node_type_to_property_mapping = {
            '__PropInjProbe__': MALICIOUS_PROPERTY
        }

        sink_error = None
        try:
            graph_store.get_nodes(['__prop-inj-probe__'])
            graph_store.get_one_hop_edges(['__prop-inj-probe__'])
        except Exception as e:
            sink_error = e

        canary_after = self._canary_count(graph_store)

        try:
            graph_store.execute_query(
                f'MATCH (c:`{PROP_CANARY_LABEL}`) DETACH DELETE c'
            )
        except Exception as e:
            handler.add_exception(e)

        handler.add_output('canary_before', canary_before)
        handler.add_output('canary_after', canary_after)
        handler.add_output('sink_error', str(sink_error) if sink_error else None)

        class PropertyNameInjectionSafetyAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._canary_before = canary_before
                cls._canary_after = canary_after
                cls._sink_error = sink_error

            def test_setup_created_canary(self):
                """Canary node exists before the sinks run"""
                self.assertEqual(self._canary_before, 1)

            def test_sinks_execute_without_error(self):
                """Escaped property produces valid Cypher; a raw n.{prop}
                regression would be a syntax error"""
                self.assertIsNone(self._sink_error)

            def test_canary_survives_injection_payload(self):
                """Canary still present: no injected DETACH DELETE executed"""
                self.assertEqual(self._canary_after, 1)

        handler.run_assertions(PropertyNameInjectionSafetyAssertions)
