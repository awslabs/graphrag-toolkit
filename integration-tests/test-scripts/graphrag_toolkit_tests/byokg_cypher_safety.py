# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.byokg_rag.graphstore import NeptuneAnalyticsGraphStore
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
