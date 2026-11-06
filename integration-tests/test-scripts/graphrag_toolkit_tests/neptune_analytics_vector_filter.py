# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify the Neptune Analytics vector index applies a metadata filter inside
top_k against a live graph.

NeptuneIndex.top_k accepted filter_config and ignored it, so vector search
returned the nearest chunks from the whole tenant whatever filter the caller
passed. This test runs the real chunk index over the extracted corpus, once
unfiltered and once filtered to a single source URL, and confirms the filtered
call returns only chunks from that source.

Requires the Neptune Analytics built-in vector index (a 'neptune-graph://'
VECTOR_STORE), i.e. the 'neptune-graph' env-type. Skips on any other vector
store.
"""

import os
import unittest
from typing import Dict, Any

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.vector.neptune_vector_indexes import NEPTUNE_ANALYTICS
from llama_index.core.schema import QueryBundle
from llama_index.core.vector_stores.types import (
    FilterCondition, FilterOperator, MetadataFilter, MetadataFilters,
)

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler


QUERY = 'Neptune Analytics'
TOP_K = 20

# One of the URLs extract.ExtractToFileSystem loads, held as source metadata.
TARGET_URL = 'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html'


class NeptuneAnalyticsVectorFilter(IntegrationTestBase):
    """Drive the real Neptune Analytics chunk index with a source metadata filter
    and confirm top_k returns only chunks from the matching source.
    """

    @property
    def description(self):
        return 'Neptune Analytics vector index filters top_k on source metadata'

    def _is_neptune_analytics_vector_store(self):
        return os.environ.get('VECTOR_STORE', '').startswith(NEPTUNE_ANALYTICS)

    def wait(self) -> bool:
        if not self._is_neptune_analytics_vector_store():
            return False
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str=QUERY), top_k=1)) == 0

    def _urls(self, index, filter_config=None):
        results = index.top_k(QueryBundle(query_str=QUERY), top_k=TOP_K, filter_config=filter_config)
        return [r['source']['metadata']['url'] for r in results]

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        if not self._is_neptune_analytics_vector_store():
            handler.skip()
            return

        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:

            index = vector_store.get_index('chunk')

            unfiltered_urls = self._urls(index)
            filtered_urls = self._urls(index, FilterConfig(source_filters=MetadataFilters(
                filters=[MetadataFilter(key='url', value=TARGET_URL, operator=FilterOperator.EQ)],
                condition=FilterCondition.AND,
            )))
            case_insensitive_urls = self._urls(index, FilterConfig(source_filters=MetadataFilters(
                filters=[MetadataFilter(key='url', value='WHAT-IS-NEPTUNE-ANALYTICS', operator=FilterOperator.TEXT_MATCH_INSENSITIVE)],
                condition=FilterCondition.AND,
            )))

        handler.add_output('unfiltered_urls', sorted(set(unfiltered_urls)))
        handler.add_output('filtered_url_count', len(filtered_urls))
        handler.add_output('filtered_urls', sorted(set(filtered_urls)))
        handler.add_output('case_insensitive_urls', sorted(set(case_insensitive_urls)))

        class NeptuneAnalyticsVectorFilterAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls._unfiltered_urls = unfiltered_urls
                cls._filtered_urls = filtered_urls
                cls._case_insensitive_urls = case_insensitive_urls

            def test_unfiltered_results_span_more_than_one_source(self):
                """The corpus holds chunks from other sources, so the filtered
                result below is the filter working, not a one-source graph"""
                self.assertGreater(len(set(self._unfiltered_urls)), 1)

            def test_filtered_results_are_not_empty(self):
                """The filtered query still returns chunks"""
                self.assertGreater(len(self._filtered_urls), 0)

            def test_filtered_results_come_only_from_the_matching_source(self):
                """Every chunk the filtered query returns belongs to the filtered source"""
                self.assertEqual(set(self._filtered_urls), {TARGET_URL})

            def test_a_case_insensitive_filter_runs_and_matches_the_same_source(self):
                """TEXT_MATCH_INSENSITIVE renders valid openCypher and finds the same source"""
                self.assertEqual(set(self._case_insensitive_urls), {TARGET_URL})

        handler.run_assertions(NeptuneAnalyticsVectorFilterAssertions)
