# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for the `.keyword` sub-field fix in OpenSearchIndex.

OpenSearch Serverless auto-maps metadata string fields as both an analyzed
`text` field and an exact-match `keyword` sub-field. The id-lookup `terms`
queries in `get_embeddings` / `_get_existing_doc_ids_for_ids` must target the
`.keyword` sub-field, otherwise mixed-case / uppercase ids (e.g. SEC-10Q chunk
ids, personal-context-v2 doc ids) fail to match because the text analyzer
lowercases the value.

Covers:
  - get_embeddings constructs a terms query against metadata.<KEY>.key.keyword
  - _get_existing_doc_ids_for_ids does the same
  - mixed-case ids reach the query with case preserved (the regression)
"""

import sys
import types
from unittest.mock import MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Bootstrap: inject fake opensearch modules before the source module loads
# (mirrors test_opensearch_clients.py so the suite runs without opensearchpy)
# ---------------------------------------------------------------------------

def _install_opensearch_mocks():
    import llama_index

    fake_exceptions_mod = types.ModuleType("opensearchpy.exceptions")

    class _NotFoundError(Exception):
        def __init__(self, status_code=404, error="not found", info=None):
            super().__init__(error)
            self.status_code = status_code

    class _RequestError(Exception):
        pass

    fake_exceptions_mod.NotFoundError = _NotFoundError
    fake_exceptions_mod.RequestError = _RequestError

    fake_opensearch_mod = types.ModuleType("opensearchpy")
    fake_opensearch_mod.OpenSearch = MagicMock(name="OpenSearch")
    fake_opensearch_mod.AsyncOpenSearch = MagicMock(name="AsyncOpenSearch")
    fake_opensearch_mod.AWSV4SignerAsyncAuth = MagicMock(name="AWSV4SignerAsyncAuth")
    fake_opensearch_mod.AsyncHttpConnection = MagicMock(name="AsyncHttpConnection")
    fake_opensearch_mod.Urllib3AWSV4SignerAuth = MagicMock(name="Urllib3AWSV4SignerAuth")
    fake_opensearch_mod.Urllib3HttpConnection = MagicMock(name="Urllib3HttpConnection")
    fake_opensearch_mod.exceptions = fake_exceptions_mod

    fake_llama_vs_mod = types.ModuleType("llama_index.vector_stores")
    fake_llama_os_mod = types.ModuleType("llama_index.vector_stores.opensearch")

    class _FakeOpensearchVectorClient:
        _get_opensearch_version = None
        _bulk_ingest_embeddings = None

    fake_llama_os_mod.OpensearchVectorClient = _FakeOpensearchVectorClient
    fake_llama_vs_mod.opensearch = fake_llama_os_mod
    llama_index.vector_stores = fake_llama_vs_mod

    sys.modules["opensearchpy"] = fake_opensearch_mod
    sys.modules["opensearchpy.exceptions"] = fake_exceptions_mod
    sys.modules["llama_index.vector_stores"] = fake_llama_vs_mod
    sys.modules["llama_index.vector_stores.opensearch"] = fake_llama_os_mod


_install_opensearch_mocks()

import graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes as ovi  # noqa: E402
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY  # noqa: E402

KEYWORD_FIELD = f'metadata.{INDEX_KEY}.key.keyword'


def _make_index():
    return ovi.OpenSearchIndex(
        index_name='chunk',
        endpoint='http://localhost:9200',
        dimensions=1024,
        embed_model='cohere.embed-english-v3',  # id-lookup paths don't embed; string is valid
    )


def _capture_search_bodies(index, call):
    """Run `call(index)` with a mocked OpenSearch client; return the search bodies."""
    bodies = []

    def fake_search(index=None, body=None):
        bodies.append(body)
        return {'hits': {'hits': []}}  # empty -> paginated_search stops after one page

    mock_os = MagicMock()
    mock_os.search.side_effect = fake_search
    mock_client = MagicMock()
    mock_client._os_client = mock_os

    with patch.object(ovi.OpenSearchIndex, 'client', new_callable=PropertyMock, return_value=mock_client), \
         patch.object(ovi.OpenSearchIndex, 'underlying_index_name', return_value='chunk'):
        call(index)
    return bodies


class TestKeywordSubfieldQuery:

    def test_get_embeddings_targets_keyword_subfield(self):
        bodies = _capture_search_bodies(_make_index(), lambda i: i.get_embeddings(['abc123']))
        assert bodies, "expected the OpenSearch client to be queried"
        terms = bodies[0]['query']['terms']
        assert KEYWORD_FIELD in terms, f"query must target {KEYWORD_FIELD}, got {list(terms)}"

    def test_get_existing_doc_ids_targets_keyword_subfield(self):
        bodies = _capture_search_bodies(_make_index(), lambda i: i._get_existing_doc_ids_for_ids(['abc123']))
        assert bodies, "expected the OpenSearch client to be queried"
        terms = bodies[0]['query']['terms']
        assert KEYWORD_FIELD in terms, f"query must target {KEYWORD_FIELD}, got {list(terms)}"

    def test_mixed_case_ids_preserved_for_exact_match(self):
        # Real-world mixed-case ids must reach the query with case intact. The
        # original bug matched against the analyzed (lowercased) text field and
        # silently missed these; the .keyword sub-field does an exact match.
        ids = ['09xZARpUfN7F', 'SEC-10Q']
        bodies = _capture_search_bodies(_make_index(), lambda i: i.get_embeddings(ids))
        values = bodies[0]['query']['terms'][KEYWORD_FIELD]
        assert '09xZARpUfN7F' in values, "mixed-case id must be preserved (not lowercased)"
        assert 'SEC10Q' in values, "_clean_id strips punctuation but must preserve case"
