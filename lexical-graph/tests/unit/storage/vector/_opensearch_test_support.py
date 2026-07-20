# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared opensearchpy/llama_index mock bootstrap, so opensearch_vector_indexes tests
run without the optional opensearch-py / llama-index-vector-stores-opensearch packages.

Every test file that imports opensearch_vector_indexes must call install_opensearch_mocks()
before that import, and use these same Fake*Error classes rather than defining their own:
the module under test binds `from opensearchpy.exceptions import ...` once, at first import,
and that binding is cached for the rest of the process. Separate private classes per file
would make `except RequestError` match only whichever file's mocks happened to load first.
"""

import sys
import types
from unittest.mock import MagicMock


class FakeNotFoundError(Exception):
    def __init__(self, status_code=404, error="not found", info=None):
        super().__init__(error)
        self.status_code = status_code
        self.error = error
        self.info = info


class FakeRequestError(Exception):
    def __init__(self, status_code=400, error="illegal_argument_exception", info=None):
        super().__init__(error)
        self.status_code = status_code
        self.error = error
        self.info = info


def install_opensearch_mocks():
    """Install fake opensearch modules into sys.modules so the source can import."""
    import llama_index

    fake_exceptions_mod = types.ModuleType("opensearchpy.exceptions")
    fake_exceptions_mod.NotFoundError = FakeNotFoundError
    fake_exceptions_mod.RequestError = FakeRequestError

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
