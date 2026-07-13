# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import builtins

import pytest

from graphrag_toolkit.lexical_graph.indexing.load.readers.providers.advanced_pdf_reader_provider import (
    AdvancedPDFReaderProvider,
)
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import (
    PDFReaderConfig,
)


def test_raises_exception_if_dependencies_not_installed(monkeypatch):
    # Force the pymupdf import to fail regardless of whether it is installed,
    # so the test is deterministic in any environment.
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "pymupdf":
            raise ImportError("No module named 'pymupdf'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError) as exc_info:
        AdvancedPDFReaderProvider(PDFReaderConfig())

    assert exc_info.value.args[0] == "pymupdf package not found, install with 'pip install pymupdf'"
