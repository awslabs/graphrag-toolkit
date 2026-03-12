# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import sys

def test_raises_exception_if_dependencies_not_installed():
    # Clean up any mocked providers module from other tests
    if 'graphrag_toolkit.lexical_graph.indexing.load.readers.providers' in sys.modules:
        providers_module = sys.modules['graphrag_toolkit.lexical_graph.indexing.load.readers.providers']
        # Only remove if it's a Mock object (from other tests)
        if hasattr(providers_module, '_mock_name'):
            del sys.modules['graphrag_toolkit.lexical_graph.indexing.load.readers.providers']
    
    from graphrag_toolkit.lexical_graph.indexing.load.readers.providers import AdvancedPDFReaderProvider
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import PDFReaderConfig 

    with pytest.raises(ImportError) as exc_info:  
            reader = AdvancedPDFReaderProvider(PDFReaderConfig())

    assert exc_info.value.args[0] == "pymupdf package not found, install with 'pip install pymupdf'"
    
  


    