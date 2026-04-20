# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

def test_raises_exception_if_dependencies_not_installed():
    from graphrag_toolkit.lexical_graph.indexing.load.readers.providers import DocxReaderProvider
    from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DocxReaderConfig 
       
    with pytest.raises(ImportError) as exc_info:  
         reader = DocxReaderProvider(DocxReaderConfig())
 
    assert exc_info.value.args[0] == "python-docx package not found, install with 'pip install python-docx'"