# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List

from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_provider_base import KeywordProviderBase
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.types import QueryBundle

class PassThruKeywordProvider(KeywordProviderBase):
    
    def __init__(self, args:ProcessorArgs):
        super().__init__(args)

    def _get_keywords(self, query_bundle:QueryBundle) -> List[str]:  

        keywords = [query_bundle.query_str]
        return keywords