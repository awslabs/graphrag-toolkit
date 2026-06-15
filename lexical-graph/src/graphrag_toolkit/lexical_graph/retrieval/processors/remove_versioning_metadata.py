# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.versioning import VERSIONING_METADATA_KEYS
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle

class RemoveVersioningMetadata(ProcessorBase):

    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:

        def remove_versioning_info(index:int, search_result:SearchResult):
            
            for key in VERSIONING_METADATA_KEYS:
                if key in search_result.source.metadata:
                    del search_result.source.metadata[key]

            return search_result
        
        return self._apply_to_search_results(search_results, remove_versioning_info)
