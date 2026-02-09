# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from llama_index.core.schema import QueryBundle

class UpdateChunkMetadata(ProcessorBase):

    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        def update_chunks(topic:Topic):
            for chunk in topic.chunks:
                value = chunk.metadata.pop('value', None)
                chunk.metadata.pop('chunkId', None)
                if value:
                    chunk.value = value
            return topic

        def update_search_result_chunks(index:int, search_result:SearchResult):
            return self._apply_to_topics(search_result, update_chunks)
        
        return self._apply_to_search_results(search_results, update_search_result_chunks)


