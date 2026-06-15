# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from graphrag_toolkit.core.types import QueryBundle

class ClearTopicIds(ProcessorBase):
   
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:

        def clear_topic_id(topic:Topic):
            topic.topicId = None
            return topic

        def clear_search_result_topics(index:int, search_result:SearchResult):
            return self._apply_to_topics(search_result, clear_topic_id)
        
        return self._apply_to_search_results(search_results, clear_search_result_topics)


