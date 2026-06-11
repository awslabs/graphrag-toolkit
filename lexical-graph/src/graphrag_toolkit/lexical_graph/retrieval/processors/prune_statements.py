# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, Topic

from graphrag_toolkit.core.types import QueryBundle

class PruneStatements(ProcessorBase):
    
    def __init__(self, args:ProcessorArgs, filter_config:FilterConfig):
        super().__init__(args, filter_config)

    def _process_results(self, search_results:SearchResultCollection, query:QueryBundle) -> SearchResultCollection:
        
        if not search_results.results:
            return search_results
        
        if not self.args.statement_pruning_factor and not self.args.statement_pruning_threshold:
            return search_results
        
        max_statement_score = max([
                statement.score
                for search_result in search_results.results
                for topic in search_result.topics
                for statement in topic.statements
            ])
        
        min_threshold = max_statement_score * self.args.statement_pruning_factor

        min_threshold = min_threshold if not self.args.statement_pruning_threshold else max(min_threshold, self.args.statement_pruning_threshold)

        def prune_statements(topic:Topic):
            surviving_statements = [
                s 
                for s in topic.statements 
                if s.score >= min_threshold
            ]
            topic.statements = surviving_statements
            return topic

        def prune_search_result(index:int, search_result:SearchResult):
            return self._apply_to_topics(search_result, prune_statements)
        
        return self._apply_to_search_results(search_results, prune_search_result)
        


