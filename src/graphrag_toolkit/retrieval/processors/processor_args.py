# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any
from dateutil.parser import parse

from graphrag_toolkit.retrieval.model import Source

def format_source_metadata(source:Source):
    def format_value(s):
        try: 
            date = parse(s, fuzzy=False)
            return date.strftime("%B %-d, %Y")

        except ValueError:
            if s.startswith('http'):
                return ''
            else:
                return s
    return ', '.join([format_value(v) for _,v in source.metadata])

class ProcessorArgs():
    def __init__(self, **kwargs):
        
        self.expand_entities = kwargs.get('expand_entities', True)
        self.include_facts = kwargs.get('include_facts', False)
        self.derive_subqueries = kwargs.get('derive_subqueries', False)
        self.strip_source_metadata = kwargs.get('strip_source_metadata', True) 
        self.debug_results = kwargs.get('debug_results', [])
        self.reranker = kwargs.get('reranker', 'tfidf')
        self.max_statements = kwargs.get('max_statements', 100)
        self.max_search_results = kwargs.get('max_search_results', 20)
        self.max_statements_per_topic = kwargs.get('max_statements_per_topic', 10)
        self.max_keywords = kwargs.get('max_keywords', 10)
        self.max_subqueries = kwargs.get('max_subqueries', 2) 
        self.intermediate_limit = kwargs.get('intermediate_limit', 50)
        self.query_limit = kwargs.get('query_limit', 10)  
        self.vss_top_k = kwargs.get('vss_top_k', 10)
        self.vss_diversity_factor = kwargs.get('vss_diversity_factor', 5)
        self.statement_pruning_threshold = kwargs.get('statement_pruning_threshold', 0.01)
        self.results_pruning_threshold = kwargs.get('results_pruning_threshold', 0.08)
        self.num_workers = kwargs.get('num_workers', 10)
        self.format_source_metadata_fn = kwargs.get('format_source_metadata_fn', format_source_metadata)
  
    def to_dict(self, new_args:Dict[str, Any]={}):
        args = self.__dict__
        return args | new_args
    
    def __repr__(self):
        return str(self.to_dict())