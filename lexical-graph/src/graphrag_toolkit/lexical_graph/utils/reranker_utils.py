# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import statistics
import numpy
import tfidf_matcher as tm
from typing import List, Optional
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

def to_float(v):
    if isinstance(v, numpy.float64) or isinstance(v, numpy.float32):
        return v.item()
    else:
        return v

def score_values_with_tfidf(values:List[str], 
                 match_values:List[str], 
                 limit:Optional[int]=None, 
                 ngram_length:Optional[int]=3, 
                 num_primary_match_values:Optional[int]=None):
        
        values_to_score = values.copy()
        
        num_match_values = len(match_values)
        num_primary_match_values = coalesce(num_primary_match_values, num_match_values)
        max_num_values_to_score =  len(values_to_score)
        
        def calculate_ranked_score(row_index, score):
            multiplier = 1.0 if row_index < num_primary_match_values else 0.9
            return score * multiplier
        
        if limit:
            max_num_values_to_score = min(limit, max_num_values_to_score)

        while len(values_to_score) <= max_num_values_to_score:
            values_to_score.append('')

        scored_values = {}

        try:
            
            matcher_results = tm.matcher(match_values, values_to_score, max_num_values_to_score, ngram_length)

            max_i = len(matcher_results.columns)
        
            for row_index in range(0, num_match_values):
                for col_index in range(1, max_i, 3) :
                    value = matcher_results.iloc[row_index, col_index]
                    base_score = matcher_results.iloc[row_index, col_index+1]
                    score = calculate_ranked_score(row_index, base_score) if base_score > 0.0 else base_score
                    if value not in scored_values:
                        scored_values[value] = [score]
                    else:
                        scored_values[value].append(score)
                                         
        except ValueError:
            scored_values = {v: [0.0] for v in values_to_score if v}
            
        scored_values = { k: to_float(statistics.mean(v)) for k,v in scored_values.items() }  
        sorted_scored_values = dict(sorted(scored_values.items(), key=lambda item: item[1], reverse=True))
        
        return sorted_scored_values