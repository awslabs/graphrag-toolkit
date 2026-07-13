# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, List, Dict, Optional, Callable, Union, Iterable, Generator

from .graph_operation import GraphOperation

def _default_params_adapter(v):

    def _dedup(parameters:List):
        params_map = {}
        for p in parameters:
            params_map[str(p).lower()] = p
        return list(params_map.values())
    
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        return {'params': _dedup(v)}
    if isinstance(v, Generator):
        params = _dedup([p for p in v])
        return {'params':params}
    raise ValueError(f'Invalid input parameters. Expected list or dictionary, but received {type(v).__name__}.')

DEFAULT_PARAMS_ADAPTER = _default_params_adapter

class Query():
    def __init__(self, 
                 query:str, 
                 params_adapter:Optional[Callable[[Any], Dict]]=None,
                 child_queries:Optional[List]=None,
                 operation:Optional[GraphOperation]=None):
        self.query = query
        self.params_adapter = params_adapter or DEFAULT_PARAMS_ADAPTER
        self.child_queries = child_queries or []
        self.operation = operation

class Job():
    def __init__(self, query:Query, params:Any):
        self.query = query
        self.params = params
        
    def run(self, graph_store_fn:Callable[[str, Dict], List[Any]], **kwargs):
        parameters = self.query.params_adapter(self.params)
        if self.query.operation is not None:
            kwargs['operation'] = self.query.operation
        else:
            kwargs.pop('tenant_id', None)
        return graph_store_fn(self.query.query, parameters, **kwargs)
        
class QueryTree():
    
    def __init__(self, name:str, root_query:Query):
        self.id = f'query-tree-{name}'
        self.root_query = root_query
        
    def run(self, params, graph_store_fn:Callable[[str, Dict], List[Any]], **kwargs) -> Iterable[Any]:
        
        job_queue = []
        
        job = Job(self.root_query, params)
        
        while job:
        
            results = job.run(graph_store_fn, **kwargs)
            
            if job.query.child_queries:
                for q in job.query.child_queries:
                    job_queue.append(Job(q, results))
            else:
                for r in results:
                    yield r
            
            job = job_queue.pop() if job_queue else None