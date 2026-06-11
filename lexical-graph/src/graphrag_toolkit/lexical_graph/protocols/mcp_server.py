# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import json
from typing import List, Dict, Any, Annotated, Optional, Union, Literal, Protocol, Tuple, runtime_checkable
from pydantic import Field

from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph import TenantId, TenantIdType, to_tenant_id, DEFAULT_TENANT_ID, DEFAULT_TENANT_NAME
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.summary import GraphSummary, get_domain

from graphrag_toolkit.core.response import StreamingResponse
from fastmcp.tools.tool_transform import ArgTransform
from fastmcp.utilities.types import NotSet

logger = logging.getLogger(__name__)


@runtime_checkable
class UpdateParametersFunction(Protocol):

    def __call__(self, tool_params:Dict[str,Any], query_engine_params:Dict[str,Any]) -> None:
        pass


class ToolParameters():

    def __init__(
            self, 
            parameters:List[ArgTransform]=[],
            update_params_function:UpdateParametersFunction = lambda _, query_engine_params: query_engine_params
        ):

        if len(parameters) > 3:
            raise ValueError('Maximum number of tool parameters exceeded. You can only supply up to 3 tool parameters.')
        
        self.parameters = parameters
        self.update_params_function = update_params_function

        for p in self.parameters:
            if p.name == NotSet:
                raise ValueError('Tool parameter name missing')
            p.required = True
            p.hide = False

class ToolParametersTransform():
    def __init__(self, transform_args:Dict[str,ArgTransform]={}):
        self.transform_args = transform_args

    def transform_params(self, query:str, query_method:Literal['retrieve', 'query'], _arg_1:Any, _arg_2:Any, _arg_3:Any):
        
        params = {
            'query': query,
            'query_method': query_method,
        }

        if '_arg_1' in self.transform_args:
            params[self.transform_args['_arg_1'].name] = _arg_1
        if '_arg_2' in self.transform_args:
            params[self.transform_args['_arg_2'].name] = _arg_2
        if '_arg_3' in self.transform_args:
            params[self.transform_args['_arg_3'].name] = _arg_3

        logger.debug(f'tool_params: {params}')
        
        return params

def tool_search(graph_store:GraphStore, tenant_ids:List[TenantId]):

    def search_for_tool(search_term: Annotated[str, Field(description='Entity, concept or phrase for which one or more tools are to be found')]) -> List[str]:
        
        permitted_tools = [str(tenant_id) for tenant_id in tenant_ids]
        
        cypher = '''MATCH (n) 
        WHERE n.search_str STARTS WITH $search_term
        RETURN DISTINCT labels(n) AS lbls
        '''

        properties = {
            'search_term': search_term.lower()
        }

        results = graph_store.execute_query(cypher, properties)

        tool_names = set()

        for result in results:
            for label in result['lbls']:
                parts = label.split('__')
                if len(parts) == 4:
                    tool_names.add(parts[2])
                elif len(parts) == 3:
                    tool_names.add(str(DEFAULT_TENANT_ID))

        tools = [
            t 
            for t in list(tool_names) 
            if t in permitted_tools
        ]

        logger.debug(f'{search_term}: {tools}')

        return tools

    return search_for_tool

def query_tenant_graph(
        graph_store:GraphStore, 
        vector_store:VectorStore, 
        tenant_id:TenantId, 
        domain:str, 
        tool_params_transform:ToolParametersTransform, 
        update_params:UpdateParametersFunction, 
        **kwargs
    ):
    
    description = f'A natural language query related to the domain of {domain}' if domain else 'A natural language query'
    
    def query_graph(
            query: Annotated[str, Field(description=description)], 
            _arg_1: Annotated[Optional[str], Field(description='Placeholder', default=None)],
            _arg_2: Annotated[Optional[str], Field(description='Placeholder', default=None)],
            _arg_3: Annotated[Optional[str], Field(description='Placeholder', default=None)]
        ) -> List[Dict[str, Any]]:

        query_engine_kwargs = kwargs.copy()
        query_engine_kwargs['enable_multipart_queries'] = query_engine_kwargs.pop('enable_multipart_queries', True)

        tool_params = tool_params_transform.transform_params(query, 'retrieve', _arg_1, _arg_2, _arg_3)

        update_params(tool_params, query_engine_kwargs)
        
        query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
            graph_store, 
            vector_store,
            tenant_id=tenant_id,
            **query_engine_kwargs
        )

        start = time.time()

        if tool_params['query_method'] == 'retrieve':

            response = query_engine.retrieve(tool_params['query'])
            results = [json.loads(n.text) for n in response]

        else:

            response = query_engine.query(tool_params['query'])

            if isinstance(response, StreamingResponse):
                response = response.get_response()
            
            results = [{
                'text': response.response
            }]
            
        end = time.time()

        logger.debug(f'[{tenant_id}]: {query} [{len(results)} results, {int((end-start) * 1000)} millis]')

        return results
        
    return query_graph

def get_tenant_ids(graph_store:GraphStore):

    cypher = '''MATCH (n)<-[:`__EXTRACTED_FROM__`]-()
    WITH DISTINCT labels(n) as lbls
    WITH split(lbls[0], '__') AS lbl_parts WHERE size(lbl_parts) > 2
    WITH lbl_parts WHERE lbl_parts[1] = 'Source' AND lbl_parts[2] <> ''
    RETURN DISTINCT lbl_parts[2] AS tenant_id
    '''

    results = graph_store.execute_query(cypher)

    return [result['tenant_id'] for result in results]

TenantConfigType = Union[List[TenantIdType], Dict[str, Dict[str, Any]]]

"""
Config: 

{
    '<tenant_id>': {
        'description': '<short description - optional>',
        'refresh': True|False,
        'tool_parameters': ToolParameters(),
        'query_engine_args': {
            LexicalGraphQueryEngine kwargs
        }
    }
}
"""

def create_mcp_server(graph_store:GraphStore, vector_store:VectorStore, tenant_ids:Optional[TenantConfigType]=None, **kwargs):

    try:
        from fastmcp import FastMCP
        from fastmcp.tools import Tool
    except ImportError as e:
        raise ImportError(
            "fastmcp package not found, install with 'pip install fastmcp'"
        ) from e

    mcp = FastMCP(name='LexicalGraphServer')

    graph_summary = GraphSummary(graph_store)

    tenant_id_configs = {}

    if not tenant_ids:
        tenant_id_configs[DEFAULT_TENANT_NAME] = {}
        tenant_id_configs.update({tenant_id:{} for tenant_id in get_tenant_ids(graph_store)})
    else:
        if isinstance(tenant_ids, list):
            tenant_id_configs.update({str(tenant_id):{} for tenant_id in tenant_ids})
        else:
            tenant_id_configs.update(tenant_ids)
    
    for tenant_id_name, tenant_id_config in tenant_id_configs.items():

        tenant_id = to_tenant_id(tenant_id_name)

        refresh = tenant_id_config.get('refresh', False)
        
        summary = graph_summary.create_graph_summary(tenant_id, tenant_id_config.get('description', ''), refresh=refresh)

        if summary:

            domain = get_domain(summary)

            query_engine_args = kwargs.copy()
            query_engine_args.update(tenant_id_config.get('query_engine_args', {}))

            tool_parameters:ToolParameters = tenant_id_config.get('tool_parameters', ToolParameters())
            transform_args = {
                f'_arg_{i + 1}': arg_transform
                for i, arg_transform in enumerate(tool_parameters.parameters)
            }
            tool_params_transform = ToolParametersTransform(transform_args)

            logger.debug(f'Adding tool: [tenant_id: {tenant_id}, domain: {domain}, tool_params: {[a.name for a in transform_args.values()]}, query_engine_args: {query_engine_args}]')

            tool = Tool.from_function(
                fn=query_tenant_graph(
                    graph_store, 
                    vector_store, 
                    tenant_id, 
                    domain, 
                    tool_params_transform,
                    tool_parameters.update_params_function,
                    **query_engine_args
                ),
                name = str(tenant_id),
                description = summary
            )

            if tool_parameters.parameters:
                tool = Tool.from_tool(tool, transform_args=transform_args)

            mcp.add_tool(tool)

    if tenant_ids:
        mcp.add_tool(
            Tool.from_function(
                fn=tool_search(graph_store, tenant_ids),
                name = 'search_',
                description = 'Given a search term, returns the name of one or more tools that can be used to provide information about the search term. Use this tool to help find other tools that can answer a query.'
            )        
        )

    return mcp
