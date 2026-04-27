# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
import logging
from typing import Optional

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph import to_tenant_id, TenantIdType, TenantId
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType

from llama_index.core.prompts import PromptTemplate

logger = logging.getLogger(__name__)


GRAPH_SUMMARY_PROMPT = '''
You are an expert AI assistant specialising in knowledge graphs. Based on any supplied hints, togther with the provided entities and property graph path descriptions, your task is to summarize the domain, scope and uses of a knowledge graph so that it can be utilized by a Model Context Protocol client. Include a list of up to 10 different questions the graph can be used to answer. Provide an authoritative response: do not use words such as 'appears to' or 'seems'. In your response describe the graph as a 'knowledge base', not a graph.

<hints>
{hint}
<hints>

<entities>
{entities}
</entities>

<propertyGraphPaths>
{paths}
</propertyGraphPaths>

## Response format (separate each section with a new line)

Domain: <Headline description of the domain>
Scope: <The knowledge base covers...>
Uses: <List valuable uses>
Example questions: <10 example questions>
'''

def get_domain(s):
    for line in s.split('\n'):
        if line.startswith('Domain:'):
            return line[7:].strip()
    return None

ONE_HOUR_SECONDS = 1 * 60 * 60

class GraphSummary():

    def __init__(self, graph_store:GraphStore, llm:LLMCacheType=None, prompt_template=None, use_cached_entries=True):
        self.graph_store = graph_store
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache
        )
        self.prompt_template=prompt_template or GRAPH_SUMMARY_PROMPT
        self.use_cached_entries = use_cached_entries
        
    def _get_entities(self, tenant_id:TenantId):
        
        graph_store = MultiTenantGraphStore.wrap(
            self.graph_store,
            tenant_id
        )
        
        cypher = '''MATCH (n:`__Entity__`)-[r:`__SUBJECT__`]->()
        WITH n, count(r) AS score ORDER BY score DESC LIMIT 100
        RETURN n.value + ' [' + n.class + ']' as entity'''
        
        results = graph_store.execute_query(cypher)
        
        return results
        
    def _get_paths(self, tenant_id:TenantId):
        
        tenant_id = to_tenant_id(tenant_id)
        graph_store = MultiTenantGraphStore.wrap(
            self.graph_store,
            tenant_id
        )
        
        cypher = '''MATCH (n:`__SYS_Class__`) 
        WITH n, n.count AS score ORDER BY score DESC LIMIT 10
        MATCH p=(n)-[r]->()
        WITH nodes(p) AS nodes, relationships(p) AS rels, r.count AS score ORDER BY score DESC LIMIT 100
        RETURN '(' + nodes[0].value + ')-[' + rels[0].value + ']->(' + nodes[1].value + ')' AS path'''
        
        results = graph_store.execute_query(cypher)
        
        return results
        
    def _generate_summary(self, entities:str, paths:str, description:Optional[str]=None):
        return self.llm.predict(
            PromptTemplate(template=self.prompt_template),
            entities=entities,
            paths=paths,
            hint=description or ''
        )
    
    def _get_cached_summary(self, tenant_id:TenantId):
        
        cypher = f'''MATCH (n:`__SYS_Tenant__`)
        WHERE {self.graph_store.node_id("n.tenantId")} = $tenantId
        RETURN n.value AS summary, n.last_updated_datetime AS last_updated_datetime
        '''

        parameters = {
            'tenantId': f'sys_tenant:{tenant_id}'
        }

        results = self.graph_store.execute_query(cypher, parameters)

        if not results:
            logger.debug(f"No cached summary found for '{tenant_id}'")
            return None
        
        result = results[0]

        time_now = int(time.time())

        if (int(result['last_updated_datetime']) + ONE_HOUR_SECONDS) < time_now:
            logger.debug(f"Cached summary for '{tenant_id}' is stale")
            return None
        
        logger.debug(f"Returning cached summary for '{tenant_id}'")

        return result['summary']
    
    def _cache_summary(self, tenant_id:TenantId, summary:str):

        logger.debug(f"Caching summary for '{tenant_id}'")

        time_now = int(time.time())

        cypher = f'''MERGE (n:`__SYS_Tenant__`{{{self.graph_store.node_id("tenantId")}: $tenantId}})
        ON CREATE SET n.value = $summary, n.last_updated_datetime = $lastUpdatedDatetime
        ON MATCH SET n.value = $summary, n.last_updated_datetime = $lastUpdatedDatetime
        '''

        parameters = {
            'tenantId': f'sys_tenant:{tenant_id}',
            'summary': summary,
            'lastUpdatedDatetime': time_now
        }

        self.graph_store.execute_query_with_retry(cypher, parameters)
        
    def create_graph_summary(self, tenant_id:TenantIdType=None, description:Optional[str]=None, refresh:Optional[bool]=False):

        tenant_id = to_tenant_id(tenant_id)

        if self.use_cached_entries and not refresh:
            summary = self._get_cached_summary(tenant_id)
            if summary is not None:
                return summary
            
        logger.debug(f"Creating new summary for '{tenant_id}'")
            
        entities = '\n'.join([
            result['entity'] for result in self._get_entities(tenant_id)
        ])
        
        paths = '\n'.join([
            result['path'] for result in self._get_paths(tenant_id)
        ])

        if entities and paths:
            summary = self._generate_summary(entities, paths, description)
            self._cache_summary(tenant_id, summary)
            return summary
        else:
            return None