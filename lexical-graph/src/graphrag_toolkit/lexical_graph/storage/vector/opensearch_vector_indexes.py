# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import logging
from typing import List, Any, Optional
from dataclasses import dataclass

from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.schema import BaseNode, NodeWithScore, QueryBundle
from llama_index.core.async_utils import asyncio_run
from llama_index.core.vector_stores.types import  VectorStoreQueryResult, VectorStoreQueryMode, MetadataFilters
from llama_index.core.indices.utils import embed_nodes

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig, EmbeddingType
from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, to_embedded_query
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

logger = logging.getLogger(__name__)

try:
    from llama_index.vector_stores.opensearch import OpensearchVectorClient
    from opensearchpy.exceptions import NotFoundError, RequestError
    from opensearchpy import AWSV4SignerAsyncAuth, AsyncHttpConnection
    from opensearchpy import Urllib3AWSV4SignerAuth, Urllib3HttpConnection
    from opensearchpy import OpenSearch, AsyncOpenSearch
except ImportError as e:
    raise ImportError(
        "opensearch-py and/or llama-index-vector-stores-opensearch packages not found, install with 'pip install opensearch-py llama-index-vector-stores-opensearch'"
    ) from e


def _get_opensearch_version(self) -> str:
    #info = asyncio_run(self._os_async_client.info())
    return '2.0.9'

import llama_index.vector_stores.opensearch 
llama_index.vector_stores.opensearch.OpensearchVectorClient._get_opensearch_version = _get_opensearch_version

@dataclass
class DummyAuth:
    service:str

def create_os_client(endpoint, **kwargs):

    session = GraphRAGConfig.session
    region = GraphRAGConfig.aws_region
    credentials = session.get_credentials()
    service = 'aoss'

    auth = Urllib3AWSV4SignerAuth(credentials, region, service)

    return OpenSearch(
        hosts=[endpoint],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=Urllib3HttpConnection,
        timeout=300,
        max_retries=10,
        retry_on_timeout=True,
        **kwargs
    )

def create_os_async_client(endpoint, **kwargs):

    session = GraphRAGConfig.session
    region = GraphRAGConfig.aws_region
    credentials = session.get_credentials()
    service = 'aoss'

    auth = AWSV4SignerAsyncAuth(credentials, region, service)

    return AsyncOpenSearch(
        hosts=[endpoint],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=AsyncHttpConnection,
        timeout=300,
        max_retries=10,
        retry_on_timeout=True,
        **kwargs
    )
    

def index_exists(endpoint, index_name, dimensions, writeable) -> bool:

    client = create_os_client(endpoint, pool_maxsize=1)

    embedding_field = 'embedding'
    method = {
        "name": "hnsw",
        "space_type": "l2",
        "engine": "nmslib",
        "parameters": {"ef_construction": 256, "m": 48},
    }

    idx_conf = {
        "settings": {"index": {"knn": True, "knn.algo_param.ef_search": 100}},
        "mappings": {
            "properties": {
                embedding_field: {
                    "type": "knn_vector",
                    "dimension": dimensions,
                    "method": method,
                },
            }
        }
    }

    index_exists = False

    try:
        index_exists = client.indices.exists(index_name)
        if not index_exists and writeable:
            logger.debug(f'Creating OpenSearch index [index_name: {index_name}, endpoint: {endpoint}]')
            client.indices.create(index=index_name, body=idx_conf)
            index_exists = True
    except RequestError as e:
        if e.error == 'resource_already_exists_exception':
            pass
        else:
            logger.exception('Error creating an OpenSearch index')
    finally:
        client.close()

    return index_exists
        
    
def create_opensearch_vector_client(endpoint, index_name, dimensions, embed_model):
        
    text_field = 'value'
    embedding_field = 'embedding'

    logger.debug(f'Creating OpenSearch vector client [endpoint: {endpoint}, index_name={index_name}, embed_model={embed_model}, dimensions={dimensions}]')
 
    client = None
    retry_count = 0
    while not client:
        try:
            client = OpensearchVectorClient(
                endpoint, 
                index_name, 
                dimensions, 
                embedding_field=embedding_field, 
                text_field=text_field, 
                os_client=create_os_client(endpoint),
                os_async_client=create_os_async_client(endpoint),
                http_auth=DummyAuth(service='aoss')
            )
        except NotFoundError as err:
            retry_count += 1
            logger.warning(f'Error while creating OpenSearch vector client [retry_count: {retry_count}, error: {err}]')
            if retry_count > 3:
                raise err
                
    logger.debug(f'Created OpenSearch vector client [client: {client}, retry_count: {retry_count}]')
            
    return client
        
class DummyOpensearchVectorClient():
    def __init__(self):
        self._os_async_client = None

    async def aindex_results(self, nodes: List[BaseNode], **kwargs: Any) -> List[str]:
        return []
    async def aquery(
        self,
        query_mode: VectorStoreQueryMode,
        query_str: Optional[str],
        query_embedding: List[float],
        k: int,
        filters: Optional[MetadataFilters] = None,
    ) -> VectorStoreQueryResult:
        return VectorStoreQueryResult(nodes=[], similarities=[], ids=[])
        
    
class OpenSearchIndex(VectorIndex):

    @staticmethod
    def for_index(index_name, endpoint, embed_model=None, dimensions=None):
        
        embed_model = embed_model or GraphRAGConfig.embed_model
        dimensions = dimensions or GraphRAGConfig.embed_dimensions

        return OpenSearchIndex(index_name=index_name, endpoint=endpoint, dimensions=dimensions, embed_model=embed_model)
    
    class Config:
        arbitrary_types_allowed = True

    endpoint:str
    index_name:str
    dimensions:int
    embed_model:EmbeddingType

    _client: OpensearchVectorClient = PrivateAttr(default=None)

    def __getstate__(self):
        self._client = None
        return super().__getstate__()
        
    @property
    def client(self) -> OpensearchVectorClient:
        if not self._client:
            if index_exists(self.endpoint, self.underlying_index_name(), self.dimensions, self.writeable):
                self._client = create_opensearch_vector_client(
                    self.endpoint, 
                    self.underlying_index_name(), 
                    self.dimensions, 
                    self.embed_model
                )
            else:
                self._client = DummyOpensearchVectorClient()
        return self._client
    
    def __del__(self):
        if self._client and isinstance(self._client, OpensearchVectorClient):
            asyncio_run(self._client._os_async_client.close())
        
    def _clean_id(self, s):
        return ''.join(c for c in s if c.isalnum())
    
    def _to_top_k_result(self, r):
        
        result = {
            'score': r.score 
        }

        if INDEX_KEY in r.metadata:
            index_name = r.metadata[INDEX_KEY]['index']
            result[index_name] = r.metadata[index_name]
            if 'source' in r.metadata:
                result['source'] = r.metadata['source']
        else:
            for k,v in r.metadata.items():
                result[k] = v
            
        return result
        
    def _to_get_embedding_result(self, hit):
        
        source = hit['_source']
        data = json.loads(source['metadata']['_node_content'])

        result = {
            'id': source['id'],
            'value': source['value'],
            'embedding': source['embedding']
        }

        for k,v in data['metadata'].items():
            if k != INDEX_KEY:
                result[k] = v
            
        return result

    def add_embeddings(self, nodes):

        if not self.writeable:
            raise IndexError(f'Index {self.index_name()} is read-only')
        
        async def aadd_embeddings(nodes):
                    
            id_to_embed_map = embed_nodes(
                nodes, self.embed_model
            )

            docs = []

            for node in nodes:

                doc:BaseNode = node.copy()
                doc.embedding = id_to_embed_map[node.node_id]

                docs.append(doc)

            if docs:
                await self.client.aindex_results(docs)
            
        asyncio_run(aadd_embeddings(nodes))
        
        return nodes
    
    def top_k(self, query_bundle:QueryBundle, top_k:int=5):
        
        async def atop_k(query_bundle, top_k):
        
            query_bundle = to_embedded_query(query_bundle, self.embed_model)

            scored_nodes = []
            
            try:

                results:VectorStoreQueryResult = await self.client.aquery(
                    VectorStoreQueryMode.DEFAULT, 
                    query_str=query_bundle.query_str, 
                    query_embedding=query_bundle.embedding, 
                    k=top_k
                )

                scored_nodes.extend([
                    NodeWithScore(node=node, score=score) 
                    for node, score in zip(results.nodes, results.similarities)
                ])

            except NotFoundError as e:
                if self.tenant_id.is_default_tenant():
                    raise e
                else:
                    logger.warning(f'Multi-tenant index {self.underlying_index_name()} does not exist')
                
            return scored_nodes

        scored_nodes = asyncio_run(atop_k(query_bundle, top_k))

        return [self._to_top_k_result(node) for node in scored_nodes]

    # opensearch has a limit of 10,000 results per search, so we use this to paginate the search
    async def paginated_search(self, query, page_size=10000, max_pages=None):
        client = self.client._os_async_client

        if not client:
            pass

        search_after = None
        page = 0
        
        while True:
            body = {
                "size": page_size,
                "query": query,
                "sort": [{"_id": "asc"}]
            }
            
            if search_after:
                body["search_after"] = search_after

            response = await client.search(
                index=self.underlying_index_name(),
                body=body
            )

            hits = response['hits']['hits']
            if not hits:
                break

            yield hits

            search_after = hits[-1]['sort']
            page += 1

            if max_pages and page >= max_pages:
                break

    async def get_all_embeddings(self, query:str, max_results=None):
        all_results = []
        
        async for page in self.paginated_search(query, page_size=10000):
            all_results.extend(self._to_get_embedding_result(hit) for hit in page)
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                break
        
        return all_results
    
    def get_embeddings(self, ids:List[str]=[]):

        query = {
            "terms": {
                f'metadata.{INDEX_KEY}.key': [self._clean_id(i) for i in ids]
            }
        }

        results = asyncio_run(self.get_all_embeddings(query, max_results=len(ids) * 2))
        
        return results
