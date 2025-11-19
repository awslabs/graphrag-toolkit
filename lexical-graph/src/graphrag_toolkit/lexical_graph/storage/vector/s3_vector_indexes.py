# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import numpy as np
import time
import boto3

from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from typing import List, Sequence, Dict, Any, Optional, Callable
from urllib.parse import urlparse

from graphrag_toolkit.lexical_graph.metadata import FilterConfig, type_name_for_key_value, format_datetime
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO, TIMESTAMP_LOWER_BOUND, TIMESTAMP_UPPER_BOUND
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig, EmbeddingType
from graphrag_toolkit.lexical_graph.storage.vector import VectorIndex, to_embedded_query
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

from llama_index.core.schema import BaseNode, QueryBundle
from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.indices.utils import embed_nodes
from llama_index.core.vector_stores.types import FilterCondition, FilterOperator, MetadataFilter, MetadataFilters

VECTOR_DIMENSION = 384
DISTANCE_METRIC = 'cosine'
VECTOR_DATA_TYPE = 'float32'
DEFAULT_MAX_RESULTS = 5
DEFAULT_BATCH_SIZE = 25
MAX_METADATA_TAGS = 10  # AWS S3 Vector Store limit for total metadata tags

logger = logging.getLogger(__name__) 

class MetadataLimitExceededError(ValueError):
   def __init__(self, vector_id: str, actual_count: int, max_allowed: int):
        self.vector_id = vector_id
        self.actual_count = actual_count
        self.max_allowed = max_allowed
        super().__init__(
            f"Vector '{vector_id}' has {actual_count} metadata tags, "
            f"but maximum allowed is {max_allowed}"
        )


def validate_metadata_limits(metadata: Dict[str, Any], max_tags: int, vector_id: str) -> None:
    if not isinstance(metadata, dict):
        raise TypeError(f"Vector '{vector_id}' metadata must be a dictionary, got {type(metadata)}")
    
    actual_count = len(metadata)
    if actual_count > max_tags:
        raise MetadataLimitExceededError(vector_id, actual_count, max_tags)


def create_vector_bucket(s3_vectors_client, bucket_name:str) -> str:

    try:
        # Check if vector bucket already exists using S3 Vectors API
        s3_vectors_client.get_vector_bucket(vectorBucketName=bucket_name)
        logger.debug(f"Using existing vector bucket: {bucket_name}")
        return bucket_name
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            logger.error(f"Error while creating vector bucket: {str(e)}")
            raise
    
    # Create vector bucket using S3 Vectors API
    try:
        s3_vectors_client.create_vector_bucket(
            vectorBucketName=bucket_name
        )
        logger.debug(f"Created new bucket: {bucket_name}")
    except ClientError as create_error:
        if create_error.response['Error']['Code'] == 'ConflictException':
            logger.debug(f"Using existing bucket: {bucket_name}")
        else:
            raise
    
    return bucket_name


def create_vector_index(s3_vectors_client, bucket_name: str, index_name: str, 
                       dimension: int = None, distance_metric: str = None) -> str:
       
    if dimension is None:
        dimension = VECTOR_DIMENSION
    if distance_metric is None:
        distance_metric = DISTANCE_METRIC
    
    try:
        s3_vectors_client.create_index(
            vectorBucketName=bucket_name,
            indexName=index_name,
            dimension=dimension,
            distanceMetric=distance_metric.lower(),
            dataType='float32',
            metadataConfiguration={
                'nonFilterableMetadataKeys': []
            }
        )
        logger.debug(f"Created index: {index_name}")
    except ClientError as index_error:
        if index_error.response['Error']['Code'] == 'ConflictException':
            logger.debug(f"Using existing index: {index_name}")
        else:
            raise
    return index_name


def ingest_vectors(s3_vectors_client, bucket_name: str, index_name: str, 
                  vector_data: List[Dict[str, Any]], batch_size: int = None) -> Dict[str, Any]:
     
    # Use default batch size if not provided
    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE
    
    # Validate metadata limits for all vectors before ingestion
    for vector_item in vector_data:
        vector_id = vector_item.get('id', 'unknown')
        metadata = vector_item.get('metadata', {})
        validate_metadata_limits(metadata, MAX_METADATA_TAGS, vector_id)
    
    successful_ingestions = 0
    failed_ingestions = 0
    ingestion_errors = []
    ingested_vector_ids = []
    
    # Process in batches
    for i in tqdm(range(0, len(vector_data), batch_size), desc="Ingesting vectors"):
        batch = vector_data[i:i + batch_size]
        
        # Prepare batch for ingestion - using correct format from AWS docs
        batch_vectors = []
        for vector_item in batch:
            vector_entry = {
                'key': vector_item['id'],
                'data': {'float32': vector_item['vector']},  # Correct format from AWS docs
                'metadata': vector_item['metadata']
            }
            batch_vectors.append(vector_entry)
        
        # S3 Vectors API call
        try:
            response = s3_vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=batch_vectors
            )
            
            # All vectors in batch are successful if no exception
            successful_ingestions += len(batch)
            ingested_vector_ids.extend([v['id'] for v in vector_data[i:i + batch_size]])
            
        except Exception as e:
            print(f"Ingestion failure: {e}")
            failed_ingestions += len(batch)
            error_msg = f"Batch {i//batch_size + 1} failed: {str(e)}"
            ingestion_errors.append(error_msg)
    
    # Summary
    total_vectors = len(vector_data)
    success_rate = (successful_ingestions / total_vectors) * 100 if total_vectors > 0 else 0
    
    return {
        'total_vectors': total_vectors,
        'successful_ingestions': successful_ingestions,
        'failed_ingestions': failed_ingestions,
        'success_rate': success_rate,
        'errors': ingestion_errors,
        'ingested_vector_ids': ingested_vector_ids
    }


def search_vectors(s3_vectors_client, bucket_name: str, index_name: str, 
                  query_vector: List[float], metadata_filters: Dict[str, Any] = None,
                  max_results: int = 10) -> List[Dict[str, Any]]:
    """Perform similarity search on vectors."""
    # Prepare query parameters - based on AWS documentation examples
    query_params = {
        'vectorBucketName': bucket_name,
        'indexName': index_name,
        'queryVector': {'float32': query_vector},  # Correct format from AWS docs
        'topK': max_results,
        'returnDistance': True,
        'returnMetadata': True
    }
    
    # Add filter only if provided - using simple key-value format from AWS docs
    if metadata_filters:
        query_params['filter'] = metadata_filters
    
    response = s3_vectors_client.query_vectors(**query_params)
    
    # Process response
    results = []
    if 'vectors' in response:
        for vector_result in response['vectors']:
            result = {
                'id': vector_result['key'],
                'similarity_score': 1.0 - vector_result.get('distance', 0.0),  # Convert distance to similarity
                'metadata': vector_result.get('metadata', {}),
                'distance': vector_result.get('distance', 0.0)
            }
            results.append(result)
    
    return results

def get_vectors(s3_vectors_client, bucket_name: str, index_name: str, ids:List[str]=[]) -> List[Dict[str, Any]]:

    response = s3_vectors_client.get_vectors(
        vectorBucketName=bucket_name,
        indexName=index_name,
        keys=ids,
        returnData=True,
        returnMetadata=True
    )

    results = []
    if 'vectors' in response:
        for vector_result in response['vectors']:
            result = {
                'id': vector_result['key'],
                'embedding': vector_result.get('data', {}).get('float32', []),
                'metadata': vector_result.get('metadata', {})
            }
            results.append(result)
    
    return results


def delete_vectors(s3_vectors_client, bucket_name: str, index_name: str, 
                  vector_ids: List[str]) -> Dict[str, Any]:
    """Delete specific vectors by their IDs."""
    try:
        response = s3_vectors_client.delete_vectors(
            vectorBucketName=bucket_name,
            indexName=index_name,
            keys=vector_ids
        )
        
        # S3 Vectors delete API returns success if no exception
        successful_deletions = len(vector_ids)
        failed_deletions = 0
        deletion_errors = []
        
    except Exception as e:
        successful_deletions = 0
        failed_deletions = len(vector_ids)
        deletion_errors = [f"Delete operation failed: {str(e)}"]
    
    total_requested = len(vector_ids)
    success_rate = (successful_deletions / total_requested) * 100 if total_requested > 0 else 0
    
    return {
        'total_requested': total_requested,
        'successful_deletions': successful_deletions,
        'failed_deletions': failed_deletions,
        'success_rate': success_rate,
        'errors': deletion_errors
    }

class S3VectorIndex(VectorIndex):
    @staticmethod
    def for_index(index_name, bucket_name, prefix=None, embed_model=None, dimensions=None, **kwargs):
        return S3VectorIndex(
            index_name=index_name, 
            bucket_name=bucket_name, 
            prefix=prefix, 
            embed_model=embed_model, 
            dimensions=dimensions
        )

    _client:Optional[Any] = PrivateAttr(default=None)
    initialized:bool=False
    bucket_name:str
    prefix:Optional[str]=None
    embed_model: Any
    dimensions: int
        
    def __getstate__(self):
        self._client = None
        return super().__getstate__()

    @property
    def client(self):
        if self._client is None:
            session = GraphRAGConfig.session
            self._client = session.client('s3vectors')
            self._init_index(self._client)
        return self._client
    
    def underlying_index_name(self) -> str:
        underlying_index_name = super().underlying_index_name().replace('_', '-')
        underlying_index_name = underlying_index_name if not self.prefix else f'{self.prefix}.{underlying_index_name}'
        if len(underlying_index_name) > 63:
            raise ValueError(f'Vector index names must be between 3 and 63 characters long: {underlying_index_name}')
        return underlying_index_name
        
    def _init_index(self, client):
        if not self.initialized:
            create_vector_bucket(client, self.bucket_name)
            create_vector_index(client, self.bucket_name, self.underlying_index_name(), dimension=self.dimensions)
            self.initialized = True

    def add_embeddings(self, nodes:Sequence[BaseNode]) -> Sequence[BaseNode]:

        if not self.writeable:
            raise IndexError(f'Index {self.index_name} is read-only')
        
        id_to_embed_map = embed_nodes(
            nodes, self.embed_model
        )

        vector_data = [
            {
                'id': node.id_,
                'vector': id_to_embed_map[node.node_id],
                'metadata': node.metadata
            }
            for node in nodes
        ]

        ingest_vectors(self.client, self.bucket_name, self.underlying_index_name(), vector_data)

        return nodes
    
    def _to_top_k_result(self, r):
        
        result = {
            'score': r['similarity_score']
        }

        if INDEX_KEY in r['metadata']:
            index_name = r['metadata'][INDEX_KEY]['index']
            result[index_name] = r['metadata'][index_name]
            if 'source' in r['metadata']:
                result['source'] = r['metadata']['source']
        else:
            for k,v in r['metadata'].items():
                result[k] = v
            
        return result
    
    def _to_embeddings_result(self, r):

        result = {
            'id': r['id'],
            'embedding': r['embedding'],
            'value': None
        }

        for k,v in r['metadata'].items():
            if k != INDEX_KEY:
                result[k] = v
            
        return result
    

    def top_k(self, query_bundle:QueryBundle, top_k:int=5, filter_config:Optional[FilterConfig]=None) -> Sequence[Dict[str, Any]]:
        
        query_bundle = to_embedded_query(query_bundle, self.embed_model)

        search_results = search_vectors(self.client, self.bucket_name, self.underlying_index_name(), query_vector=query_bundle.embedding, max_results=top_k)

        top_k_results = [
            self._to_top_k_result(r)
            for r in search_results
        ]

        return top_k_results

    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Dict[str, Any]]:
        
        vectors = get_vectors(self.client, self.bucket_name, self.underlying_index_name(), ids)

        get_embeddings_results = [
            self._to_embeddings_result(r)
            for r in vectors
        ]
        
        return get_embeddings_results
    
    def update_versioning(self, versioning_timestamp:int, ids:List[str]=[]) -> List[str]:
        raise NotImplementedError
    
    def enable_for_versioning(self, ids:List[str]=[]) -> List[str]:
        raise NotImplementedError
