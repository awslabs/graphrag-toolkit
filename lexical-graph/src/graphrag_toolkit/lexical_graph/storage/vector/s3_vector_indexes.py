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

from llama_index.core.schema import BaseNode, TextNode, QueryBundle
from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.indices.utils import embed_nodes
from llama_index.core.vector_stores.types import FilterCondition, FilterOperator, MetadataFilter, MetadataFilters

DISTANCE_METRIC = 'cosine'
VECTOR_DATA_TYPE = 'float32'
MAX_RESULTS = 100
DEFAULT_BATCH_SIZE = 100
MAX_METADATA_TAGS = 50
NON_FILTERABLE_FIELDS = ['value', 'metadata']
SOURCE_METADATA_PREFIX = 'source.metadata.'
SOURCE_VERSIONING_PREFIX = 'source.versioning.'
MAX_KEY_LENGTH = 63

logger = logging.getLogger(__name__) 

def to_s3_operator(operator: FilterOperator) -> tuple[str, Callable[[Any], str]]:

    default_value_formatter = lambda x: x
    
    operator_map = {
        FilterOperator.EQ: ('$eq', default_value_formatter), 
        FilterOperator.GT: ('$gt', default_value_formatter), 
        FilterOperator.LT: ('$lt', default_value_formatter), 
        FilterOperator.NE: ('$ne', default_value_formatter), 
        FilterOperator.GTE: ('$gte', default_value_formatter), 
        FilterOperator.LTE: ('$lte', default_value_formatter), 
        #FilterOperator.IN: ('in', default_value_formatter),  # In array (string or number)
        #FilterOperator.NIN: ('nin', default_value_formatter),  # Not in array (string or number)
        #FilterOperator.ANY: ('any', default_value_formatter),  # Contains any (array of strings)
        #FilterOperator.ALL: ('all', default_value_formatter),  # Contains all (array of strings)
        #FilterOperator.TEXT_MATCH: ('LIKE', lambda x: f"%%{x}%%"),
        #FilterOperator.TEXT_MATCH_INSENSITIVE: ('~*', default_value_formatter),
        #FilterOperator.CONTAINS: ('contains', default_value_formatter),  # metadata array contains value (string or number)
        FilterOperator.IS_EMPTY: ('$exists', default_value_formatter),  # the field is not exist or empty (null or empty array)
    }

    if operator not in operator_map:
        raise ValueError(f'Unsupported filter operator: {operator}')
    
    return operator_map[operator]

def formatter_for_type(type_name:str) -> Callable[[Any], str]:
    
    if type_name == 'text':
        return lambda x: f'"{x}"'
    elif type_name == 'timestamp':
        return lambda x: f'"{format_datetime(x)}"'
    elif type_name in ['int', 'float']:
        return lambda x:x
    else:
        raise ValueError(f'Unsupported type name: {type_name}')


def parse_metadata_filters_recursive(metadata_filters:MetadataFilters) -> str:

    def to_key(key: str) -> str:
        if key == VALID_FROM:
            return f'{SOURCE_VERSIONING_PREFIX}valid_from'      
        elif key == VALID_TO:
            return f'{SOURCE_VERSIONING_PREFIX}valid_to'
        else:
            return f'{SOURCE_METADATA_PREFIX}{key}'
    
    def to_s3_filter(f: MetadataFilter) -> str:
        
        key = to_key(f.key)
        (operator, operator_formatter) = to_s3_operator(f.operator)

        if f.operator == FilterOperator.IS_EMPTY:
            return f'{{"{key}": {{ "{operator}": false }}}}'
        else:
            type_name = type_name_for_key_value(f.key, f.value)
            type_formatter = formatter_for_type(type_name)

        return f'{{"{key}": {{ "{operator}": {type_formatter(operator_formatter(str(f.value)))} }}}}'

    condition = metadata_filters.condition.value

    filter_strs = []

    for metadata_filter in metadata_filters.filters:
        if isinstance(metadata_filter, MetadataFilter):
            if metadata_filters.condition == FilterCondition.NOT:
                raise ValueError(f'Expected MetadataFilters for FilterCondition.NOT, but found MetadataFilter')
            filter_strs.append(to_s3_filter(metadata_filter))
        elif isinstance(metadata_filter, MetadataFilters):
            filter_strs.append(parse_metadata_filters_recursive(metadata_filter))
        else:
            raise ValueError(f'Invalid metadata filter type: {type(metadata_filter)}')
        
    if metadata_filters.condition == FilterCondition.AND:       
        return json.loads(f'{{"$and": [{",".join(filter_strs)}]}}')
    elif metadata_filters.condition == FilterCondition.OR:
        return json.loads(f'{{"$or": [{",".join(filter_strs)}]}}')
    else:
        raise ValueError(f'Unsupported filters condition: {metadata_filters.condition}')


def filter_config_to_s3_filters(filter_config:FilterConfig) -> str:

    if filter_config is None or filter_config.source_filters is None:
        return None
    
    return parse_metadata_filters_recursive(filter_config.source_filters)

def _node_to_s3_vector(id:str, value:str, embedding: List[float], node_metadata:Dict[str, Any]) -> Dict[str, Any]:

    metadata = {
        'value': value
    }

    versioning = node_metadata.get('source', {}).get('versioning', {})

    metadata[f'{SOURCE_VERSIONING_PREFIX}valid_from'] = versioning.pop('valid_from', TIMESTAMP_LOWER_BOUND)
    metadata[f'{SOURCE_VERSIONING_PREFIX}valid_to'] = versioning.pop('valid_to', TIMESTAMP_UPPER_BOUND)

    for k,v in node_metadata.get('source', {}).get('metadata', {}).items():
        metadata[f'{SOURCE_METADATA_PREFIX}{k}'] = v

    node_metadata.get('source', {})['metadata'] = {}

    metadata['metadata'] = json.dumps(node_metadata)

    return {
        'key': id,
        'data': {
            VECTOR_DATA_TYPE: embedding
        },
        'metadata': metadata
    }

def node_to_s3_vector(node:TextNode, embedding:List[float])-> Dict[str, Any]:
    return _node_to_s3_vector(node.id_, node.text, embedding, node.metadata)

def s3_vector_to_dict(s3_vector:Dict[str, Any]) -> Dict[str, Any]:

    result = {
        'id': s3_vector['key'],
        'embedding': s3_vector.get('data', {}).get(VECTOR_DATA_TYPE, [])
    }

    s3_vector_metadata = s3_vector.get('metadata', {})

    result['value'] = s3_vector_metadata.get('value', '')

    node_metadata = json.loads(s3_vector_metadata.get('metadata', '{}'))
    if 'source' not in node_metadata:
        node_metadata['source'] = {}

    source = node_metadata['source']
    if 'metadata' not in source:
        source['metadata'] = {}
    if 'versioning' not in source:
        source['versioning'] = {}


    source_metadata = source['metadata']
    versioning = source['versioning']

    versioning['valid_from'] = s3_vector_metadata.get(f'{SOURCE_VERSIONING_PREFIX}valid_from', TIMESTAMP_LOWER_BOUND)
    versioning['valid_to'] = s3_vector_metadata.get(f'{SOURCE_VERSIONING_PREFIX}valid_to', TIMESTAMP_UPPER_BOUND)

    for k, v in s3_vector_metadata.items():
        if k.startswith(SOURCE_METADATA_PREFIX):
            source_metadata[k[len(SOURCE_METADATA_PREFIX):]] = v

    for k, v in node_metadata.items():
        result[k] = v

    return result

def validate_metadata_limits(metadata: Dict[str, Any], max_tags: int, vector_id: str) -> None:
    
    if not isinstance(metadata, dict):
        raise TypeError(f"Vector '{vector_id}' metadata must be a dictionary, got {type(metadata)}")
    
    actual_count = len(metadata)
    if actual_count + len(NON_FILTERABLE_FIELDS) > max_tags:
        raise ValueError(f"Vector '{vector_id}' has {actual_count} user-supplied metadata fields, plus {len(NON_FILTERABLE_FIELDS)} reserved fields, but maximum allowed is {max_tags}")
    
    for k, _ in metadata.items():
        if len(k) + len(SOURCE_METADATA_PREFIX) > MAX_KEY_LENGTH:
            raise ValueError(f"Key must not exceed {MAX_KEY_LENGTH - len(SOURCE_METADATA_PREFIX)} characters: {k}")
        

def create_vector_bucket(s3_vectors_client, bucket_name:str) -> str:

    try:
        # Check if vector bucket already exists using S3 Vectors API
        s3_vectors_client.get_vector_bucket(vectorBucketName=bucket_name)
        logger.debug(f'Using existing vector bucket: {bucket_name}')
        return bucket_name
    except ClientError as e:
        if e.response['Error']['Code'] != 'NotFoundException':
            logger.error(f"Error while creating vector bucket: {str(e)}")
            raise
    
    # Create vector bucket using S3 Vectors API
    try:
        s3_vectors_client.create_vector_bucket(
            vectorBucketName=bucket_name
        )
        logger.debug(f'Created new bucket: {bucket_name}')
    except ClientError as create_error:
        if create_error.response['Error']['Code'] == 'ConflictException':
            logger.debug(f'Using existing bucket: {bucket_name}')
        else:
            raise
    
    return bucket_name


def create_vector_index(s3_vectors_client, bucket_name:str, index_name:str, dimension:int, distance_metric:str=None) -> str:
       
    if distance_metric is None:
        distance_metric = DISTANCE_METRIC
    
    try:
        s3_vectors_client.create_index(
            vectorBucketName=bucket_name,
            indexName=index_name,
            dimension=dimension,
            distanceMetric=distance_metric.lower(),
            dataType=VECTOR_DATA_TYPE,
            metadataConfiguration={
                'nonFilterableMetadataKeys': NON_FILTERABLE_FIELDS
            }
        )
        logger.debug(f'Created index: {index_name}')
    except ClientError as index_error:
        if index_error.response['Error']['Code'] == 'ConflictException':
            logger.debug(f'Using existing index: {index_name}')
        else:
            raise
    return index_name


def ingest_vectors(s3_vectors_client, bucket_name:str, index_name:str, vector_data:List[Dict[str, Any]], batch_size:int=None) -> Dict[str, Any]:
     
    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE
    
    for vector_item in vector_data:
        vector_id = vector_item['key']
        metadata = vector_item.get('metadata', {})
        validate_metadata_limits(metadata, MAX_METADATA_TAGS, vector_id)
    
    successful_ingestions = 0
    failed_ingestions = 0
    ingestion_errors = []
    ingested_vector_ids = []
    
    # Process in batches
    for i in tqdm(range(0, len(vector_data), batch_size), desc='Ingesting vectors'):
        batch = vector_data[i:i + batch_size]
        
        # S3 Vectors API call
        try:
            response = s3_vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=batch
            )
            
            # All vectors in batch are successful if no exception
            successful_ingestions += len(batch)
            ingested_vector_ids.extend([v['key'] for v in vector_data[i:i + batch_size]])
            
        except Exception as e:
            print(f'Ingestion failure: {e}')
            failed_ingestions += len(batch)
            error_msg = f'Batch {i//batch_size + 1} failed: {str(e)}'
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


def search_vectors(s3_vectors_client, bucket_name:str, index_name:str, query_vector:List[float], metadata_filters:Dict[str, Any]=None, max_results:int=10) -> List[Dict[str, Any]]:

    if max_results > MAX_RESULTS:
        raise IndexError(f'max_results too large ({max_results}) - S3 vectors supports up to {MAX_RESULTS} Top-K results per QueryVectors request')
    
    query_params = {
        'vectorBucketName': bucket_name,
        'indexName': index_name,
        'queryVector': {VECTOR_DATA_TYPE: query_vector}, 
        'topK': max_results,
        'returnDistance': True,
        'returnMetadata': True
    }
    
    if metadata_filters:
        query_params['filter'] = metadata_filters
    
    response = s3_vectors_client.query_vectors(**query_params)
    
    results = []
    if 'vectors' in response:
        for vector_result in response['vectors']:
            result = s3_vector_to_dict(vector_result)
            result['score'] = 1.0 - vector_result.get('distance', 0.0)
            results.append(result)
    
    return results

def get_vectors(s3_vectors_client, bucket_name:str, index_name:str, ids:List[str]=[], batch_size:int=None) -> List[Dict[str, Any]]:

    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE

    results = []

    for i in tqdm(range(0, len(ids), batch_size), desc='Getting vectors'):
        batch = ids[i:i + batch_size]

        response = s3_vectors_client.get_vectors(
            vectorBucketName=bucket_name,
            indexName=index_name,
            keys=batch,
            returnData=True,
            returnMetadata=True
        )

        
        if 'vectors' in response:
            for vector_result in response['vectors']:
                result = s3_vector_to_dict(vector_result)
                results.append(result)
    
    return results


def delete_vectors(s3_vectors_client, bucket_name:str, index_name:str, vector_ids:List[str], batch_size:int=None) -> Dict[str, Any]:

    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE

    successful_deletions = 0
    failed_deletions = 0
    deletion_errors = []
    failed_vector_ids = []

    for i in tqdm(range(0, len(vector_ids), batch_size), desc='Deleting vectors'):
        batch = vector_ids[i:i + batch_size]
    
        try:
            response = s3_vectors_client.delete_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=batch
            )
            
            successful_deletions += len(vector_ids)
            
            
        except Exception as e:
            failed_deletions += len(vector_ids)
            deletion_errors.extend([f'Delete operation failed: {str(e)}'])
            failed_vector_ids.extend(batch)
    
    total_requested = len(vector_ids)
    success_rate = (successful_deletions / total_requested) * 100 if total_requested > 0 else 0
    
    return {
        'total_requested': total_requested,
        'successful_deletions': successful_deletions,
        'failed_deletions': failed_deletions,
        'success_rate': success_rate,
        'errors': deletion_errors,
        'failed_vector_ids': failed_vector_ids
    }

class S3VectorIndex(VectorIndex):
    @staticmethod
    def for_index(index_name, bucket_name, prefix=None, embed_model=None, dimensions=None, **kwargs):

        embed_model = embed_model or GraphRAGConfig.embed_model
        dimensions = dimensions or GraphRAGConfig.embed_dimensions

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
            node_to_s3_vector(node, id_to_embed_map[node.node_id])
            for node in nodes
        ]

        results = ingest_vectors(
            self.client, 
            self.bucket_name, 
            self.underlying_index_name(), 
            vector_data
        )

        if results['failed_ingestions'] > 0:
            logger.error(f'Errors while adding embeddings: {results}')
            
        return nodes
    
    def _to_top_k_result(self, r):
    
        result = {
            'score': r['score']
        }

        if INDEX_KEY in r:
            index_name = r[INDEX_KEY]['index']
            result[index_name] = r[index_name]
            if 'source' in r:
                result['source'] = r['source']
        else:
            for k,v in r.items():
                result[k] = v
            
        return result
    
    def _to_embeddings_result(self, r):

        r.pop(INDEX_KEY, None)
            
        return r
    

    def top_k(self, query_bundle:QueryBundle, top_k:int=5, filter_config:Optional[FilterConfig]=None) -> Sequence[Dict[str, Any]]:
        
        query_bundle = to_embedded_query(query_bundle, self.embed_model)

        search_results = search_vectors(
            self.client, 
            self.bucket_name, 
            self.underlying_index_name(), 
            query_vector=query_bundle.embedding, 
            metadata_filters=filter_config_to_s3_filters(filter_config),
            max_results=top_k
        )

        top_k_results = [
            self._to_top_k_result(r)
            for r in search_results
        ]

        return top_k_results

    def get_embeddings(self, ids:List[str]=[]) -> Sequence[Dict[str, Any]]:
        
        vectors = get_vectors(
            self.client, 
            self.bucket_name, 
            self.underlying_index_name(), 
            ids
        )

        get_embeddings_results = [
            self._to_embeddings_result(r)
            for r in vectors
        ]
        
        return get_embeddings_results
    
    def update_versioning(self, versioning_timestamp:int, ids:List[str]=[]) -> List[str]:
        
        vectors = get_vectors(
            self.client, 
            self.bucket_name, 
            self.underlying_index_name(), 
            ids
        )

        for vector in vectors:
            vector['source']['versioning']['valid_to'] = versioning_timestamp

        s3_vectors = []

        for vector in vectors:
            id = vector.pop('id')
            value = vector.pop('value')
            embedding = vector.pop('embedding')

            s3_vectors.append(_node_to_s3_vector(id, value, embedding, vector))

        ingest_vectors(
            self.client,
            self.bucket_name,
            self.underlying_index_name(),
            s3_vectors
        )

    def enable_for_versioning(self, ids:List[str]=[]) -> List[str]:
        
        vectors = get_vectors(
            self.client, 
            self.bucket_name, 
            self.underlying_index_name(), 
            ids
        )

        for vector in vectors:
            if 'versioning' not in vector['source']:
                vector['source']['versioning'] = {}
            
            vector['source']['versioning']['valid_from'] = TIMESTAMP_LOWER_BOUND
            vector['source']['versioning']['valid_to'] = TIMESTAMP_UPPER_BOUND

        s3_vectors = []

        for vector in vectors:
            id = vector.pop('id')
            value = vector.pop('value')
            embedding = vector.pop('embedding')

            s3_vectors.append(_node_to_s3_vector(id, value, embedding, vector))

        ingest_vectors(
            self.client,
            self.bucket_name,
            self.underlying_index_name(),
            s3_vectors
        )
    
    def delete_embeddings(self, ids:List[str]=[]):

        delete_vectors_results = delete_vectors(
            self.client, 
            self.bucket_name, 
            self.underlying_index_name(), 
            ids
        )

        if delete_vectors_results['failed_deletions'] > 0:
            logger.error(f'Errors while deleting embeddings: {delete_vectors_results}')
            return delete_vectors_results['failed_vector_ids']
        else:
            return []
