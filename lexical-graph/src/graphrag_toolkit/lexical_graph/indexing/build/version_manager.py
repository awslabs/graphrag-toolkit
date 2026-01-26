# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from tqdm import tqdm
from typing import Any, List, Union, Dict, Tuple

from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO, VERSION_INDEPENDENT_ID_FIELDS, TIMESTAMP_LOWER_BOUND, TIMESTAMP_UPPER_BOUND
from graphrag_toolkit.lexical_graph.metadata import format_metadata_list, to_metadata_filter
from graphrag_toolkit.lexical_graph.errors import IndexError
from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import filter_config_to_opencypher_filters
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore, DummyVectorIndex, VectorIndex
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.constants import DEFAULT_EMBEDDING_INDEXES, INDEX_KEY

from llama_index.core.schema import BaseNode
from llama_index.core.utils import iter_batch
from llama_index.core.schema import NodeRelationship

logger = logging.getLogger(__name__)

GraphInfoType = Union[str, GraphStore]
VectorStoreInfoType = Union[str, VectorStore]

class VersionManager(NodeHandler):

    @staticmethod
    def for_graph_and_vector_store(graph_info:GraphInfoType=None, vector_store_info:VectorStoreInfoType=None, index_names=DEFAULT_EMBEDDING_INDEXES, **kwargs):

        graph_store = None
        vector_store = None
        
        if isinstance(graph_info, GraphStore):
            graph_store=graph_info
        else:
            graph_store=GraphStoreFactory.for_graph_store(graph_info, **kwargs)

        if isinstance(vector_store_info, VectorStore):
            vector_store=vector_store_info
        else:
            vector_store=VectorStoreFactory.for_vector_store(vector_store_info, index_names, **kwargs)

        return VersionManager(graph_store=graph_store, vector_store=vector_store)
    
    graph_store: GraphStore 
    vector_store: VectorStore


    def _get_existing_source_nodes(self, version_independent_id_fields:List[str], node:BaseNode) -> List[Dict[str, Any]]:
        
        if not version_independent_id_fields:
            return []
        
        formatted_version_independent_id_fields = format_metadata_list(version_independent_id_fields)
        version_independent_id_fields_filter = f"coalesce(source.{VERSION_INDEPENDENT_ID_FIELDS}, '{formatted_version_independent_id_fields}') = $versionIndependentIdFields"
        
        other_filter_criteria = {
            version_independent_id_field:node.metadata['source']['metadata'][version_independent_id_field]
            for version_independent_id_field in version_independent_id_fields
            if node.metadata.get('source', {}).get('metadata', {}).get(version_independent_id_field, None)
        }

        filter_config = to_metadata_filter(other_filter_criteria)
        where_clauses =  filter_config_to_opencypher_filters(filter_config)

        if not where_clauses:
            return []

        cypher = f'''// find source nodes to be versioned
        MATCH (source:`__Source__`)
        WHERE {where_clauses} AND {version_independent_id_fields_filter}
        RETURN {{
            source_id: {self.graph_store.node_id("source.sourceId")},
            valid_from: coalesce(source.{VALID_FROM}, {TIMESTAMP_LOWER_BOUND}),
            valid_to: coalesce(source.{VALID_TO}, {TIMESTAMP_UPPER_BOUND})
        }} AS result ORDER BY result.valid_from DESC
        '''

        parameters = {
            'versionIndependentIdFields': formatted_version_independent_id_fields
        }

        results = self.graph_store.execute_query(cypher, parameters)

        return [result['result'] for result in results]
    
    def _get_node_ids(self, index:VectorIndex, source_ids:List[str]) -> Dict[str, List[str]]:
        
        if isinstance(index, DummyVectorIndex):
            return []
        
        if index.index_name == 'chunk':
            cypher = f'''// get chunk ids to be versioned
            MATCH (s)<-[:`__EXTRACTED_FROM__`]-(c)
            WHERE {self.graph_store.node_id("s.sourceId")} IN $sourceIds
            WITH {self.graph_store.node_id("s.sourceId")} as source_id, collect({self.graph_store.node_id("c.chunkId")}) as node_ids
            RETURN {{
                sourceId: source_id,
                nodeIds: node_ids
            }} AS result
            '''
        elif index.index_name == 'topic':
            cypher = f'''// get topic ids to be versioned
            MATCH (s)<-[:`__EXTRACTED_FROM__`]-()<-[:`__MENTIONED_IN__`]-(t)
            WHERE {self.graph_store.node_id("s.sourceId")} IN $sourceIds
            WITH {self.graph_store.node_id("s.sourceId")} as source_id, collect({self.graph_store.node_id("t.topicId")}) as node_ids
            RETURN {{
                sourceId: source_id,
                nodeIds: node_ids
            }} AS result
            '''
        elif index.index_name == 'statement':
            cypher = f'''// get topic ids to be versioned
            MATCH (s)<-[:`__EXTRACTED_FROM__`]-()<-[:`__MENTIONED_IN__`]-()<-[:`__BELONGS_TO__`]-(l)
            WHERE {self.graph_store.node_id("s.sourceId")} IN $sourceIds
            WITH {self.graph_store.node_id("s.sourceId")} as source_id, collect({self.graph_store.node_id("l.statementId")}) as node_ids
            RETURN {{
                sourceId: source_id,
                nodeIds: node_ids
            }} AS result
            '''
        else:
            raise ValueError(f'Invalid index name: {index.index_name}')
        
        parameters = {
            'sourceIds': source_ids
        }

        results = self.graph_store.execute_query(cypher, parameters)

        return {result['result']['sourceId']:result['result']['nodeIds'] for result in results}
    
    def _get_updates(self, new_source_node:Dict[str, Any], existing_source_nodes:List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:

        # order existing source nodes, most recent first
        sorted_existing_source_nodes = sorted(existing_source_nodes, key=lambda item:item['valid_from'], reverse=True)

        prev_valid_from = None
        adjustments = []

        for n in sorted_existing_source_nodes:
            if new_source_node['valid_from'] == n['valid_from']:
                # same valid_from as existing node, so ensure same valid_to as existing node
                new_source_node['valid_to'] = n['valid_to']
            elif new_source_node['valid_from'] > n['valid_from']:
                # newer than existing node, so update valid_to
                new_source_node['valid_to'] = prev_valid_from
            else:
                prev_valid_from = n['valid_from']

        if not new_source_node['valid_to']:   
            if prev_valid_from:
                # new node is the earliest, so set valid_to based to prev earliest 
                new_source_node['valid_to'] = prev_valid_from
            else:
                # new node is the latest
                new_source_node['valid_to'] = TIMESTAMP_UPPER_BOUND
            
                
        if new_source_node['valid_to'] == TIMESTAMP_UPPER_BOUND:
            # latest source node, so archive previous latest source nodes
            for n in sorted_existing_source_nodes:
                if new_source_node['valid_from'] > n['valid_from'] and n['valid_to'] == TIMESTAMP_UPPER_BOUND:
                    adjustments.append({'source_id':n['source_id'], 'valid_from':n['valid_from'], 'valid_to':new_source_node['valid_from']})
        else:
            # is historical source node, so insert into timeline, adjusting other historical nodes as necessary
            for n in sorted_existing_source_nodes:
                if new_source_node['valid_from'] > n['valid_from'] and new_source_node['valid_from'] < n['valid_to'] and new_source_node['valid_to'] >= n['valid_to']:
                    adjustments.append({'source_id':n['source_id'], 'valid_from':n['valid_from'], 'valid_to':new_source_node['valid_from']})

        return (new_source_node, adjustments)

    
    def _set_source_node_version_info(self, source_id:str, versioning_timestamp:int, version_independent_id_fields:List[str]):

        cypher = f'''// set version info for old source node
        MATCH (s:`__Source__`)
        WHERE {self.graph_store.node_id("s.sourceId")} = $sourceId
        SET s.{VALID_TO} = $versioningTimestamp, s.{VERSION_INDEPENDENT_ID_FIELDS} = $versionIndependentIdFields
        '''

        properties = {
            'sourceId': source_id,
            'versioningTimestamp': versioning_timestamp,
            'versionIndependentIdFields': format_metadata_list(version_independent_id_fields)
        }

        self.graph_store.execute_query_with_retry(cypher, properties)

    def _update_vector_store_versions(self, source_id:str, node_ids:List[str], versioning_timestamp:int, index:VectorIndex):

        node_id_batches = [
            node_ids[x:x+100] 
            for x in range(0, len(node_ids), 100)
        ]

        logger.debug(f'Updating valid_to version info for {source_id} in vector index [index: {index.underlying_index_name()}, batch_sizes: {[len(b) for b in node_id_batches]}, valid_to: {versioning_timestamp}]')

        for node_id_batch in node_id_batches:
            for num_attempts in range(1, 6):
                failed_ids = index.update_versioning(versioning_timestamp, node_id_batch)
                if failed_ids:
                    logger.warning(f'Transient error while updating vector index, retrying in {num_attempts} seconds')
                    time.sleep(num_attempts)
                else:
                    break
            if failed_ids:
                raise IndexError(f'Failed to update valid_to version info for {source_id} in vector index [index: {index.underlying_index_name()}, source_id: {source_id}, node_ids: {node_id_batch}]')


    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        
        node_iterable = nodes if not self.show_progress else tqdm(nodes, desc='Applying version updates')
        
        source_version_info = {}

        for node in node_iterable:

            if [key for key in [INDEX_KEY] if key in node.metadata]:

                index_name = node.metadata[INDEX_KEY]['index']

                if index_name == 'source':

                    source_id = node.metadata['source']['sourceId']
                    versioning_timestamp = node.metadata['source']['versioning'].get('valid_from', 'extract_timestamp')
                    version_independent_id_fields = node.metadata.get('source', {}).get('versioning', {}).get('id_fields', None)

                    new_source_node = {'source_id': source_id, 'valid_from': versioning_timestamp, 'valid_to': None}
                    existing_source_nodes = self._get_existing_source_nodes(version_independent_id_fields, node)

                    logger.debug(f'Begin versioning source node [new_source_node: {new_source_node}, existing_source_nodes: {existing_source_nodes}]')

                    (source_node, other_source_nodes) = self._get_updates(new_source_node, existing_source_nodes)

                    logger.debug(f'Proposed versioning changes [source_node: {source_node}, other_source_nodes: {other_source_nodes}]')

                    node.metadata['source']['versioning']['valid_from'] = source_node['valid_from']
                    node.metadata['source']['versioning']['valid_to'] = source_node['valid_to']
                    node.metadata['source']['versioning']['prev_versions'] = [o['source_id'] for o in other_source_nodes]

                    source_version_info[source_id] = source_node

                    for other_source_node in other_source_nodes:
                        valid_to = other_source_node['valid_to']
                        for index in self.vector_store.all_indexes():
                            node_id_map = self._get_node_ids(index, [other_source_node['source_id']])
                            for other_source_id, node_ids in node_id_map.items():
                                self._update_vector_store_versions(other_source_id, node_ids, valid_to, index)
                                self._set_source_node_version_info(other_source_id, valid_to, version_independent_id_fields)

                else:

                    source_id = node.metadata.get('source', {}).get('sourceId', None)

                    if source_id:
                        version_info = source_version_info.get(source_id, None)

                        if version_info:
                            node.metadata['source']['versioning']['valid_from'] = version_info['valid_from']
                            node.metadata['source']['versioning']['valid_to'] = version_info['valid_to']
                        else:
                            logger.debug(f'No version info found for source {source_id}')
                        
            yield node

        

        