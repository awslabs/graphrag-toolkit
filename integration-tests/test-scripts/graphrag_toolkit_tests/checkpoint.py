# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
import uuid
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs
from graphrag_toolkit.lexical_graph.indexing.build import Checkpoint

from llama_index.core.schema import Document


class ExtractWithCheckpoint(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Extract documents, and checkpoint'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store
            )
            
            docs = [
                Document(id_=f'test-{i}', text=f'Boggles can live for up to {i+10} years')
                for i in range(0, 4)
            ]
            
            checkpoint = Checkpoint(f'checkpoint-{uuid.uuid4().hex}')
            
            extracted_docs_1 = FileBasedDocs(
                docs_directory='extracted'
            )
            
            params['ExtractWithCheckpoint.collection_id'] = extracted_docs_1.collection_id
            
            graph_index.extract(docs, handler=extracted_docs_1, show_progress=True, checkpoint=checkpoint)
            
            extracted_docs_2 = FileBasedDocs(
                docs_directory='extracted'
            )
            
            graph_index.extract(docs, handler=extracted_docs_2, show_progress=True, checkpoint=checkpoint)
            
            
            class CheckpointAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs_1 = len([d for d in extracted_docs_1])
                    cls._num_extracted_docs_2 = len([d for d in extracted_docs_2])
                    cls._expected_num_docs = len(docs)
            
                def test_extracts_docs_on_first_run(self):
                    """Extracted directory contains documents"""
                    
                    self.assertEqual(self._num_extracted_docs_1, self._expected_num_docs)
                    
                def test_ignores_checkpointed_docs_on_second_run(self):
                    """Extracted directory is empty because docs have already been checkpointed"""
                    
                    self.assertEqual(self._num_extracted_docs_2, 0)
                    
            handler.run_assertions(CheckpointAssertions)
            
            
            
from graphrag_toolkit.lexical_graph.indexing import NodeHandler

class BuildCounter(NodeHandler):
    
    count:int=0
 
    def accept(self, nodes, **kwargs):
        for node in nodes:
            self.count += 1
            yield node
            
class BuildWithCheckpoint(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Extract documents, and checkpoint'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                tenant_id='chk'
            )
            
            checkpoint = Checkpoint(f'checkpoint-{uuid.uuid4().hex}')
            
            docs = FileBasedDocs(
                docs_directory='extracted',
                collection_id=params['ExtractWithCheckpoint.collection_id']
            )
            
            counter_1 = BuildCounter()
            counter_2 = BuildCounter()
            
            graph_index.build(docs, checkpoint=checkpoint, handler=counter_1)
            graph_index.build(docs, checkpoint=checkpoint, handler=counter_2)
                       
            
            class BuildWithCheckpoint(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._count_1 = counter_1.count
                    cls._count_2 = counter_2.count
            
                def test_builds_on_first_run(self):
                    """Builds graph on first run"""
                    
                    self.assertGreater(self._count_1, 0)
                    
                def test_ignores_checkpointed_nodes_on_second_run(self):
                    """Does not build graph because nodes have already been checkpointed"""
                    
                    self.assertEqual(self._count_2, 0)
                    
            handler.run_assertions(BuildWithCheckpoint)
    
    