# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import glob
import os
import unittest
import uuid
from typing import Any, Dict

from graphrag_toolkit.lexical_graph import (
    ExtractionConfig,
    GraphRAGConfig,
    IndexingConfig,
    LexicalGraphIndex,
    TenantId,
)
from graphrag_toolkit.lexical_graph.config import DEFAULT_EXTRACTION_MODEL
from graphrag_toolkit.lexical_graph.indexing.extract.ontology import (
    OntologyConfig,
    authored_name,
)
from graphrag_toolkit.lexical_graph.indexing.extract.ontology.ontology import Ontology
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import (
    MultiTenantGraphStore,
    NonRedactedGraphQueryLogFormatting,
)
from llama_index.core.schema import Document, TextNode

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

ONTOLOGY_PATH = './source-data/ontologies/neptune.ttl'


def _text_and_url_only(collection_id:str):
    """The collection's text and url, without the extraction results its files already carry."""
    documents = []
    for path in sorted(glob.glob(f'source-data/{collection_id}/**/*.json', recursive=True)):
        with open(path) as f:
            node = TextNode.from_json(f.read())
        documents.append(Document(text=node.text, metadata={'url': node.metadata['url']}))
    return documents


class OntologyGuidedExtraction(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract and build Neptune documentation with a strict ontology and typed properties'

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', DEFAULT_EXTRACTION_MODEL)

        tenant_id = TenantId(f'onto.{uuid.uuid4().hex[:9]}')
        print(f'TENANT_ID: {tenant_id}')

        index = Ontology.load(ONTOLOGY_PATH).index()
        declared_classes = {authored_name(c) for c in index.classes.values()}
        declared_relations = {authored_name(p) for p in index.object_properties.values()}
        datatype_keys = sorted(p.local_name for p in index.datatype_properties.values())

        docs = _text_and_url_only('collection-1')

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
                tenant_id=tenant_id,
                indexing_config=IndexingConfig(
                    extraction=ExtractionConfig(
                        ontology=OntologyConfig(
                            ONTOLOGY_PATH,
                            ontology_authority='strict',
                            typed_properties='subject'
                        )
                    )
                )
            )

            graph_index.extract_and_build(docs, show_progress=True)

            class OntologyGuidedExtractionAssertions(unittest.TestCase):

                @classmethod
                def setUpClass(cls):
                    cls._graph_store = MultiTenantGraphStore.wrap(graph_store, tenant_id)

                def test_graph_contains_entities(self):
                    """Strict extraction kept at least one entity"""

                    results = self._graph_store.execute_query('MATCH (n:`__Entity__`) RETURN count(n) AS count')

                    self.assertGreater(results[0]['count'], 0)

                def test_every_entity_class_is_declared(self):
                    """Every entity class in the graph is a class the ontology declares"""

                    results = self._graph_store.execute_query('MATCH (n:`__Entity__`) RETURN DISTINCT n.class AS class')
                    classes = {r['class'] for r in results}

                    self.assertEqual(classes - declared_classes, set())

                def test_every_relationship_is_a_declared_object_property(self):
                    """Every entity-to-entity relationship is an object property the ontology declares"""

                    results = self._graph_store.execute_query(
                        'MATCH (:`__Entity__`)-[r:`__RELATION__`]->(:`__Entity__`) RETURN DISTINCT r.value AS value'
                    )
                    relations = {r['value'] for r in results}

                    self.assertEqual(relations - declared_relations, set())

                def test_datatype_property_values_are_stored_as_numbers(self):
                    """Declared integer properties land on entities as numbers, and at least one is present"""

                    values = []
                    for key in datatype_keys:
                        results = self._graph_store.execute_query(
                            f'MATCH (n:`__Entity__`) WHERE n.`{key}` IS NOT NULL RETURN n.`{key}` AS value'
                        )
                        values.extend(r['value'] for r in results)

                    self.assertTrue(values, f'no entity carries any of {datatype_keys}')
                    for value in values:
                        self.assertIsInstance(value, int)
                        self.assertNotIsInstance(value, bool)

            handler.run_assertions(OntologyGuidedExtractionAssertions)
