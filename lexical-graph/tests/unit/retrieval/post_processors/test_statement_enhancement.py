# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import MagicMock, patch

from llama_index.core.schema import NodeWithScore, TextNode

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import S3ChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore
from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.retrieval.post_processors.statement_enhancement import (
    StatementEnhancementPostProcessor,
)

ENHANCED = '<modified_statement>enhanced</modified_statement>'


def _node(statement='a statement', chunk=None):
    metadata = {'source': {'sourceId': 's1'}}
    if statement is not None:
        metadata['statement'] = {'value': statement}
    if chunk is not None:
        metadata['chunk'] = chunk
    return NodeWithScore(node=TextNode(text=statement or '', metadata=metadata), score=1.0)


def _llm(response=ENHANCED):
    # spec=LLMCache so the constructor takes the mock as-is rather than trying
    # to wrap it in a real LLMCache, which validates its llm argument.
    llm = MagicMock(spec=LLMCache)
    llm.predict.return_value = response
    return llm


def _processor(chunk_store=None, response=ENHANCED):
    processor = StatementEnhancementPostProcessor(llm=_llm(response))
    processor.chunk_store = chunk_store
    return processor


def _processor_for(s3_chunk_store, graph_store=None):
    """Build through the real constructor, with S3_CHUNK_STORE set to a value."""
    with patch.object(type(GraphRAGConfig), 's3_chunk_store',
                      property(lambda self: s3_chunk_store)):
        return StatementEnhancementPostProcessor(llm=_llm(), graph_store=graph_store)


class TestChunkTextOnTheNode(unittest.TestCase):

    def test_uses_the_value_carried_on_the_node(self):
        processor = _processor()
        node = _node(chunk={'chunkId': 'c1', 'value': 'in-graph text'})

        result = processor.enhance_statement(node)

        self.assertEqual(result.node.text, 'enhanced')
        self.assertEqual(processor.llm.predict.call_args.kwargs['context'], 'in-graph text')

    def test_does_not_call_the_store_when_the_node_carries_the_text(self):
        store = MagicMock()
        processor = _processor(chunk_store=store)

        processor._postprocess_nodes([_node(chunk={'chunkId': 'c1', 'value': 'in-graph text'})])

        store.get_batch.assert_not_called()


class TestChunkTextFromTheStore(unittest.TestCase):

    def test_resolves_text_the_node_does_not_carry(self):
        store = MagicMock()
        store.get_batch.return_value = {'c1': 'stored text'}
        processor = _processor(chunk_store=store)

        results = processor._postprocess_nodes([_node(chunk={'chunkId': 'c1'})])

        store.get_batch.assert_called_once_with(['c1'])
        self.assertEqual(results[0].node.text, 'enhanced')
        self.assertEqual(processor.llm.predict.call_args.kwargs['context'], 'stored text')

    def test_fetches_every_missing_chunk_in_one_call(self):
        store = MagicMock()
        store.get_batch.return_value = {'c1': 'one', 'c2': 'two'}
        processor = _processor(chunk_store=store)

        processor._postprocess_nodes([_node(chunk={'chunkId': 'c1'}), _node(chunk={'chunkId': 'c2'})])

        store.get_batch.assert_called_once_with(['c1', 'c2'])


class TestNodesThatCannotBeEnhanced(unittest.TestCase):
    """
    Reading chunk text straight off the node raised a KeyError that the broad
    except swallowed, so a node with no chunk text looked enhanced and was not.
    These assert the node comes back untouched and the model is never called.
    """

    def test_no_chunk_text_and_no_store_leaves_the_node_alone(self):
        processor = _processor()
        node = _node(chunk={'chunkId': 'c1'})

        self.assertIs(processor.enhance_statement(node), node)
        processor.llm.predict.assert_not_called()

    def test_a_store_miss_leaves_the_node_alone(self):
        store = MagicMock()
        store.get_batch.return_value = {}
        processor = _processor(chunk_store=store)
        node = _node(chunk={'chunkId': 'c1'})

        self.assertIs(processor._postprocess_nodes([node])[0], node)
        processor.llm.predict.assert_not_called()

    def test_no_chunk_metadata_at_all_leaves_the_node_alone(self):
        processor = _processor()
        node = _node()

        self.assertIs(processor.enhance_statement(node), node)
        processor.llm.predict.assert_not_called()

    def test_no_statement_leaves_the_node_alone(self):
        processor = _processor()
        node = _node(statement=None, chunk={'chunkId': 'c1', 'value': 'text'})

        self.assertIs(processor.enhance_statement(node), node)
        processor.llm.predict.assert_not_called()


class TestUnmatchedResponse(unittest.TestCase):

    def test_a_response_without_the_tag_returns_the_original_node(self):
        processor = _processor(response='no tag here')
        node = _node(chunk={'chunkId': 'c1', 'value': 'text'})

        self.assertIs(processor.enhance_statement(node), node)


class TestChunkStoreResolution(unittest.TestCase):
    """The constructor decides which store answers a node that carries no chunk text.

    An external store stands on its own; the graph store only adds the in-graph
    fallback. Gating the external store on a graph store was what left the
    post-processor unenhanced for every caller that passes none.
    """

    def test_external_store_is_opened_without_a_graph_store(self):
        processor = _processor_for('s3://bucket/prefix')

        self.assertIsInstance(processor.chunk_store, S3ChunkStore)
        self.assertIsNone(processor.chunk_store.fallback)

    def test_external_store_takes_the_graph_store_as_its_fallback(self):
        processor = _processor_for('s3://bucket/prefix', graph_store=MagicMock())

        self.assertIsInstance(processor.chunk_store, S3ChunkStore)
        self.assertIsInstance(processor.chunk_store.fallback, InGraphChunkStore)

    def test_graph_store_alone_reads_chunks_from_the_graph(self):
        processor = _processor_for(None, graph_store=MagicMock())

        self.assertIsInstance(processor.chunk_store, InGraphChunkStore)

    def test_no_store_configured_leaves_the_processor_without_one(self):
        processor = _processor_for(None)

        self.assertIsNone(processor.chunk_store)

    def test_an_unrecognised_uri_fails_at_construction(self):
        with self.assertRaises(ValueError):
            _processor_for('redis://cache/chunks')


if __name__ == '__main__':
    unittest.main()
