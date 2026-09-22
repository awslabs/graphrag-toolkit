# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Red-state tests for path traversal in every id-to-path sink.

Each sink joins an id onto a prepared directory, and IdRewriter returns any id
already starting ``aws:`` as given, so ``aws:../../etc/x`` reaches a sink
untouched. Before `validate_id` was shared, only FileSystemTap validated, and
`FileBasedDocs.accept`, `CheckpointWriter.accept` and
`BatchExtractorBase._save_node_in_temp_dir` created directories and wrote files
outside their output tree; `CheckpointFilter.checkpoint_does_not_exist` probed a
path outside it. FileBasedDocs must not lean on `filename_sanitizer` for this:
it defaults to a no-op, `windows_safe_filename` rewrites colons rather than
separators, and a custom sanitizer can introduce a separator of its own.
"""

import os
from pathlib import Path
from typing import Any, List

import pytest
from llama_index.core.llms import MockLLM
from llama_index.core.schema import (
    BaseNode, Document, NodeRelationship, RelatedNodeInfo, TextNode,
)

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import CheckpointFilter, CheckpointWriter
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
from graphrag_toolkit.lexical_graph.indexing.extract.file_system_tap import FileSystemTap
from graphrag_toolkit.lexical_graph.indexing.load.file_based_docs import (
    FileBasedDocs, windows_safe_filename,
)
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.utils import LLMCache

# The id from the report: IdRewriter passes it through because of the 'aws:' prefix.
REPORTED_ID = 'aws:../../etc/x'

# Climbs far enough to leave the output tree the fixtures nest, and every leading
# segment resolves, so pre-fix this wrote a file rather than raising ENOENT.
ESCAPING_ID = '../../../../pwned'

BENIGN_SOURCE_ID = 'aws::1a2b3c4d:5e6f'
BENIGN_NODE_ID = 'aws::1a2b3c4d:5e6f:0a1b2c3d'


class PassThrough(NodeHandler):
    """Minimal inner handler, so CheckpointWriter sees the node ids as given."""

    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        yield from nodes


class _BatchExtractor(BatchExtractorBase):
    """Concrete subclass, so _save_node_in_temp_dir can be exercised directly."""

    @classmethod
    def class_name(cls) -> str:
        return '_BatchExtractor'

    def _get_json(self, node, llm, inference_parameters):
        return {}

    def _run_non_batch_extractor(self, nodes):
        return []

    def _update_node(self, node: TextNode, node_metadata_map):
        return node


def _node(node_id, source_id):
    node = TextNode(text='chunk', id_=node_id)
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
    return node


def _names_under(root):
    """Every path below `root`, so a test can assert nothing escaped."""
    return {p for p in Path(root).rglob('*')}


def _nested_dir(root, leaf):
    """A sink directory deep enough that a climbing id leaves it but stays in `root`."""
    path = os.path.join(root, 'a', 'b', 'c', 'd', leaf)
    os.makedirs(path)
    return path


class TestFileSystemTap:
    """The sink that already validated — pinned so the shared move is a no-op here."""

    @pytest.mark.parametrize('doc_id', [REPORTED_ID, ESCAPING_ID])
    def test_doc_id_is_rejected(self, tmp_path, doc_id):
        tap = FileSystemTap(subdirectory_name='run', clean=True, output_dir=str(tmp_path))

        with pytest.raises(ValueError, match='separator'):
            tap.handle_input_docs([SourceDocument(refNode=Document(text='t', doc_id=doc_id))])

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        tap = FileSystemTap(subdirectory_name='run', clean=True, output_dir=str(tmp_path))
        doc = SourceDocument(refNode=Document(text='t', doc_id=BENIGN_SOURCE_ID))
        doc.nodes = [TextNode(text='chunk', id_=node_id)]

        with pytest.raises(ValueError, match='separator'):
            tap.handle_output_doc(doc)


class TestFileBasedDocs:
    """`accept` makedirs on the source id and opens a file under the node id."""

    @pytest.mark.parametrize('source_id', [REPORTED_ID, ESCAPING_ID])
    def test_source_id_is_rejected(self, tmp_path, source_id):
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(docs_directory=docs_dir, collection_id='coll')
        doc = SourceDocument(nodes=[_node(BENIGN_NODE_ID, source_id)])

        with pytest.raises(ValueError, match='separator'):
            list(handler.accept([doc]))

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(docs_directory=docs_dir, collection_id='coll')
        doc = SourceDocument(nodes=[_node(node_id, BENIGN_SOURCE_ID)])

        with pytest.raises(ValueError, match='separator'):
            list(handler.accept([doc]))

    def test_nothing_is_created_outside_the_collection_directory(self, tmp_path):
        """Pre-fix this made a directory and wrote a JSON file above `docs_directory`."""
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(docs_directory=docs_dir, collection_id='coll')
        doc = SourceDocument(nodes=[_node(ESCAPING_ID, f'aws:{ESCAPING_ID}_dir')])

        before = _names_under(tmp_path)

        with pytest.raises(ValueError):
            list(handler.accept([doc]))

        assert _names_under(tmp_path) == before

    def test_a_sanitizer_does_not_excuse_validation(self, tmp_path):
        """windows_safe_filename rewrites colons, so the separators survive it."""
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(
            docs_directory=docs_dir,
            collection_id='coll',
            filename_sanitizer=windows_safe_filename,
        )
        doc = SourceDocument(nodes=[_node(BENIGN_NODE_ID, REPORTED_ID)])

        assert '/' in windows_safe_filename(REPORTED_ID)
        with pytest.raises(ValueError, match='separator'):
            list(handler.accept([doc]))

    def test_a_sanitizer_that_introduces_a_separator_is_rejected(self, tmp_path):
        """The sanitizer's output is what gets joined, so validating only the id
        would let a colon-to-slash sanitizer climb out on a benign id."""
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(
            docs_directory=docs_dir,
            collection_id='coll',
            filename_sanitizer=lambda name: name.replace(':', '/'),
        )
        doc = SourceDocument(nodes=[_node(BENIGN_NODE_ID, 'aws:..:..:..:..:pwned')])

        before = _names_under(tmp_path)

        with pytest.raises(ValueError, match='sanitized source_id'):
            list(handler.accept([doc]))

        assert _names_under(tmp_path) == before


class TestCheckpointWriter:
    """`accept` touches a file named for the node id."""

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        checkpoint_dir = _nested_dir(str(tmp_path), 'cp')
        writer = CheckpointWriter(
            inner=PassThrough(), checkpoint_dir=checkpoint_dir, checkpoint_name='cp',
        )

        with pytest.raises(ValueError, match='separator'):
            list(writer.accept([TextNode(text='chunk', id_=node_id)]))

    def test_nothing_is_created_outside_the_checkpoint_directory(self, tmp_path):
        """Pre-fix this touched a checkpoint file above `checkpoint_dir`."""
        checkpoint_dir = _nested_dir(str(tmp_path), 'cp')
        writer = CheckpointWriter(
            inner=PassThrough(), checkpoint_dir=checkpoint_dir, checkpoint_name='cp',
        )

        before = _names_under(tmp_path)

        with pytest.raises(ValueError):
            list(writer.accept([TextNode(text='chunk', id_=ESCAPING_ID)]))

        assert _names_under(tmp_path) == before


class TestBatchExtractorTempDir:
    """`_save_node_in_temp_dir` names a temp file for the node id."""

    def _extractor(self, temp_dir):
        config = BatchConfig(
            role_arn='arn:aws:iam::123456789012:role/test', region='us-east-1', bucket_name='test',
        )
        return _BatchExtractor(
            batch_config=config,
            llm=LLMCache(llm=MockLLM()),
            prompt_template='t',
            batch_inference_dir=temp_dir,
            description='test',
        )

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        temp_dir = _nested_dir(str(tmp_path), 'batch')
        extractor = self._extractor(temp_dir)

        with pytest.raises(ValueError, match='separator'):
            extractor._save_node_in_temp_dir(TextNode(text='chunk', id_=node_id), temp_dir)

    def test_an_absolute_node_id_is_rejected(self, tmp_path):
        """join() discards the temp dir for an absolute id, so pre-fix this wrote
        wherever the id pointed."""
        temp_dir = _nested_dir(str(tmp_path), 'batch')
        extractor = self._extractor(temp_dir)

        with pytest.raises(ValueError, match='separator'):
            extractor._save_node_in_temp_dir(TextNode(text='chunk', id_='/tmp/pwned'), temp_dir)


class TestCheckpointFilter:
    """The filter only probes with os.path.exists, but it probes the joined path,
    so an unvalidated id decides a node's fate from somewhere else on disk."""

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        checkpoint_filter = CheckpointFilter(
            checkpoint_name='cp',
            checkpoint_dir=str(tmp_path),
            inner=PassThrough(),
            tenant_id=TenantId(),
        )

        with pytest.raises(ValueError, match='separator'):
            checkpoint_filter.checkpoint_does_not_exist(node_id)

    def test_a_rewritten_id_still_passes(self, tmp_path):
        checkpoint_filter = CheckpointFilter(
            checkpoint_name='cp',
            checkpoint_dir=str(tmp_path),
            inner=PassThrough(),
            tenant_id=TenantId(),
        )

        assert checkpoint_filter.checkpoint_does_not_exist(BENIGN_NODE_ID) is True
        (tmp_path / BENIGN_NODE_ID).touch()
        assert checkpoint_filter.checkpoint_does_not_exist(BENIGN_NODE_ID) is False


class TestRewrittenIdsStillWrite:
    """The ids the pipeline actually produces carry no separator."""

    def test_file_system_tap_writes_source_and_chunk(self, tmp_path):
        tap = FileSystemTap(subdirectory_name='run', clean=True, output_dir=str(tmp_path))
        doc = SourceDocument(refNode=Document(text='t', doc_id=BENIGN_SOURCE_ID))
        doc.nodes = [TextNode(text='chunk', id_=BENIGN_NODE_ID)]

        tap.handle_input_docs([doc])
        tap.handle_output_doc(doc)

        assert os.path.exists(os.path.join(tap.raw_sources_dir, BENIGN_SOURCE_ID))
        assert os.path.exists(os.path.join(tap.chunks_dir, f'{BENIGN_NODE_ID}.json'))

    def test_file_based_docs_writes_the_chunk(self, tmp_path):
        handler = FileBasedDocs(docs_directory=str(tmp_path), collection_id='coll')
        doc = SourceDocument(nodes=[_node(BENIGN_NODE_ID, BENIGN_SOURCE_ID)])

        list(handler.accept([doc]))

        assert (tmp_path / 'coll' / BENIGN_SOURCE_ID / f'{BENIGN_NODE_ID}.json').exists()

    def test_checkpoint_writer_touches_the_checkpoint(self, tmp_path):
        writer = CheckpointWriter(
            inner=PassThrough(), checkpoint_dir=str(tmp_path), checkpoint_name='cp',
        )

        list(writer.accept([TextNode(text='chunk', id_=BENIGN_NODE_ID)]))

        assert (tmp_path / BENIGN_NODE_ID).exists()
