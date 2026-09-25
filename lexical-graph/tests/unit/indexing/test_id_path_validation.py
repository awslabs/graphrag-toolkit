# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Path-traversal tests for the sinks that join an id onto a prepared directory.

IdRewriter returns any id already starting ``aws:`` as given, so ``aws:../../etc/x``
reaches a sink untouched. `FileSystemTap` holds the same guard; its contract is
pinned in `extract/test_file_system_tap.py`.
"""

import os
from pathlib import Path
from typing import Any, List

import pytest
from llama_index.core.llms import MockLLM
from llama_index.core.schema import (
    BaseNode, NodeRelationship, RelatedNodeInfo, TextNode,
)

from graphrag_toolkit.lexical_graph.indexing import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import CheckpointFilter, CheckpointWriter
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
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


class _ForgedIdExtractor(_BatchExtractor):
    """Reports results under an id no node was saved as, the way a tampered batch
    output's recordId would."""

    @classmethod
    def class_name(cls) -> str:
        return '_ForgedIdExtractor'

    def _run_non_batch_extractor(self, nodes):
        return [{ESCAPING_ID: 'extracted'}]


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


class TestFileBasedDocs:
    """`accept` makedirs on the source id and opens a file under the node id. The
    sanitizer's output is what gets joined, so that is what has to be validated:
    it defaults to a no-op, `windows_safe_filename` rewrites colons rather than
    separators, and a custom sanitizer can introduce a separator of its own."""

    @pytest.mark.parametrize('source_id', [REPORTED_ID, ESCAPING_ID])
    def test_source_id_is_rejected(self, tmp_path, source_id):
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(docs_directory=docs_dir, collection_id='coll')
        doc = SourceDocument(nodes=[_node(BENIGN_NODE_ID, source_id)])

        with pytest.raises(ValueError, match='invalid characters'):
            list(handler.accept([doc]))

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(docs_directory=docs_dir, collection_id='coll')
        doc = SourceDocument(nodes=[_node(node_id, BENIGN_SOURCE_ID)])

        with pytest.raises(ValueError, match='invalid characters'):
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
        with pytest.raises(ValueError, match='invalid characters'):
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

    def test_a_sanitizer_still_gets_to_repair_a_byo_id(self, tmp_path):
        """Validation runs on the sanitizer's output, so the caller-supplied ids the
        sanitizer was added for still write instead of raising."""
        docs_dir = _nested_dir(str(tmp_path), 'docs')
        handler = FileBasedDocs(
            docs_directory=docs_dir,
            collection_id='coll',
            filename_sanitizer=lambda name: name.replace(' ', '_'),
        )
        doc = SourceDocument(nodes=[_node(BENIGN_NODE_ID, 'aws:annual report 2024')])

        list(handler.accept([doc]))

        written = Path(docs_dir) / 'coll' / 'aws:annual_report_2024' / f'{BENIGN_NODE_ID}.json'
        assert written.exists()


class TestCheckpointWriter:
    """`accept` touches a file named for the node id."""

    @pytest.mark.parametrize('node_id', [REPORTED_ID, ESCAPING_ID])
    def test_node_id_is_rejected(self, tmp_path, node_id):
        checkpoint_dir = _nested_dir(str(tmp_path), 'cp')
        writer = CheckpointWriter(
            inner=PassThrough(), checkpoint_dir=checkpoint_dir, checkpoint_name='cp',
        )

        with pytest.raises(ValueError, match='invalid characters'):
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
    """`_save_node_in_temp_dir` names a temp file for the node id, and the results
    loop names a temp file for the id the extraction reports back."""

    def _extractor(self, temp_dir, cls=_BatchExtractor):
        config = BatchConfig(
            role_arn='arn:aws:iam::123456789012:role/test', region='us-east-1', bucket_name='test',
        )
        return cls(
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

        with pytest.raises(ValueError, match='invalid characters'):
            extractor._save_node_in_temp_dir(TextNode(text='chunk', id_=node_id), temp_dir)

    def test_an_absolute_node_id_is_rejected(self, tmp_path):
        """join() discards the temp dir for an absolute id, so pre-fix this wrote
        wherever the id pointed."""
        temp_dir = _nested_dir(str(tmp_path), 'batch')
        extractor = self._extractor(temp_dir)

        with pytest.raises(ValueError, match='invalid characters'):
            extractor._save_node_in_temp_dir(TextNode(text='chunk', id_='/tmp/pwned'), temp_dir)

    def test_a_node_id_reported_by_the_extraction_is_rejected(self, tmp_path):
        """The results loop joins the id the output reports, not the one that was
        written, so the read side needs the guard too."""
        temp_dir = _nested_dir(str(tmp_path), 'batch')
        extractor = self._extractor(temp_dir, cls=_ForgedIdExtractor)

        with pytest.raises(ValueError, match='invalid characters'):
            list(extractor._process_nodes([TextNode(text='chunk', id_=BENIGN_NODE_ID)]))


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

        with pytest.raises(ValueError, match='invalid characters'):
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
