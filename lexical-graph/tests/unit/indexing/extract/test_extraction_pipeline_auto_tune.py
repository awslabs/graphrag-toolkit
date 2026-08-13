# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for auto-tuning batch extraction in ExtractionPipeline.

These tests avoid launching the real ProcessPoolExecutor by stubbing
``_run_extractor_round`` (which is the boundary where worker processes would be
spawned). The focus is on the auto-tuning orchestration: detection/dispatch,
incremental chunking, bucket filling, round submission, and consolidation.
"""

import pytest
from unittest.mock import patch
from llama_index.core.llms import MockLLM
from llama_index.core.schema import Document, TextNode, NodeRelationship, RelatedNodeInfo

from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline import ExtractionPipeline


class _StubBatchExtractor(BatchExtractorBase):
    """Minimal concrete batch extractor for detection/dispatch tests."""

    @classmethod
    def class_name(cls) -> str:
        return "_StubBatchExtractor"

    def _get_json(self, node, llm, inference_parameters):
        return {}

    def _run_non_batch_extractor(self, nodes):
        return list(nodes)

    def _update_node(self, node, node_metadata_map):
        return node


def make_batch_extractor(auto_tune=False, max_batch_size=100):
    config = BatchConfig(
        role_arn="arn:aws:iam::123456789012:role/test-role",
        region="us-east-1",
        bucket_name="test-bucket",
        max_batch_size=max_batch_size,
        auto_tune=auto_tune,
    )
    llm = LLMCache(llm=MockLLM(), enable_cache=False)
    return _StubBatchExtractor(
        batch_config=config,
        llm=llm,
        prompt_template="prompt",
        description="Test",
    )


def make_source_documents(num_docs):
    """Build source documents, each already carrying one chunk TextNode with a
    SOURCE relationship (so no node parser is required)."""
    docs = []
    for d in range(num_docs):
        source = Document(text=f"source-{d}", id_=f"src-{d}")
        node = TextNode(text=f"chunk-{d}", id_=f"chunk-{d}")
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
            node_id=f"src-{d}", metadata={}
        )
        docs.append(node)
    return docs


def make_multichunk_documents(chunk_counts):
    """Build pre-chunked inputs where document i already has ``chunk_counts[i]``
    chunk TextNodes sharing one SOURCE id. No node parser runs, so the chunk
    count per document is exactly as specified."""
    nodes = []
    for d, count in enumerate(chunk_counts):
        for c in range(count):
            node = TextNode(text=f"doc{d}-chunk{c}", id_=f"doc{d}-chunk{c}")
            node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
                node_id=f"src-{d}", metadata={}
            )
            nodes.append(node)
    return nodes


class TestAutoTuneDetection:
    def test_auto_tune_disabled_by_default(self):
        pipeline = ExtractionPipeline(components=[make_batch_extractor(auto_tune=False)])
        assert pipeline._auto_tune is False

    def test_auto_tune_enabled_from_batch_config(self):
        pipeline = ExtractionPipeline(components=[make_batch_extractor(auto_tune=True)])
        assert pipeline._auto_tune is True

    def test_no_batch_extractor_means_no_auto_tune(self):
        pipeline = ExtractionPipeline(components=[])
        assert pipeline._auto_tune is False
        assert pipeline._batch_config is None


class TestDispatch:
    def test_disabled_uses_fixed_batch_path(self):
        pipeline = ExtractionPipeline(components=[make_batch_extractor(auto_tune=False)])
        with patch.object(pipeline, "_extract_fixed_batch", return_value=iter([])) as fixed, \
             patch.object(pipeline, "_extract_auto_tuned", return_value=iter([])) as auto:
            list(pipeline.extract([Document(text="d")]))
            fixed.assert_called_once()
            auto.assert_not_called()

    def test_enabled_uses_auto_tuned_path(self):
        pipeline = ExtractionPipeline(components=[make_batch_extractor(auto_tune=True)])
        with patch.object(pipeline, "_extract_fixed_batch", return_value=iter([])) as fixed, \
             patch.object(pipeline, "_extract_auto_tuned", return_value=iter([])) as auto:
            list(pipeline.extract([Document(text="d")]))
            auto.assert_called_once()
            fixed.assert_not_called()


class TestAutoTunedRounds:
    """Exercise the streaming/bucket/round orchestration with the extractor tail
    stubbed so no worker processes are spawned. The stub returns its input nodes
    unchanged, so output chunk identity can be compared to input."""

    def _run(self, pipeline, docs):
        captured_rounds = []

        def fake_round(round_buckets, extractor_transforms):
            captured_rounds.append([[n.node_id for n in b] for b in round_buckets])
            for bucket in round_buckets:
                for node in bucket:
                    yield node

        with patch.object(pipeline, "_run_extractor_round", side_effect=fake_round):
            output = list(pipeline.extract(docs))
        return output, captured_rounds

    def test_small_input_consolidates_to_single_round(self):
        # max_batch_size large, few docs -> one consolidated job at exhaustion.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=25000)],
            num_workers=4,
        )
        docs = make_source_documents(5)
        output, rounds = self._run(pipeline, docs)

        assert len(rounds) == 1          # single submission round
        assert len(rounds[0]) == 1       # consolidated into one job
        assert len(rounds[0][0]) == 5    # all 5 chunks
        assert len(output) == 5

    def test_full_buckets_trigger_round(self):
        # max_batch_size=100, num_workers=2, capacity 200. 250 chunks => one full
        # round of 200, then a 50-chunk tail. 50 < BEDROCK_MIN(100), so the tail
        # merges into the round's last job (150) rather than a sub-min round.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_source_documents(250)
        output, rounds = self._run(pipeline, docs)

        assert len(rounds) == 1
        assert sorted(len(b) for b in rounds[0]) == [100, 150]  # tail merged into last job
        assert sum(len(b) for r in rounds for b in r) == 250
        assert len(output) == 250

    def test_chunk_equivalence_all_nodes_processed(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_source_documents(300)
        output, rounds = self._run(pipeline, docs)

        # Every input chunk is submitted exactly once (ids are rewritten by the
        # IdRewriter, so compare counts and uniqueness rather than raw ids).
        submitted = [nid for r in rounds for bucket in r for nid in bucket]
        assert len(submitted) == 300
        assert len(set(submitted)) == 300
        assert len(output) == 300

    def test_no_bucket_exceeds_max_batch_size(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=3,
        )
        docs = make_source_documents(500)
        _, rounds = self._run(pipeline, docs)

        for r in rounds:
            for bucket in r:
                assert len(bucket) <= 100

    def test_never_more_jobs_than_num_workers(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_source_documents(500)
        _, rounds = self._run(pipeline, docs)

        for r in rounds:
            assert len(r) <= 2

    def test_empty_input_yields_nothing(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        output, rounds = self._run(pipeline, [])
        assert output == []
        assert rounds == []

    def test_batch_size_acts_as_document_cap(self):
        # Explicit batch_size=2 caps documents per round. 5 single-chunk docs
        # would flush after docs 2 and 4; the final 1-chunk tail (< BEDROCK_MIN)
        # merges into the previous round, yielding 2 emitted rounds.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=25000)],
            num_workers=2,
            batch_size=2,
        )
        assert pipeline._explicit_batch_size == 2
        docs = make_source_documents(5)
        _, rounds = self._run(pipeline, docs)
        # Rounds: [2] then [2 + merged 1 = 3]. All 5 chunks submitted.
        assert len(rounds) == 2
        assert sum(len(b) for r in rounds for b in r) == 5

    def test_sub_min_tail_merges_into_previous_round(self):
        # num_workers=1, max_batch_size=100. Doc A (60) flushes when doc B (60)
        # would overshoot; the held round is [60]. Doc B's 60-chunk tail is
        # < BEDROCK_MIN(100), so it merges into the held round's last job -> a
        # single 120-chunk job, which is batched rather than two sub-min rounds
        # that would both fall back to synchronous extraction.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=1,
        )
        docs = make_multichunk_documents([60, 60])
        output, rounds = self._run(pipeline, docs)

        assert [[len(j) for j in r] for r in rounds] == [[120]]
        assert sum(len(b) for r in rounds for b in r) == 120

    def test_tail_not_merged_when_it_would_exceed_bedrock_max(self):
        # The tail-merge must never grow a job past Bedrock's hard per-job limit.
        # BEDROCK_MAX_BATCH_SIZE is patched low so the guard fires with small data:
        # held last job (250) + tail (60) = 310 > patched max (260), so the tail
        # is left as its own round instead of being merged.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=250)],
            num_workers=1,
        )
        docs = make_multichunk_documents([250, 60])
        with patch(
            "graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline.BEDROCK_MAX_BATCH_SIZE",
            260,
        ):
            output, rounds = self._run(pipeline, docs)

        assert [[len(j) for j in r] for r in rounds] == [[250], [60]]  # not merged
        assert sum(len(b) for r in rounds for b in r) == 310

    def test_oversized_document_split_across_rounds(self):
        # A single document with more chunks than a whole round's capacity
        # (num_workers=2 x max_batch_size=100 = 200) must still be fully
        # processed, split across successive full rounds.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_multichunk_documents([250])
        output, rounds = self._run(pipeline, docs)

        total_submitted = sum(len(b) for r in rounds for b in r)
        assert total_submitted == 250
        # A single 250-chunk doc drains a full round of 200 ([100,100]); the
        # 50-chunk tail (< BEDROCK_MIN) merges into the last job -> [100,150].
        # A merged tail may exceed max_batch_size (as split_nodes' tail-merge
        # does), but never by more than BEDROCK_MIN.
        for r in rounds:
            for bucket in r:
                assert len(bucket) <= 100 + 100  # max_batch_size + BEDROCK_MIN bound

    def test_sub_min_tail_not_merged_when_no_prior_round(self):
        # A tiny corpus that never fills a round has no prior round to merge
        # into, so its single sub-min round is emitted as-is (the extractor
        # routes it to the synchronous fallback downstream).
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=25000)],
            num_workers=2,
        )
        docs = make_source_documents(30)  # 30 chunks, well below BEDROCK_MIN
        output, rounds = self._run(pipeline, docs)
        assert [[len(j) for j in r] for r in rounds] == [[30]]
        assert len(output) == 30

    def test_above_min_tail_is_its_own_round(self):
        # A final remainder >= BEDROCK_MIN is submitted as its own round (not
        # merged), since it is a valid standalone batch job.
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_source_documents(300)  # 200 + 100; tail of 100 is >= BEDROCK_MIN
        output, rounds = self._run(pipeline, docs)
        assert len(rounds) == 2
        assert sorted(len(b) for b in rounds[0]) == [100, 100]
        assert [len(b) for b in rounds[1]] == [100]
        assert len(output) == 300

    def test_multichunk_documents_all_chunks_processed(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_multichunk_documents([30, 45, 55, 40, 60])  # 230 chunks total
        output, rounds = self._run(pipeline, docs)

        total_submitted = sum(len(b) for r in rounds for b in r)
        assert total_submitted == 230

    def test_jobs_filled_to_max_batch_size(self):
        # Regression guard: the contiguous packing must fill every job to
        # max_batch_size except the last job of each round. (The prior
        # least-filled+keep-whole design under-filled every job.)
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=3,
        )
        docs = make_multichunk_documents([70] * 20)  # 1400 chunks
        _, rounds = self._run(pipeline, docs)

        for r in rounds:
            # Every job except the last in the round is exactly max_batch_size.
            for job in r[:-1]:
                assert len(job) == 100
            assert 0 < len(r[-1]) <= 100
        # Total jobs should be close to the theoretical minimum (1400/100 = 14),
        # not inflated by under-filling. Allow a small slack for round tails.
        total_jobs = sum(len(r) for r in rounds)
        assert total_jobs <= 16


class TestDocumentContiguity:
    """Verify a document's chunks reconstruct into a single output SourceDocument,
    even when the extractor reorders nodes by id. Uses the real _emit_extracted /
    _source_documents_from_base_nodes reconstruction and an extractor stub that
    reproduces BatchExtractorBase's sort-by-node-id."""

    def _run_with_realistic_extractor(self, pipeline, docs):
        captured_rounds = []

        def realistic_round(round_buckets, extractor_transforms):
            captured_rounds.append([len(b) for b in round_buckets])
            # BatchExtractorBase sorts nodes by id within a worker and recombines
            # results across workers; emulate that ordering hazard here.
            all_nodes = [n for bucket in round_buckets for n in bucket]
            all_nodes.sort(key=lambda n: n.node_id)
            for node in all_nodes:
                yield node

        with patch.object(pipeline, "_run_extractor_round", side_effect=realistic_round):
            output = list(pipeline.extract(docs))
        return output, captured_rounds

    def test_one_source_document_per_input_document(self):
        # Multi-chunk docs + a small max_batch_size so several rounds occur. Even
        # with the extractor reordering nodes by id, each input document must
        # reconstruct into exactly one output SourceDocument (no fragmentation).
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=True, max_batch_size=100)],
            num_workers=2,
        )
        docs = make_multichunk_documents([40] * 8)  # 8 sources, 320 chunks, >1 round

        output, rounds = self._run_with_realistic_extractor(pipeline, docs)

        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        assert all(isinstance(sd, SourceDocument) for sd in output)
        # Exactly one output SourceDocument per input document.
        source_ids = set()
        for sd in output:
            ids_in_doc = {
                n.relationships[NodeRelationship.SOURCE].node_id for n in sd.nodes
            }
            assert len(ids_in_doc) == 1  # every chunk in the SourceDocument shares one source
            source_ids |= ids_in_doc
        assert len(output) == len(source_ids)  # no source split across two SourceDocuments
        assert len(output) == 8


class TestFixedBatchPathUnaffected:
    """auto_tune=False must retain the fixed-batch_size document-pull behavior."""

    def test_disabled_pipeline_reports_explicit_batch_size(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=False, max_batch_size=100)],
            num_workers=2,
            batch_size=8,
        )
        assert pipeline._auto_tune is False
        assert pipeline.batch_size == 8

    def test_disabled_dispatches_to_fixed_batch(self):
        pipeline = ExtractionPipeline(
            components=[make_batch_extractor(auto_tune=False, max_batch_size=100)],
            num_workers=2,
            batch_size=4,
        )
        with patch.object(pipeline, "_extract_fixed_batch", return_value=iter([])) as fixed:
            list(pipeline.extract([Document(text="d")]))
            fixed.assert_called_once()
