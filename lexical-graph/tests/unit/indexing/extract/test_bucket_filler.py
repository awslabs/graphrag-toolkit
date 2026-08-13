# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.extract.bucket_filler import BucketFiller


def make_chunks(n, prefix="c"):
    """Create n TextNodes with deterministic ids for order/identity comparisons."""
    return [TextNode(text=f"{prefix}-{i}", id_=f"{prefix}-{i}") for i in range(n)]


def flat_ids(jobs):
    """Flatten job node ids preserving order."""
    return [node.node_id for job in jobs for node in job]


class TestConstruction:
    def test_invalid_num_workers_raises(self):
        with pytest.raises(ValueError):
            BucketFiller(num_workers=0, max_batch_size=100)

    def test_invalid_max_batch_size_raises(self):
        with pytest.raises(ValueError):
            BucketFiller(num_workers=2, max_batch_size=0)

    def test_round_capacity_and_empty_state(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100)
        assert filler.round_capacity == 300
        assert filler.pending() == 0
        assert filler.drain_round() == []


class TestOvershoot:
    def test_empty_buffer_never_overshoots(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        assert filler.would_overshoot(500) is False  # oversized doc still accepted

    def test_overshoot_true_when_would_exceed_capacity(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)  # capacity 200
        filler.add_document_chunks(make_chunks(180))
        assert filler.would_overshoot(30) is True   # 180 + 30 > 200
        assert filler.would_overshoot(20) is False  # 180 + 20 == 200, fits


class TestContiguousPacking:
    def test_full_round_fills_every_job_to_max(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        filler.add_document_chunks(make_chunks(200))
        jobs = filler.drain_round()
        assert [len(j) for j in jobs] == [100, 100]  # exactly num_workers full jobs
        assert filler.pending() == 0

    def test_remainder_only_in_last_job(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100)
        filler.add_document_chunks(make_chunks(250))
        jobs = filler.drain_round()
        assert [len(j) for j in jobs] == [100, 100, 50]  # full, full, remainder

    def test_never_more_than_num_workers_jobs_per_round(self):
        # 250 chunks with capacity 200 -> one round takes 200 (2 jobs), 50 remains.
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        filler.add_document_chunks(make_chunks(250))
        jobs = filler.drain_round()
        assert len(jobs) == 2
        assert [len(j) for j in jobs] == [100, 100]
        assert filler.pending() == 50  # remainder carried forward
        # next drain takes the remainder
        jobs2 = filler.drain_round()
        assert [len(j) for j in jobs2] == [50]
        assert filler.pending() == 0

    def test_order_and_count_preserved(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        added = make_chunks(200)
        filler.add_document_chunks(added)
        jobs = filler.drain_round()
        assert flat_ids(jobs) == [n.node_id for n in added]  # exact order preserved

    def test_add_ignores_empty(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        filler.add_document_chunks([])
        assert filler.pending() == 0


class TestConsolidation:
    def test_small_total_single_job(self):
        filler = BucketFiller(num_workers=4, max_batch_size=25000)
        filler.add_document_chunks(make_chunks(30))
        jobs = filler.drain_round()
        assert len(jobs) == 1
        assert len(jobs[0]) == 30

    def test_final_round_minimum_jobs(self):
        # 230 chunks, capacity 500 -> ceil(230/100)=3 jobs [100,100,30], <= num_workers(5)
        filler = BucketFiller(num_workers=5, max_batch_size=100)
        filler.add_document_chunks(make_chunks(230))
        jobs = filler.drain_round()
        assert [len(j) for j in jobs] == [100, 100, 30]

    def test_multiple_documents_accumulate_in_order(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        a = make_chunks(60, "a")
        b = make_chunks(60, "b")
        c = make_chunks(60, "c")
        filler.add_document_chunks(a)
        filler.add_document_chunks(b)
        filler.add_document_chunks(c)  # 180 total, under capacity 200
        jobs = filler.drain_round()
        # contiguous slice: [100, 80], documents split across the job boundary
        assert [len(j) for j in jobs] == [100, 80]
        assert flat_ids(jobs) == [n.node_id for n in (a + b + c)]


class TestOversizedDocument:
    def test_document_larger_than_round_drains_in_full_rounds(self):
        # Single 450-chunk doc, capacity 200. Caller drains while pending>=capacity.
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        filler.add_document_chunks(make_chunks(450))
        rounds = []
        while filler.pending() >= filler.round_capacity:
            rounds.append(filler.drain_round())
        final = filler.drain_round()  # remainder
        # Two full rounds of [100,100] then a final [50]
        assert [[len(j) for j in r] for r in rounds] == [[100, 100], [100, 100]]
        assert [len(j) for j in final] == [50]
