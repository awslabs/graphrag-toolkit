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


class TestSubMinimumTailMerge:
    """drain_round must fold a trailing sub-minimum job into the previous job
    (as split_nodes does), so a round with >= min_batch_size chunks never emits
    an under-minimum job that would fall back to synchronous extraction."""

    def test_min_batch_size_defaults_to_one_and_does_not_merge(self):
        # Backwards-compatible default: no minimum, so plain slicing is retained.
        filler = BucketFiller(num_workers=3, max_batch_size=100)
        filler.add_document_chunks(make_chunks(210))
        assert [len(j) for j in filler.drain_round()] == [100, 100, 10]

    def test_invalid_min_batch_size_raises(self):
        with pytest.raises(ValueError):
            BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=0)

    def test_trailing_sub_min_job_folds_into_previous(self):
        # 210 chunks, max 100, min 100 -> [100, 110] (the 10-tail merges), matching
        # split_nodes rather than [100, 100, 10].
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(210))
        jobs = filler.drain_round()
        assert [len(j) for j in jobs] == [100, 110]
        assert all(len(j) >= 100 for j in jobs)

    def test_two_job_round_with_sub_min_tail_folds_to_one_job(self):
        # 180 chunks, max 100, min 100 -> [100, 80] would leave an 80-tail; merges
        # into a single 180 job.
        filler = BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(180))
        assert [len(j) for j in filler.drain_round()] == [180]

    def test_exact_multiple_is_unaffected(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(300))
        assert [len(j) for j in filler.drain_round()] == [100, 100, 100]

    def test_tail_just_below_minimum_merges_at_minimum_stays(self):
        # min < max makes the boundary explicit: a 49-tail (< min 50) merges,
        # a 50-tail (== min 50) is a valid job and stays.
        below = BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=50)
        below.add_document_chunks(make_chunks(149))
        assert [len(j) for j in below.drain_round()] == [149]

        at_min = BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=50)
        at_min.add_document_chunks(make_chunks(150))
        assert [len(j) for j in at_min.drain_round()] == [100, 50]

    def test_whole_round_below_minimum_stays_single_small_job(self):
        # No previous job to merge into: a genuinely sub-min round is returned as
        # one small job for the driver / sync fallback to handle.
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(60))
        assert [len(j) for j in filler.drain_round()] == [60]

    def test_order_preserved_after_merge(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        added = make_chunks(210)
        filler.add_document_chunks(added)
        jobs = filler.drain_round()
        assert flat_ids(jobs) == [n.node_id for n in added]
