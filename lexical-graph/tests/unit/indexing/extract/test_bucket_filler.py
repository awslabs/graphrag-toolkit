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
        assert [len(j) for j in jobs2] == [25, 25]
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
        filler = BucketFiller(num_workers=4, max_batch_size=25000, min_batch_size=100)
        filler.add_document_chunks(make_chunks(30))
        jobs = filler.drain_round()
        assert len(jobs) == 1
        assert len(jobs[0]) == 30

    def test_multiple_documents_accumulate_in_order(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100)
        a = make_chunks(60, "a")
        b = make_chunks(60, "b")
        c = make_chunks(60, "c")
        filler.add_document_chunks(a)
        filler.add_document_chunks(b)
        filler.add_document_chunks(c)  # 180 total, under capacity 200
        jobs = filler.drain_round()
        # contiguous slice: [90, 90], documents split across the job boundary
        assert [len(j) for j in jobs] == [90, 90]
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
        # Two full rounds of [100,100] then a final even split of the 50 left
        assert [[len(j) for j in r] for r in rounds] == [[100, 100], [100, 100]]
        assert [len(j) for j in final] == [25, 25]


class TestUniformShortRound:
    """A round below capacity is split evenly so no worker sits idle."""

    @pytest.mark.parametrize(
        "chunks, workers, max_size, expected",
        [
            (1500, 4, 1000, [375] * 4),
            (2100, 4, 1000, [525] * 4),
            (900, 4, 1000, [225] * 4),
            (26000, 4, 25000, [6500] * 4),
            (1001, 4, 1000, [251, 250, 250, 250]),
        ],
    )
    def test_short_round_splits_evenly(self, chunks, workers, max_size, expected):
        filler = BucketFiller(num_workers=workers, max_batch_size=max_size, min_batch_size=100)
        filler.add_document_chunks(make_chunks(chunks))
        assert [len(j) for j in filler.drain_round()] == expected
        assert filler.pending() == 0

    def test_too_few_chunks_for_every_worker_stays_above_the_minimum(self):
        filler = BucketFiller(num_workers=4, max_batch_size=1000, min_batch_size=100)
        filler.add_document_chunks(make_chunks(250))
        assert [len(j) for j in filler.drain_round()] == [125, 125]

    def test_full_round_is_unchanged(self):
        filler = BucketFiller(num_workers=4, max_batch_size=1000, min_batch_size=100)
        filler.add_document_chunks(make_chunks(4500))
        assert [len(j) for j in filler.drain_round()] == [1000] * 4
        assert filler.pending() == 500

    def test_order_preserved_across_even_jobs(self):
        filler = BucketFiller(num_workers=4, max_batch_size=1000, min_batch_size=100)
        added = make_chunks(1503)
        filler.add_document_chunks(added)
        assert flat_ids(filler.drain_round()) == [n.node_id for n in added]

    @pytest.mark.parametrize("chunks", range(100, 4001, 37))
    def test_sizes_stay_within_limits_and_differ_by_at_most_one(self, chunks):
        filler = BucketFiller(num_workers=4, max_batch_size=1000, min_batch_size=100)
        filler.add_document_chunks(make_chunks(chunks))
        sizes = [len(j) for j in filler.drain_round()]
        assert max(sizes) - min(sizes) <= 1
        assert min(sizes) >= 100
        assert max(sizes) <= 1000


class TestMinimumJobSize:
    """drain_round must never emit a job below min_batch_size when the round holds
    at least min_batch_size chunks, because that job would fall back to synchronous
    extraction."""

    def test_min_batch_size_defaults_to_one(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100)
        filler.add_document_chunks(make_chunks(210))
        assert [len(j) for j in filler.drain_round()] == [70, 70, 70]

    def test_invalid_min_batch_size_raises(self):
        with pytest.raises(ValueError):
            BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=0)

    def test_minimum_wins_over_maximum_when_both_cannot_hold(self):
        # 210 chunks, max 100, min 100 cannot fit. Two jobs of 105 keep both batched;
        # the batch extractor re-splits a job above max_batch_size.
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(210))
        jobs = filler.drain_round()
        assert [len(j) for j in jobs] == [105, 105]

    def test_round_too_small_for_two_jobs_stays_one_job(self):
        filler = BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(180))
        assert [len(j) for j in filler.drain_round()] == [180]

    def test_exact_multiple_is_unaffected(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(300))
        assert [len(j) for j in filler.drain_round()] == [100, 100, 100]

    def test_job_count_boundary_at_the_minimum(self):
        below = BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=50)
        below.add_document_chunks(make_chunks(99))
        assert [len(j) for j in below.drain_round()] == [99]

        at_min = BucketFiller(num_workers=2, max_batch_size=100, min_batch_size=50)
        at_min.add_document_chunks(make_chunks(100))
        assert [len(j) for j in at_min.drain_round()] == [50, 50]

    def test_whole_round_below_minimum_stays_single_small_job(self):
        # No previous job to merge into: a genuinely sub-min round is returned as
        # one small job for the driver / sync fallback to handle.
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        filler.add_document_chunks(make_chunks(60))
        assert [len(j) for j in filler.drain_round()] == [60]

    def test_order_preserved_at_the_limit(self):
        filler = BucketFiller(num_workers=3, max_batch_size=100, min_batch_size=100)
        added = make_chunks(210)
        filler.add_document_chunks(added)
        jobs = filler.drain_round()
        assert flat_ids(jobs) == [n.node_id for n in added]
