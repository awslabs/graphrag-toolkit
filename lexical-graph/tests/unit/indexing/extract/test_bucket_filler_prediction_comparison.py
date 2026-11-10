# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compare even short rounds with an exact prediction of the chunks left.

Rounds run one after another and a round lasts as long as its largest job, so
elapsed time is modelled as the sum of each round's largest job. This assumes job
time scales with job size. The prediction is given the true total, which is the
best any estimate could do.
"""

import math

import pytest

from graphrag_toolkit.lexical_graph.indexing.extract.bucket_filler import BucketFiller

MIN_BATCH_SIZE = 100

SCENARIOS = [
    # chunks, num_workers, max_batch_size
    (900, 4, 1000),
    (1500, 4, 1000),
    (2100, 4, 1000),
    (26000, 4, 25000),
    (100050, 4, 25000),
    (100150, 4, 25000),
    (100399, 4, 25000),
    (126000, 4, 25000),
    (175000, 4, 25000),
    (126000, 8, 25000),
    (26000, 8, 25000),
    (1400, 3, 250),
]


def even_jobs(chunks, num_jobs):
    size, larger_jobs = divmod(chunks, num_jobs)
    return [size + 1 if job < larger_jobs else size for job in range(num_jobs)]


def rounds_without_prediction(chunks, num_workers, max_batch_size):
    """Job sizes per round from BucketFiller, with the pipeline's sub-minimum tail merge."""
    filler = BucketFiller(num_workers, max_batch_size, MIN_BATCH_SIZE)
    filler.add_document_chunks(list(range(chunks)))
    rounds = []
    while filler.pending():
        rounds.append([len(job) for job in filler.drain_round()])
    if len(rounds) > 1 and sum(rounds[-1]) < MIN_BATCH_SIZE:
        rounds[-2][-1] += sum(rounds.pop())
    return rounds


def rounds_with_prediction(chunks, num_workers, max_batch_size):
    """The same chunks spread evenly over every job of every round they need."""
    num_rounds = math.ceil(chunks / (num_workers * max_batch_size))
    rounds = []
    for round_chunks in even_jobs(chunks, num_rounds):
        num_jobs = max(1, min(num_workers, round_chunks // MIN_BATCH_SIZE))
        rounds.append(even_jobs(round_chunks, num_jobs))
    return rounds


def modelled_time(rounds):
    return sum(max(jobs) for jobs in rounds)


@pytest.mark.parametrize('chunks, num_workers, max_batch_size', SCENARIOS)
class TestPredictionComparison:

    def test_both_keep_every_chunk(self, chunks, num_workers, max_batch_size):
        for build in (rounds_without_prediction, rounds_with_prediction):
            assert sum(map(sum, build(chunks, num_workers, max_batch_size))) == chunks

    def test_prediction_saves_less_than_two_minimum_jobs(self, chunks, num_workers, max_batch_size):
        without = modelled_time(rounds_without_prediction(chunks, num_workers, max_batch_size))
        predicted = modelled_time(rounds_with_prediction(chunks, num_workers, max_batch_size))
        assert without - predicted < 2 * MIN_BATCH_SIZE

    def test_prediction_never_needs_fewer_rounds(self, chunks, num_workers, max_batch_size):
        without = rounds_without_prediction(chunks, num_workers, max_batch_size)
        predicted = rounds_with_prediction(chunks, num_workers, max_batch_size)
        assert len(without) <= len(predicted)
