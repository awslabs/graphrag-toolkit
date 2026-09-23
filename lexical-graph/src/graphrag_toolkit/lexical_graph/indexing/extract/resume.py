# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from dataclasses import dataclass, field
from os.path import join
from typing import Dict, List, Set

from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import (
    chunk_id_from_key,
    is_complete,
    is_completion_marker,
)
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import COMPLETE, PartitionRecord

logger = logging.getLogger(__name__)


def staged_source_ids(bucket_name:str, key_prefix:str, collection_id:str, s3_client,
                      for_jsonl:bool=False) -> Set[str]:
    """
    The sources a collection already holds whole.

    One listing, then the reader's own comparison: a source counts as staged
    when its markers account for the chunks beside them. A prefix can hold a
    marker and still be missing the part that never landed, so nothing less
    than that comparison is sound.

    The JSONL format keeps its node ids inside the objects, where a listing
    cannot reach them, so it is answered with nothing.
    """
    if for_jsonl:
        logger.debug('The JSONL format keeps its node ids where a listing cannot reach them')
        return set()

    collection_path = join(key_prefix, collection_id, '')

    content = {}
    markers = {}

    for page in s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=bucket_name, Prefix=collection_path
    ):
        for obj in page.get('Contents', []):
            key = obj['Key']

            source_id = key[len(collection_path):].split('/')[0]

            if not source_id:
                continue

            if is_completion_marker(key):
                markers.setdefault(source_id, []).append(key)
            else:
                content.setdefault(source_id, []).append(key)

    staged = set()

    for source_id, marker_keys in markers.items():
        chunk_keys = content.get(source_id)

        if not chunk_keys:
            continue

        source_doc_prefix = join(collection_path, source_id, '')
        chunk_ids = [chunk_id_from_key(key, source_doc_prefix) for key in chunk_keys]

        if is_complete(chunk_ids, marker_keys, bucket_name, s3_client):
            staged.add(source_id)

    return staged


@dataclass
class ResumeReport:
    """What a restart found, before it spends anything."""

    run_id:str
    partitions_complete:int = 0
    partitions_outstanding:int = 0
    sources_staged:int = 0
    staged_source_ids:Set[str] = field(default_factory=set)
    outstanding_jobs:List[str] = field(default_factory=list)

    @property
    def is_restart(self) -> bool:
        return bool(self.partitions_complete or self.partitions_outstanding or self.sources_staged)

    def describe(self) -> str:
        if not self.is_restart:
            return f'Starting a new run [run_id: {self.run_id}]'

        described = (
            f'Resuming a run [run_id: {self.run_id}, partitions already done: '
            f'{self.partitions_complete}, partitions to finish: {self.partitions_outstanding}, '
            f'sources already staged: {self.sources_staged}]'
        )

        return f'{described} Unfinished jobs: {self.outstanding_jobs}' if self.outstanding_jobs else described


def plan_resume(manifest_store, s3_client, for_jsonl:bool=False) -> ResumeReport:
    """
    What an earlier run of this id left behind, read before any work is
    submitted so an operator sees what a long run is about to redo.
    """
    partitions:Dict[str, PartitionRecord] = manifest_store.read_partitions(s3_client)

    outstanding = [record for record in partitions.values() if record.state != COMPLETE]

    staged = staged_source_ids(
        manifest_store.bucket_name, manifest_store.key_prefix,
        manifest_store.collection_id, s3_client, for_jsonl=for_jsonl
    )

    report = ResumeReport(
        run_id=manifest_store.run_id,
        partitions_complete=len(partitions) - len(outstanding),
        partitions_outstanding=len(outstanding),
        sources_staged=len(staged),
        staged_source_ids=staged,
        outstanding_jobs=[record.job_name for record in outstanding if record.job_name],
    )

    logger.info(report.describe())

    return report
