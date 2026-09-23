# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import re

from dataclasses import dataclass, asdict, fields
from os.path import join
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import EncryptedPut, RUN_ARTIFACT_DIR, node_ids_hash
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import MISSING_KEY_CODES
from graphrag_toolkit.lexical_graph.utils.id_validation import validate_id_segment

logger = logging.getLogger(__name__)

PARTITION_DIR = 'partitions'
ROLLUP_NAME = 'rollup.json'

SUBMITTED = 'submitted'
COMPLETE = 'complete'

# Bedrock rejects a longer job name; S3 rejects a larger delete.
MAX_JOB_NAME = 63
MAX_KEYS_PER_DELETE = 1000

_NOT_IN_A_JOB_NAME = re.compile(r'[^a-zA-Z0-9]+')


def partition_id(node_ids, stage:str) -> str:
    """
    What a partition is known by: its stage, and a digest of its node ids.

    Two stages divide the same nodes identically, so the stage is what keeps
    the topic partition apart from the proposition one.
    """
    return node_ids_hash([f'{stage}\x00{node_id}' for node_id in node_ids])


def batch_job_name(prefix:str, run_id:str, partition:str, attempt:int, suffix:str='') -> str:
    """
    A job name saying which run, partition and attempt it belongs to, for the
    operator reading the console. A restart finds a job by its recorded ARN.

    The ids are shortened to fit Bedrock's limit, and the suffix keeps two
    submissions of one attempt apart, which Bedrock would otherwise refuse.
    """
    tail = f'{_NOT_IN_A_JOB_NAME.sub("-", run_id).strip("-")[:20]}-{partition[:10]}-a{attempt}'
    if suffix:
        tail = f'{tail}-{_NOT_IN_A_JOB_NAME.sub("-", suffix).strip("-")[:12]}'
    head = _NOT_IN_A_JOB_NAME.sub('-', prefix).strip('-')[:max(0, MAX_JOB_NAME - len(tail) - 1)]

    name = f'{head}-{tail}' if head else tail

    return name[:MAX_JOB_NAME]


@dataclass
class PartitionRecord:
    """What a run did with one partition, and how far it got."""

    partition_id:str
    attempt:int
    state:str
    job_name:Optional[str] = None
    job_arn:Optional[str] = None
    output_path:Optional[str] = None
    input_filename:Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=4)

    @classmethod
    def from_json(cls, body:str) -> 'PartitionRecord':
        """A record read back, without the fields a later build added."""
        recorded = json.loads(body)
        known = {f.name for f in fields(cls)}

        unknown = sorted(set(recorded) - known)
        if unknown:
            logger.warning(f'Ignoring partition record fields this version does not know {unknown}')

        return cls(**{name: value for name, value in recorded.items() if name in known})


class RunManifestStore(EncryptedPut):
    """
    Where a run records each partition, beside the collection it stages into.

    One partition has one writer, the worker holding it, so nothing contends
    and no conditional write is needed.
    """

    def __init__(self,
                 bucket_name:str,
                 key_prefix:str,
                 collection_id:str,
                 run_id:str,
                 s3_encryption_key_id:Optional[str]=None):
        validate_id_segment(run_id, 'run_id')

        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.collection_id = collection_id
        self.run_id = run_id
        self.s3_encryption_key_id = s3_encryption_key_id
        self._rollup = None

    @property
    def run_path(self) -> str:
        return join(self.key_prefix, self.collection_id, RUN_ARTIFACT_DIR, self.run_id)

    def partition_key(self, partition:str) -> str:
        return join(self.run_path, PARTITION_DIR, f'{partition}.json')

    def rollup_key(self) -> str:
        return join(self.run_path, ROLLUP_NAME)

    def _read_json(self, key:str, s3_client) -> Optional[str]:
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in MISSING_KEY_CODES:
                return None
            raise

        return response['Body'].read().decode('UTF-8')

    def read(self, partition:str, s3_client) -> Optional[PartitionRecord]:
        """
        What this run last recorded for a partition, or None if nothing.

        The rollup answers for the partitions folded into it, which is every
        one a finished run completed.
        """
        body = self._read_json(self.partition_key(partition), s3_client)

        if body is not None:
            return PartitionRecord.from_json(body)

        return self._rolled_up(s3_client).get(partition)

    def _rolled_up(self, s3_client) -> Dict[str, PartitionRecord]:
        """The rollup, read once: nothing adds to it while the workers run."""
        if self._rollup is None:
            self._rollup = self.read_rollup(s3_client)

        return self._rollup

    def write(self, record:PartitionRecord, s3_client):
        """Record what a partition is doing. The last write for a partition wins."""
        key = self.partition_key(record.partition_id)
        logger.debug(
            f'Recording a partition [bucket: {self.bucket_name}, key: {key}, '
            f'state: {record.state}, attempt: {record.attempt}]'
        )
        self._put(key, record.to_json(), 'application/json', s3_client)

        if self._rollup is not None:
            self._rollup[record.partition_id] = record

    def read_rollup(self, s3_client) -> Dict[str, PartitionRecord]:
        """The partitions an earlier run of this id rolled up as complete."""
        body = self._read_json(self.rollup_key(), s3_client)

        if body is None:
            return {}

        return {
            partition: PartitionRecord.from_json(json.dumps(record))
            for partition, record in json.loads(body).get('partitions', {}).items()
        }

    def list_partition_keys(self, s3_client) -> List[str]:
        prefix = join(self.run_path, PARTITION_DIR, '')
        pages = s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=self.bucket_name, Prefix=prefix
        )

        return [obj['Key'] for page in pages for obj in page.get('Contents', [])]

    def read_partitions(self, s3_client) -> Dict[str, PartitionRecord]:
        """
        Every partition this run has a record for. A record still in its own
        file was written after the rollup, so it speaks for its partition.
        """
        partitions = self.read_rollup(s3_client)

        for key in self.list_partition_keys(s3_client):
            body = self._read_json(key, s3_client)
            if body is None:
                continue
            record = PartitionRecord.from_json(body)
            partitions[record.partition_id] = record

        return partitions

    def merge_rollup(self, s3_client) -> Dict[str, PartitionRecord]:
        """
        Fold the completed records into the rollup, so a restart reads one
        object rather than one per partition. Written before anything is
        removed, and a partition still working is left alone.
        """
        partitions = self.read_partitions(s3_client)

        rolled_up = {
            partition: record
            for partition, record in partitions.items()
            if record.state == COMPLETE
        }

        self._put(
            self.rollup_key(),
            json.dumps({'partitions': {p: asdict(r) for p, r in rolled_up.items()}}, indent=4),
            'application/json',
            s3_client,
        )

        merged = [self.partition_key(partition) for partition in rolled_up]

        for start in range(0, len(merged), MAX_KEYS_PER_DELETE):
            s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': [
                    {'Key': key} for key in merged[start:start + MAX_KEYS_PER_DELETE]
                ]},
            )

        logger.debug(f'Rolled up {len(rolled_up)} completed partitions [run_id: {self.run_id}]')

        return rolled_up
