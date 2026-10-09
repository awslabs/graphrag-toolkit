# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import re

from dataclasses import MISSING, dataclass, asdict, fields
from os.path import basename, join
from typing import Dict, List, Optional

from graphrag_toolkit.lexical_graph.indexing.extract.run_store import RunArtifactStore, RunRecordError
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import node_ids_hash, to_batches
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

_MODEL_INVOCATION_JOB_ARN = re.compile(
    r'arn:[a-z0-9-]+:bedrock:[a-z0-9-]+:\d{12}:model-invocation-job/[A-Za-z0-9]+'
)


def _validate_job_arn(value:Optional[str]) -> None:
    """
    Reject an arn that does not name a job this code submits.

    A restart asks Bedrock for this arn's status and takes a Completed answer
    as its partition's, so an arn naming another kind of job, or another
    service, would have the run skip extraction on somebody else's say-so.
    None is a record from a build that kept no arn.
    """
    if value is None:
        return
    if not _MODEL_INVOCATION_JOB_ARN.fullmatch(value):
        raise RunRecordError(f'A partition record has a job_arn that is not a model invocation job: {value!r}')


def _validate_input_filename(value:Optional[str]) -> None:
    """
    Reject a filename that is not the plain name a run wrote.

    It is the name a restart matches output files against, in S3 to pick the
    folder to download and on disk to pick the files to parse, so a name
    holding a separator reaches past both. None is a record from a build that
    kept no filename.
    """
    if value is None:
        return
    if not value or value in ('.', '..') or basename(value) != value:
        raise RunRecordError(f'A partition record has an input_filename that is not a plain filename: {value!r}')


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
        """
        A record read back, without the fields a later build added.

        A record that lost a field it cannot do without says so, rather than
        failing as a missing argument. Its partition id is checked here as
        well as where a key is built from it, because a record is the one way
        an id that was never hashed reaches the paths a recovery removes. The
        job and the filename are checked for the same reason: a restart hands
        them to Bedrock and to S3 without looking. The output path is checked
        where the batch config says which prefix a run writes.
        """
        recorded = json.loads(body)
        known = {f.name for f in fields(cls)}

        unknown = sorted(set(recorded) - known)
        if unknown:
            logger.warning(f'Ignoring partition record fields this version does not know {unknown}')

        required = [f.name for f in fields(cls) if f.default is MISSING]
        missing = [name for name in required if name not in recorded]
        if missing:
            raise RunRecordError(f'A partition record is missing {missing}')

        validate_id_segment(recorded['partition_id'], 'partition_id')
        _validate_job_arn(recorded.get('job_arn'))
        _validate_input_filename(recorded.get('input_filename'))

        return cls(**{name: value for name, value in recorded.items() if name in known})


class RunManifestStore(RunArtifactStore):
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
        super().__init__(bucket_name, key_prefix, collection_id, s3_encryption_key_id)

        # Checked here as well as on every path built from it, so a run id that
        # climbs out of the collection is refused before any work starts.
        self.run_path(run_id)

        self.run_id = run_id
        self._rollup = None

    def partition_key(self, partition:str) -> str:
        """Where one partition's record sits. An id that climbs out is refused."""
        validate_id_segment(partition, 'partition_id')
        return join(self.run_path(self.run_id), PARTITION_DIR, f'{partition}.json')

    def rollup_key(self) -> str:
        return join(self.run_path(self.run_id), ROLLUP_NAME)

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

        try:
            recorded = json.loads(body).get('partitions', {})
        except (ValueError, AttributeError) as e:
            logger.warning(f'Ignoring a rollup that cannot be read, its partitions will be redone [key: {self.rollup_key()}, error: {e!s}]')
            return {}

        partitions = {}
        for partition, record in recorded.items():
            readable = self._readable(json.dumps(record), self.rollup_key())
            if readable is not None:
                partitions[partition] = readable

        return partitions

    @staticmethod
    def _readable(body:str, key:str) -> Optional[PartitionRecord]:
        """The record, or None if it cannot be read, so its partition is redone."""
        try:
            return PartitionRecord.from_json(body)
        except (ValueError, TypeError, RunRecordError) as e:
            logger.warning(f'Ignoring a partition record that cannot be read, its partition will be redone [key: {key}, error: {e!s}]')
            return None

    def list_partition_keys(self, s3_client) -> List[str]:
        prefix = join(self.run_path(self.run_id), PARTITION_DIR, '')
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
            record = self._readable(body, key)
            if record is not None:
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

        for batch in to_batches(merged, MAX_KEYS_PER_DELETE):
            s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': [{'Key': key} for key in batch]},
            )

        logger.debug(f'Rolled up {len(rolled_up)} completed partitions [run_id: {self.run_id}]')

        return rolled_up
