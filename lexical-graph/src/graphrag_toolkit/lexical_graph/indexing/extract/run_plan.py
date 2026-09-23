# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging

from collections import Counter
from dataclasses import dataclass, field, asdict, fields
from os.path import join
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import EncryptedPut, RUN_ARTIFACT_DIR
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import MISSING_KEY_CODES
from graphrag_toolkit.lexical_graph.utils.id_validation import validate_id_segment

logger = logging.getLogger(__name__)

PLAN_NAME = 'plan.json'

# The settings a restart must find unchanged. A run that changed one of these
# would submit different jobs from the run it is continuing, and pay for work
# already done.
#
# The worker count is not among them. Extraction clamps it to the host's core
# count, so a restart on a smaller machine offers a smaller number through no
# choice of the operator's. The recorded count is adopted instead, which is
# what pinning it was for.
SPLIT_DECIDING = ('batch_size',)

# Codes S3 and S3-compatible endpoints use when a conditional write loses.
ALREADY_WRITTEN_CODES = ('PreconditionFailed', 'ConditionalRequestConflict', '412')


def run_plan_key(key_prefix:str, collection_id:str, run_id:str) -> str:
    validate_id_segment(run_id, 'run_id')
    return join(key_prefix, collection_id, RUN_ARTIFACT_DIR, run_id, PLAN_NAME)


class RunPlanMismatch(Exception):
    """A restart whose configuration differs from the plan it is continuing."""


@dataclass
class RunPlan:
    """
    What a run fixed before it submitted any work: the document order, the
    settings that divide them, and the configuration it ran under.
    """

    run_id:str
    document_ids:List[str]
    num_workers:int
    batch_size:int
    config:Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=4)

    @classmethod
    def from_json(cls, body:str) -> 'RunPlan':
        """
        A plan read back from storage.

        Fields this version does not know are dropped, so a plan written by a
        later build still restarts rather than failing on every attempt.
        """
        recorded = json.loads(body)
        known = {f.name for f in fields(cls)}

        unknown = sorted(set(recorded) - known)
        if unknown:
            logger.warning(f'Ignoring run plan fields this version does not know {unknown}')

        return cls(**{name: value for name, value in recorded.items() if name in known})


class RunPlanStore(EncryptedPut):
    """
    Where a run's plan is kept, beside the collection it divides.

    A plan is written once and read on every restart of the same run id.
    """

    def __init__(self, bucket_name:str, key_prefix:str, collection_id:str, s3_encryption_key_id:Optional[str]=None):
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.collection_id = collection_id
        self.s3_encryption_key_id = s3_encryption_key_id

    def key(self, run_id:str) -> str:
        return run_plan_key(self.key_prefix, self.collection_id, run_id)

    def read(self, run_id:str, s3_client) -> Optional[RunPlan]:
        """The plan this run started with, or None if it has not started."""
        key = self.key(run_id)

        # One exact read rather than a listing and a download: a listing matches
        # on prefix, so a neighbouring key would answer for this one, and the
        # object can go between the two calls.
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in MISSING_KEY_CODES:
                return None
            raise

        return RunPlan.from_json(response['Body'].read().decode('UTF-8'))

    def write(self, plan:RunPlan, s3_client):
        """Store a plan, failing if this run already has one."""
        key = self.key(plan.run_id)
        logger.debug(f'Writing run plan to S3 [bucket: {self.bucket_name}, key: {key}]')
        self._put(key, plan.to_json(), 'application/json', s3_client, only_if_absent=True)

    def resolve(self, plan:RunPlan, s3_client=None) -> RunPlan:
        """
        The plan this run obeys: the recorded one if the run has started, the
        one offered if it has not. A recorded plan is never overwritten.

        Raises RunPlanMismatch when a setting that decides the division differs.
        """
        s3_client = s3_client if s3_client is not None else GraphRAGConfig.s3

        recorded = self.read(plan.run_id, s3_client)

        if recorded is None:
            try:
                self.write(plan, s3_client)
                return plan
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') not in ALREADY_WRITTEN_CODES:
                    raise
                # Another run of this id wrote first. Its plan is the one to obey.
                logger.debug(f'A concurrent run wrote this plan first [run_id: {plan.run_id}]')
                recorded = self.read(plan.run_id, s3_client)

        _check_against(recorded, plan)

        return recorded


def _check_against(recorded:RunPlan, current:RunPlan):
    """Whether a restart may continue the run it found, and why not if it may not."""
    for setting in SPLIT_DECIDING:
        if getattr(recorded, setting) != getattr(current, setting):
            raise RunPlanMismatch(
                f'{setting} differs from the plan this run started with '
                f'[recorded: {getattr(recorded, setting)}, now: {getattr(current, setting)}]. '
                f'A changed {setting} is a new run, not a restart of this one.'
            )

    # Counted rather than compared as sets: two documents with the same content
    # share an id, and losing one of them changes the division.
    recorded_ids = Counter(recorded.document_ids)
    current_ids = Counter(current.document_ids)

    gone = sorted((recorded_ids - current_ids).elements())
    added = sorted((current_ids - recorded_ids).elements())

    if gone or added:
        raise RunPlanMismatch(
            f'the collection differs from the plan this run started with '
            f'[no longer present: {gone}, not in the plan: {added}]. '
            f'A changed collection is a new run, not a restart of this one.'
        )

    # Everything else is recorded for diagnosis rather than enforced. A changed
    # model or region does not move a document into a different job.
    unset = object()
    for name in sorted(set(recorded.config) | set(current.config)):
        was = recorded.config.get(name, unset)
        now = current.config.get(name, unset)
        if was != now:
            logger.warning(
                f'Continuing a run under a changed setting that does not decide how the '
                f'collection divides [{name}: recorded '
                f'{"unset" if was is unset else was}, now {"unset" if now is unset else now}]'
            )
