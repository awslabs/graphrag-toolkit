# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from os.path import join
from typing import Optional

from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import EncryptedPut, RUN_ARTIFACT_DIR
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import MISSING_KEY_CODES
from graphrag_toolkit.lexical_graph.utils.id_validation import validate_id_segment

logger = logging.getLogger(__name__)


class RunRecordError(Exception):
    """A record a run wrote that cannot be read back as one."""


class RunArtifactStore(EncryptedPut):
    """
    Where a run keeps what it records about itself, beside the collection it
    works on.

    A run's plan and its partition records are written under the same run
    directory, so one set of coordinates answers for both and they cannot
    disagree about which collection or which run they mean.
    """

    def __init__(self,
                 bucket_name:str,
                 key_prefix:str,
                 collection_id:str,
                 s3_encryption_key_id:Optional[str]=None):
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.collection_id = collection_id
        self.s3_encryption_key_id = s3_encryption_key_id

    def run_path(self, run_id:str) -> str:
        """Where this run's artifacts sit. A run id that climbs out is refused."""
        validate_id_segment(run_id, 'run_id')
        return join(self.key_prefix, self.collection_id, RUN_ARTIFACT_DIR, run_id)

    def _read_json(self, key:str, s3_client) -> Optional[str]:
        """
        The body of one object, or None where there is no such object.

        One exact read rather than a listing and a download: a listing matches
        on prefix, so a neighbouring key would answer for this one, and the
        object can go between the two calls.
        """
        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in MISSING_KEY_CODES:
                return None
            raise

        return response['Body'].read().decode('UTF-8')
