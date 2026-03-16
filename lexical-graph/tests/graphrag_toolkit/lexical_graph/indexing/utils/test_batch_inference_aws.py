# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for AWS-dependent batch inference utilities.

Covers:
  - create_and_run_batch_job (success, with VPC config, ClientError)
  - wait_for_job_completion (Completed, Failed, Stopped)
  - download_output_files (finds folder, no folder found)
"""

import os
import pytest
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph import BatchJobError
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig


def _make_batch_config(**kwargs):
    defaults = dict(
        bucket_name="my-bucket",
        role_arn="arn:aws:iam::123456789012:role/MyRole",
        region="us-east-1",
        max_batch_size=1000,
        max_num_concurrent_batches=2,
        delete_on_success=False,
        subnet_ids=None,
        security_group_ids=None,
        s3_encryption_key_id=None,
        key_prefix=None,
    )
    defaults.update(kwargs)
    return BatchConfig(**defaults)


# ---------------------------------------------------------------------------
# create_and_run_batch_job
# ---------------------------------------------------------------------------

class TestCreateAndRunBatchJob:

    def _make_bedrock_client(self, job_arn="arn:aws:bedrock:us-east-1::job/abc"):
        client = MagicMock()
        client.create_model_invocation_job.return_value = {"jobArn": job_arn}
        return client

    def test_success_without_vpc(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import create_and_run_batch_job

        bedrock = self._make_bedrock_client()
        config = _make_batch_config()

        with patch(
            "graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.wait_for_job_completion"
        ) as mock_wait:
            create_and_run_batch_job(
                "extract-topics", bedrock, "20240101", "abc12",
                config, "inputs/file.jsonl", "outputs/", "anthropic.claude-v3"
            )
            mock_wait.assert_called_once()
            bedrock.create_model_invocation_job.assert_called_once()

    def test_success_with_vpc(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import create_and_run_batch_job

        bedrock = self._make_bedrock_client()
        config = _make_batch_config(
            subnet_ids=["subnet-abc"],
            security_group_ids=["sg-xyz"]
        )

        with patch(
            "graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.wait_for_job_completion"
        ):
            create_and_run_batch_job(
                "extract-topics", bedrock, "20240101", "abc12",
                config, "inputs/file.jsonl", "outputs/", "anthropic.claude-v3"
            )
            call_kwargs = bedrock.create_model_invocation_job.call_args[1]
            assert "vpcConfig" in call_kwargs

    def test_client_error_raises_batch_job_error(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import create_and_run_batch_job

        bedrock = MagicMock()
        bedrock.create_model_invocation_job.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "bad input"}},
            "CreateModelInvocationJob"
        )
        config = _make_batch_config()

        with pytest.raises(BatchJobError):
            create_and_run_batch_job(
                "extract-topics", bedrock, "20240101", "abc12",
                config, "inputs/file.jsonl", "outputs/", "anthropic.claude-v3"
            )


# ---------------------------------------------------------------------------
# wait_for_job_completion
# ---------------------------------------------------------------------------

class TestWaitForJobCompletion:

    def test_completes_successfully(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import wait_for_job_completion

        bedrock = MagicMock()
        bedrock.get_model_invocation_job.return_value = {"status": "Completed"}

        with patch("graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.time.sleep"):
            wait_for_job_completion(bedrock, "arn:job/abc", "file.jsonl")

        bedrock.get_model_invocation_job.assert_called_once_with(jobIdentifier="arn:job/abc")

    def test_failed_status_raises_batch_job_error(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import wait_for_job_completion

        bedrock = MagicMock()
        bedrock.get_model_invocation_job.return_value = {
            "status": "Failed",
            "message": "Something went wrong"
        }

        with patch("graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.time.sleep"):
            with pytest.raises(BatchJobError, match="Batch job failed"):
                wait_for_job_completion(bedrock, "arn:job/abc", "file.jsonl")

    def test_stopped_status_raises_batch_job_error(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import wait_for_job_completion

        bedrock = MagicMock()
        bedrock.get_model_invocation_job.return_value = {
            "status": "Stopped",
            "message": "Stopped by user"
        }

        with patch("graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.time.sleep"):
            with pytest.raises(BatchJobError):
                wait_for_job_completion(bedrock, "arn:job/abc", "file.jsonl")

    def test_polls_until_terminal_status(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import wait_for_job_completion

        bedrock = MagicMock()
        bedrock.get_model_invocation_job.side_effect = [
            {"status": "InProgress"},
            {"status": "InProgress"},
            {"status": "Completed"},
        ]

        with patch("graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.time.sleep"):
            wait_for_job_completion(bedrock, "arn:job/abc", "file.jsonl")

        assert bedrock.get_model_invocation_job.call_count == 3


# ---------------------------------------------------------------------------
# download_output_files
# ---------------------------------------------------------------------------

class TestDownloadOutputFiles:

    def test_finds_folder_and_downloads(self, tmp_path):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import download_output_files

        s3 = MagicMock()
        paginator = MagicMock()
        s3.get_paginator.return_value = paginator

        # Simulate paginator returning one page with one object whose key starts with input_filename
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "outputs/folder/my-input.jsonl.out"}]}
        ]

        s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "outputs/folder/my-input.jsonl.out"}]
        }

        download_output_files(s3, "my-bucket", "outputs/", "my-input.jsonl", str(tmp_path))

        s3.download_file.assert_called_once()

    def test_no_folder_found_raises_batch_job_error(self):
        from graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils import download_output_files
        from tenacity import RetryError

        s3 = MagicMock()
        paginator = MagicMock()
        s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"Contents": [{"Key": "outputs/other-file.txt"}]}]

        with patch("graphrag_toolkit.lexical_graph.indexing.utils.batch_inference_utils.time.sleep"):
            with pytest.raises((BatchJobError, RetryError)):
                download_output_files(s3, "my-bucket", "outputs/", "my-input.jsonl", "/tmp/out")
