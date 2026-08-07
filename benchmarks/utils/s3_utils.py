# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
import logging

logger = logging.getLogger(__name__)


def sync_benchmark_data_from_s3(dataset: str, data_dir: str):
    """
    If BENCHMARK_DATA_S3_URI is set and the local dataset directory doesn't exist,
    sync the dataset from S3.
    """
    s3_uri = os.environ.get('BENCHMARK_DATA_S3_URI')
    if not s3_uri:
        return

    local_dataset_dir = os.path.join(data_dir, dataset)
    if os.path.exists(local_dataset_dir):
        logger.info(f'Dataset directory already exists: {local_dataset_dir}')
        return

    s3_dataset_uri = s3_uri.rstrip('/') + '/' + dataset + '/'
    logger.info(f'Syncing benchmark data from {s3_dataset_uri} to {local_dataset_dir}')
    os.makedirs(local_dataset_dir, exist_ok=True)
    try:
        subprocess.run(
            ['aws', 's3', 'sync', s3_dataset_uri, local_dataset_dir],
            check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as e:
        logger.error(f'S3 sync failed for {s3_dataset_uri}: {e.stderr}')
        raise
    except FileNotFoundError:
        raise RuntimeError(
            'AWS CLI not found. Install it or unset BENCHMARK_DATA_S3_URI.'
        )
    logger.info(f'Sync complete: {local_dataset_dir}')
