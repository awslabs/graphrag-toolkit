# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
import logging
import mimetypes

import boto3

from benchmarks.utils.dataset_config import get_data_subdir

logger = logging.getLogger(__name__)


def sync_benchmark_data_from_s3(dataset: str, data_dir: str):
    """
    If BENCHMARK_DATA_S3_URI is set and the local dataset directory doesn't exist,
    sync the dataset from S3.

    PGA splits (pga_bio, pga_stat) share a single 'pga' directory in S3 and locally,
    so the mapping is applied here to stay consistent with load_qa_pairs().
    """
    s3_uri = os.environ.get('BENCHMARK_DATA_S3_URI')
    if not s3_uri:
        return

    sync_dataset = get_data_subdir(dataset)

    local_dataset_dir = os.path.join(data_dir, sync_dataset)
    if os.path.exists(local_dataset_dir):
        logger.info(f'Dataset directory already exists: {local_dataset_dir}')
        return

    s3_dataset_uri = s3_uri.rstrip('/') + '/' + sync_dataset + '/'
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


def _content_type_for_file(filepath: str) -> str:
    """Return the appropriate Content-Type for a benchmark result file."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext in ('.json', '.jsonl'):
        return 'application/json'
    # Fall back to mimetypes guess, defaulting to text/plain
    guessed, _ = mimetypes.guess_type(filepath)
    return guessed if guessed else 'text/plain'


def upload_benchmark_results_to_s3(
    local_dir: str,
    s3_sub_path: str,
) -> None:
    """
    Upload benchmark results from a local directory to S3.

    Reads S3 destination from environment variables:
        S3_RESULTS_BUCKET: Target S3 bucket name
        S3_RESULTS_PREFIX: Key prefix within the bucket

    Uploads all files under ``local_dir`` to
    ``s3://<bucket>/<prefix>/benchmark-results/<s3_sub_path>/``.

    If S3_RESULTS_BUCKET is not set, logs a warning and returns without error.
    If upload fails (credentials, permissions, network), logs a warning and
    returns without raising — an upload failure must not take down a passing run.

    Args:
        local_dir: Absolute or relative path to the directory to upload
            (e.g. the results_dir already computed by the caller).
        s3_sub_path: The sub-path under ``benchmark-results/`` in the S3 key
            (e.g. 'cuad/traversal' or 'pga_bio/topic_beam_search').
    """
    bucket = os.environ.get('S3_RESULTS_BUCKET')
    if not bucket:
        logger.warning(
            'S3_RESULTS_BUCKET not set — skipping benchmark results upload to S3'
        )
        return

    prefix = os.environ.get('S3_RESULTS_PREFIX', '').strip('/')

    if not os.path.isdir(local_dir):
        logger.warning(f'Results directory does not exist: {local_dir} — nothing to upload')
        return

    s3_key_base = '/'.join(
        part for part in [prefix, 'benchmark-results', s3_sub_path] if part
    )

    try:
        client = boto3.client('s3')
        uploaded_count = 0

        for dirpath, _dirnames, filenames in os.walk(local_dir):
            for filename in filenames:
                local_path = os.path.join(dirpath, filename)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_key = f'{s3_key_base}/{relative_path}'
                content_type = _content_type_for_file(local_path)

                logger.info(f'Uploading {local_path} -> s3://{bucket}/{s3_key}')
                client.upload_file(
                    local_path,
                    bucket,
                    s3_key,
                    ExtraArgs={
                        'ServerSideEncryption': 'AES256',
                        'ContentType': content_type,
                    },
                )
                uploaded_count += 1

        s3_uri = f's3://{bucket}/{s3_key_base}/'
        logger.info(
            f'Upload complete: {uploaded_count} file(s) uploaded to {s3_uri}'
        )
    except Exception:
        logger.warning(
            f'Failed to upload benchmark results to S3 — continuing without upload',
            exc_info=True,
        )


def upload_all_benchmark_results_to_s3(results_dir: str = 'benchmark-results') -> None:
    """
    Upload all benchmark results across all datasets and retrievers to S3.

    Walks the ``<results_dir>/`` directory expecting the structure
    ``<results_dir>/<dataset>/<retriever_id>/`` and uploads each retriever's
    results via :func:`upload_benchmark_results_to_s3`.

    Useful for bulk upload at the end of run_all_retrievers.sh where we want
    to persist everything in one pass.

    Args:
        results_dir: Root directory containing benchmark results. Defaults to
            'benchmark-results'.
    """
    if not os.path.isdir(results_dir):
        logger.warning(f'Results directory does not exist: {results_dir} — nothing to upload')
        return

    for dataset in sorted(os.listdir(results_dir)):
        dataset_path = os.path.join(results_dir, dataset)
        if not os.path.isdir(dataset_path):
            continue
        for retriever_id in sorted(os.listdir(dataset_path)):
            retriever_path = os.path.join(dataset_path, retriever_id)
            if not os.path.isdir(retriever_path):
                continue
            logger.info(f'Uploading results for dataset={dataset}, retriever={retriever_id}')
            upload_benchmark_results_to_s3(
                local_dir=retriever_path,
                s3_sub_path=f'{dataset}/{retriever_id}',
            )
