# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, List

import boto3
from botocore.exceptions import ClientError

from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import DoNotCheckpoint
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY

from llama_index.core.schema import TransformComponent, BaseNode

SAVEPOINT_ROOT_DIR = 'save_points'

logger = logging.getLogger(__name__)


class S3CheckpointFilter(TransformComponent, DoNotCheckpoint):
    """Filters nodes based on the absence of an S3 checkpoint marker.

    S3-backed equivalent of CheckpointFilter. Uses HEAD requests on zero-byte
    S3 objects instead of os.path.exists() on local files.

    Attributes:
        checkpoint_name (str): The name of the checkpoint used for filtering.
        bucket_name (str): S3 bucket where checkpoint markers are stored.
        key_prefix (str): S3 key prefix for checkpoint markers.
        inner (TransformComponent): The wrapped TransformComponent for processing nodes.
        tenant_id (TenantId): Tenant ID for multi-tenancy support.
    """
    checkpoint_name: str
    bucket_name: str
    key_prefix: str
    inner: TransformComponent
    tenant_id: TenantId

    class Config:
        arbitrary_types_allowed = True

    def _get_s3_client(self):
        from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        return GraphRAGConfig.s3

    def _marker_key(self, node_id: str) -> str:
        return f"{self.key_prefix}/{SAVEPOINT_ROOT_DIR}/{self.checkpoint_name}/{node_id}"

    def checkpoint_does_not_exist(self, node_id: str) -> bool:
        """Check whether a checkpoint marker exists in S3 for the given node.

        Args:
            node_id: Identifier of the node to check.

        Returns:
            bool: True if no checkpoint exists (node should be processed),
                  False if checkpoint exists (node should be skipped).
        """
        tenant_node_id = self.tenant_id.rewrite_id(node_id)
        key = self._marker_key(tenant_node_id)

        try:
            s3_client = self._get_s3_client()
            s3_client.head_object(Bucket=self.bucket_name, Key=key)
            logger.debug(
                f'Ignoring node because checkpoint already exists '
                f'[node_id: {tenant_node_id}, checkpoint: {self.checkpoint_name}, '
                f'component: {type(self.inner).__name__}]'
            )
            return False
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
                logger.debug(
                    f'Including node '
                    f'[node_id: {tenant_node_id}, checkpoint: {self.checkpoint_name}, '
                    f'component: {type(self.inner).__name__}]'
                )
                return True
            # Unexpected error — include node (safe default: re-process rather than skip)
            logger.warning(
                f'Checkpoint check failed for node {tenant_node_id}: {e}. '
                f'Including node (safe default).'
            )
            return True

    def __call__(self, nodes: List[BaseNode], **kwargs: Any) -> List[BaseNode]:
        """Filter nodes that already have S3 checkpoint markers, then process the rest.

        Args:
            nodes: A list of BaseNode objects to be filtered.
            **kwargs: Additional keyword arguments passed to the inner callable.

        Returns:
            A list of BaseNode objects filtered and processed by the inner callable.
        """
        discarded_count = 0
        filtered_nodes = []

        for node in nodes:
            if self.checkpoint_does_not_exist(node.id_):
                filtered_nodes.append(node)
            else:
                discarded_count += 1

        if discarded_count > 0:
            logger.info(
                f'[{type(self.inner).__name__}] Discarded {discarded_count} out of '
                f'{discarded_count + len(filtered_nodes)} nodes because they have '
                f'already been checkpointed (S3)'
            )

        return self.inner.__call__(filtered_nodes, **kwargs)


class S3CheckpointWriter(NodeHandler):
    """Writes S3 checkpoint markers for processed nodes.

    S3-backed equivalent of CheckpointWriter. Uses PUT zero-byte objects
    instead of touching local files.

    Attributes:
        checkpoint_name (str): The name of the checkpoint.
        bucket_name (str): S3 bucket where checkpoint markers are stored.
        key_prefix (str): S3 key prefix for checkpoint markers.
        inner (NodeHandler): The wrapped NodeHandler for processing nodes.
    """
    checkpoint_name: str
    bucket_name: str
    key_prefix: str
    inner: NodeHandler

    def _get_s3_client(self):
        from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
        return GraphRAGConfig.s3

    def _marker_key(self, node_id: str) -> str:
        return f"{self.key_prefix}/{SAVEPOINT_ROOT_DIR}/{self.checkpoint_name}/{node_id}"

    def touch(self, node_id: str):
        """Write a zero-byte S3 object as a checkpoint marker.

        Args:
            node_id: The node identifier to use as the marker key.
        """
        key = self._marker_key(node_id)
        try:
            s3_client = self._get_s3_client()
            s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=b'')
            logger.debug(
                f'Checkpoint marker written '
                f'[checkpoint: {self.checkpoint_name}, node_id: {node_id}, '
                f's3://{self.bucket_name}/{key}]'
            )
        except Exception as e:
            # Non-fatal: worst case is duplicate work on retry
            logger.warning(
                f'Failed to write checkpoint marker for {node_id}: {e}. '
                f'Node may be re-processed on retry.'
            )

    def accept(self, nodes: List[BaseNode], **kwargs: Any):
        """Process nodes via inner handler and write S3 checkpoint markers.

        Args:
            nodes: A list of nodes to be processed.
            **kwargs: Additional keyword arguments passed to the inner accept method.

        Yields:
            BaseNode: Nodes that have been processed by the inner handler.
        """
        for node in self.inner.accept(nodes, **kwargs):
            node_id = node.node_id
            if [key for key in [INDEX_KEY] if key in node.metadata]:
                logger.debug(
                    f'Non-checkpointable node '
                    f'[checkpoint: {self.checkpoint_name}, node_id: {node_id}, '
                    f'component: {type(self.inner).__name__}]'
                )
            else:
                logger.debug(
                    f'Checkpointable node '
                    f'[checkpoint: {self.checkpoint_name}, node_id: {node_id}, '
                    f'component: {type(self.inner).__name__}]'
                )
                self.touch(node_id)
            yield node


class S3Checkpoint:
    """S3-backed checkpoint for data processing components.

    Drop-in replacement for Checkpoint that stores markers as zero-byte S3 objects
    instead of local files. Enables checkpointing in serverless/containerized
    environments (ECS Fargate, Lambda, EKS) where local disk is ephemeral.

    Marker path: s3://{bucket_name}/{key_prefix}/save_points/{checkpoint_name}/{node_id}

    Usage:
        checkpoint = S3Checkpoint(
            checkpoint_name='enrichment-run-001',
            bucket_name='my-pipeline-bucket',
            key_prefix='checkpoints/tenant-a'
        )
        graph_index.extract(docs, handler=extracted_docs, checkpoint=checkpoint)

    Attributes:
        checkpoint_name (str): The name of the checkpoint.
        bucket_name (str): S3 bucket for storing checkpoint markers.
        key_prefix (str): S3 key prefix (no trailing slash).
        enabled (bool): Whether checkpointing is active.
    """

    def __init__(self, checkpoint_name: str, bucket_name: str, key_prefix: str = '',
                 region: str = None, enabled: bool = True):
        """Initialize an S3-backed checkpoint.

        Args:
            checkpoint_name: Name of the checkpoint (used in S3 key path).
            bucket_name: S3 bucket where checkpoint markers will be stored.
            key_prefix: Optional S3 key prefix (e.g. 'checkpoints/tenant-a').
                        No trailing slash needed.
            region: AWS region for the S3 bucket. If None, uses default from
                    GraphRAGConfig or environment.
            enabled: Whether checkpointing is active. When False, add_filter
                     and add_writer return the original objects unwrapped.
        """
        self.checkpoint_name = checkpoint_name
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix.rstrip('/') if key_prefix else ''
        self.region = region
        self.enabled = enabled

        if self.enabled:
            logger.info(
                f'S3Checkpoint initialized '
                f'[checkpoint: {checkpoint_name}, '
                f'location: s3://{bucket_name}/{self.key_prefix}/{SAVEPOINT_ROOT_DIR}/{checkpoint_name}/]'
            )
        else:
            logger.debug(
                f'S3Checkpoint disabled [checkpoint: {checkpoint_name}]'
            )

    def add_filter(self, o, tenant_id: TenantId):
        """Wrap a TransformComponent with an S3 checkpoint filter.

        Only wraps if enabled, the object is a TransformComponent, and it's not
        marked as DoNotCheckpoint.

        Args:
            o: The TransformComponent to potentially wrap.
            tenant_id: Tenant ID for multi-tenancy node ID rewriting.

        Returns:
            S3CheckpointFilter wrapping the input, or the original object.
        """
        if self.enabled and isinstance(o, TransformComponent) and not isinstance(o, DoNotCheckpoint):
            logger.debug(
                f'Wrapping with S3 checkpoint filter '
                f'[checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]'
            )
            return S3CheckpointFilter(
                inner=o,
                bucket_name=self.bucket_name,
                key_prefix=self.key_prefix,
                checkpoint_name=self.checkpoint_name,
                tenant_id=tenant_id,
            )
        else:
            logger.debug(
                f'Not wrapping with S3 checkpoint filter '
                f'[checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]'
            )
            return o

    def add_writer(self, o):
        """Wrap a NodeHandler with an S3 checkpoint writer.

        Only wraps if enabled and the object is a NodeHandler.

        Args:
            o: The NodeHandler to potentially wrap.

        Returns:
            S3CheckpointWriter wrapping the input, or the original object.
        """
        if self.enabled and isinstance(o, NodeHandler):
            logger.debug(
                f'Wrapping with S3 checkpoint writer '
                f'[checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]'
            )
            return S3CheckpointWriter(
                inner=o,
                bucket_name=self.bucket_name,
                key_prefix=self.key_prefix,
                checkpoint_name=self.checkpoint_name,
            )
        else:
            logger.debug(
                f'Not wrapping with S3 checkpoint writer '
                f'[checkpoint: {self.checkpoint_name}, component: {type(o).__name__}]'
            )
            return o
