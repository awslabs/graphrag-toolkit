# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class BatchConfig:
    """
    Configuration for batch processing settings.

    This class provides a structure for configuring batch processing, including
    AWS settings like role ARN, region, and S3 bucket details, as well as network
    and batch control parameters. It is designed to facilitate batch operations
    by defining a standardized schema for batch-related configurations.

    Attributes:
        role_arn (str): ARN of the IAM role used for batch processing.
        region (str): AWS region where the batch processing will take place.
        bucket_name (str): Name of the S3 bucket used for storing batch-related
            data.
        key_prefix (Optional[str]): Optional prefix for keys in the S3 bucket.
        s3_encryption_key_id (Optional[str]): KMS key ID used for S3 encryption,
            if any.
        subnet_ids (List[str]): List of subnet IDs used for the network
            configuration of the batch processing.
        security_group_ids (List[str]): List of security group IDs applied to the
            batch processing tasks.
        max_batch_size (int): Maximum size of a single batch. Default is 25000.
        max_num_concurrent_batches (int): Maximum number of concurrent batches
            allowed. Default is 3.
    """
    role_arn:str
    region:str
    bucket_name:str
    key_prefix:Optional[str]=None
    s3_encryption_key_id:Optional[str]=None
    subnet_ids:List[str] = field(default_factory=list)
    security_group_ids:List[str] = field(default_factory=list)
    max_batch_size:int=25000
    max_num_concurrent_batches:int=3
    delete_on_success=True