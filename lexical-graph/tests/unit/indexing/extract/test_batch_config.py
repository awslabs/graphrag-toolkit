# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig


class TestBatchConfigInitialization:
    """Tests for BatchConfig initialization."""
    
    def test_initialization_with_required_fields(self):
        """Verify BatchConfig initializes with required fields only."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket"
        )
        
        assert config.role_arn == "arn:aws:iam::123456789012:role/test-role"
        assert config.region == "us-east-1"
        assert config.bucket_name == "test-bucket"
        assert config.key_prefix is None
        assert config.s3_encryption_key_id is None
        assert config.subnet_ids == []
        assert config.security_group_ids == []
        assert config.max_batch_size == 25000
        assert config.max_num_concurrent_batches == 3
        assert config.delete_on_success is True
    
    def test_initialization_with_all_fields(self):
        """Verify BatchConfig initializes with all fields."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-west-2",
            bucket_name="my-bucket",
            key_prefix="data/batch/",
            s3_encryption_key_id="arn:aws:kms:us-west-2:123456789012:key/12345678",
            subnet_ids=["subnet-123", "subnet-456"],
            security_group_ids=["sg-123", "sg-456"],
            max_batch_size=10000,
            max_num_concurrent_batches=5,
            delete_on_success=False
        )
        
        assert config.role_arn == "arn:aws:iam::123456789012:role/test-role"
        assert config.region == "us-west-2"
        assert config.bucket_name == "my-bucket"
        assert config.key_prefix == "data/batch/"
        assert config.s3_encryption_key_id == "arn:aws:kms:us-west-2:123456789012:key/12345678"
        assert config.subnet_ids == ["subnet-123", "subnet-456"]
        assert config.security_group_ids == ["sg-123", "sg-456"]
        assert config.max_batch_size == 10000
        assert config.max_num_concurrent_batches == 5
        assert config.delete_on_success is False
    
    def test_initialization_with_custom_batch_settings(self):
        """Verify BatchConfig accepts custom batch size and concurrency settings."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="eu-west-1",
            bucket_name="test-bucket",
            max_batch_size=50000,
            max_num_concurrent_batches=10
        )
        
        assert config.max_batch_size == 50000
        assert config.max_num_concurrent_batches == 10


class TestBatchConfigDefaults:
    """Tests for BatchConfig default values."""
    
    def test_default_max_batch_size(self):
        """Verify default max_batch_size is 25000."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket"
        )
        
        assert config.max_batch_size == 25000
    
    def test_default_max_num_concurrent_batches(self):
        """Verify default max_num_concurrent_batches is 3."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket"
        )
        
        assert config.max_num_concurrent_batches == 3
    
    def test_default_delete_on_success(self):
        """Verify default delete_on_success is True."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket"
        )
        
        assert config.delete_on_success is True
    
    def test_default_optional_fields_are_none_or_empty(self):
        """Verify optional fields default to None or empty lists."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket"
        )
        
        assert config.key_prefix is None
        assert config.s3_encryption_key_id is None
        assert config.subnet_ids == []
        assert config.security_group_ids == []


class TestBatchConfigNetworkSettings:
    """Tests for BatchConfig network configuration."""
    
    def test_subnet_ids_single(self):
        """Verify subnet_ids accepts single subnet."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket",
            subnet_ids=["subnet-123"]
        )
        
        assert config.subnet_ids == ["subnet-123"]
    
    def test_subnet_ids_multiple(self):
        """Verify subnet_ids accepts multiple subnets."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket",
            subnet_ids=["subnet-123", "subnet-456", "subnet-789"]
        )
        
        assert len(config.subnet_ids) == 3
        assert "subnet-123" in config.subnet_ids
        assert "subnet-456" in config.subnet_ids
        assert "subnet-789" in config.subnet_ids
    
    def test_security_group_ids_single(self):
        """Verify security_group_ids accepts single security group."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket",
            security_group_ids=["sg-123"]
        )
        
        assert config.security_group_ids == ["sg-123"]
    
    def test_security_group_ids_multiple(self):
        """Verify security_group_ids accepts multiple security groups."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket",
            security_group_ids=["sg-123", "sg-456"]
        )
        
        assert len(config.security_group_ids) == 2
        assert "sg-123" in config.security_group_ids
        assert "sg-456" in config.security_group_ids


class TestBatchConfigS3Settings:
    """Tests for BatchConfig S3 configuration."""
    
    def test_s3_bucket_name(self):
        """Verify bucket_name is stored correctly."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="my-special-bucket"
        )
        
        assert config.bucket_name == "my-special-bucket"
    
    def test_s3_key_prefix(self):
        """Verify key_prefix is stored correctly."""
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket",
            key_prefix="batch/processing/"
        )
        
        assert config.key_prefix == "batch/processing/"
    
    def test_s3_encryption_key_id(self):
        """Verify s3_encryption_key_id is stored correctly."""
        kms_key = "arn:aws:kms:us-east-1:123456789012:key/abcd1234"
        config = BatchConfig(
            role_arn="arn:aws:iam::123456789012:role/test-role",
            region="us-east-1",
            bucket_name="test-bucket",
            s3_encryption_key_id=kms_key
        )
        
        assert config.s3_encryption_key_id == kms_key
