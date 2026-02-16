[[Home](./)]

## Amazon S3 Vectors as a Vector Store

### Topics

  - [Overview](#overview)
  - [Creating an S3 Vectors vector store](#creating-an-s3-vectors-vector-store)

### Overview

You can use [Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) as a vector store.

### Creating an S3 Vectors vector store

Use the `VectorStoreFactory.for_vector_store()` static factory method to create an instance of an Amazon S3 Vectors vector store.

To create an Amazon S3 Vectors store, supply a connection in the following format:

```
s3vectors://<bucket_name>[/<index_prefix>]
```

For example:

```python
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

s3_vectors_connection_info = 's3vectors://my-s3-vectors-bucket/db1'

with VectorStoreFactory.for_vector_store(s3_vectors_connection_info) as vector_store:
    ...
```

### IAM permissions required to use Amazon S3 Vectors as a vector store

The identity under which the graphrag-toolkit runs requires the following IAM permissions:

  - `s3vectors:GetVectorBucket`
  - `s3vectors:CreateVectorBucket`
  - `s3vectors:ListIndexes`
  - `s3vectors:CreateIndex`
  - `s3vectors:GetIndex`
  - `s3vectors:PutVectors`
  - `s3vectors:GetVectors`
  - `s3vectors:ListVectors`
  - `s3vectors:QueryVectors`
  - `s3vectors:DeleteVectors`