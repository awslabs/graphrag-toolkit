[[Home](./)]

## Amazon S3 Vectors as a Vector Store

### Topics

  - [Overview](#overview)
  - [Creating an S3 Vectors vector store](#creating-an-s3-vectors-vector-store)
    - [Connection string parameters](#connection-string-parameters)
  - [IAM permissions required to use Amazon S3 Vectors as a vector store](#iam-permissions-required-to-use-amazon-s3-vectors-as-a-vector-store)
    - [Indexing](#indexing)
    - [Querying](#querying)

### Overview

You can use [Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) as a vector store.

### Creating an S3 Vectors vector store

Use the `VectorStoreFactory.for_vector_store()` static factory method to create an instance of an Amazon S3 Vectors vector store.

To create an Amazon S3 Vectors store, supply a connection string in the following format:

```
s3vectors://<bucket_name>[/<index_prefix>]
```

For example:

```python
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory

s3_vectors_connection_info = 's3vectors://my-s3-vectors-bucket/app1'

with VectorStoreFactory.for_vector_store(s3_vectors_connection_info) as vector_store:
    ...
```

#### Connection string parameters

The connection string includes two parameters:

#####  `bucket_name` 

Mandatory. Name of an Amazon S3 vector bucket in the same AWS Region as the application running the graphrag-toolkit. If the vector bucket does not already exist, the indexing process will create a new bucket.

#####  `index_prefix` 

Optional. Prefix to be attached to the name of each index created by the indexing process. Prefixes allow you to store indexes created by different graphrag-toolkit applications in the same vector bucket.

Imagine an application with two [tenants](./multi-tenancy.md) - the default tenant, and an `admin` tenant - and a connection to an S3 Vectors vector store that uses the following connection string:

```
s3vectors://my-s3-vectors-bucket
```

 Because the vector store connection string is configured with a bucket name only, the application will create the following chunk indexes:

   - `chunk`
   - `chunk-admin`

If the connection string includes a prefix, like this -

```
s3vectors://my-s3-vectors-bucket/app1
```

- the application will create the following chunk indexes:

   - `app1.chunk`
   - `app1.chunk-admin`

### IAM permissions required to use Amazon S3 Vectors as a vector store

#### Indexing

The identity under which the graphrag-toolkit's indexing process runs requires the following IAM permissions:

  - `s3Vectors:GetVectorBucket`
  - `s3Vectors:CreateVectorBucket`
  - `s3Vectors:GetIndex`
  - `s3Vectors:CreateIndex`
  - `s3Vectors:DeleteVectors`
  - `s3Vectors:GetVectors`
  - `s3Vectors:PutVectors`

#### Querying

The identity under which the graphrag-toolkit's querying process runs requires the following IAM permissions:

  - `s3Vectors:GetVectorBucket`
  - `s3Vectors:GetIndex`
  - `s3Vectors:QueryVectors`
  - `s3Vectors:GetVectors`

See [Identity and Access management in S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-access-management.html) for more details on AWS security best practices for S3 Vectors.