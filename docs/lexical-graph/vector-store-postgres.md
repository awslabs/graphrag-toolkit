[[Home](./)]

## Postgres as a Vector Store

### Topics

  - [Overview](#overview)
  - [Install dependencies](#install-dependencies)
  - [Creating Postgres vector store](#creating-a-postgres-vector-store)
    - [Connecting to an IAM auth-enabled Postgres vector store](#connecting-to-an-iam-auth-enabled-postgres-vector-store)

### Overview

You can use a Postgres database with the [pgvector](https://github.com/pgvector/pgvector) extension as a vector store.

### Install dependencies

The Postgres vector store requires both the `psycopg2` and `pgvector` packages:

```
pip install psycopg2-binary pgvector
```

### Creating a Postgres vector store

Use the `VectorStoreFactory.for_vector_store()` static factory method to create an instance of a Postgres vector store.

To create a Postgres vector store, supply a connection string in the following format:

```
postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
```

For example:

```
postgresql://graphrag:!zfg%dGGh@mydbcluster.cluster-123456789012.us-west-2.rds.amazonaws.com:5432/postgres
```

#### Connecting to an IAM auth-enabled Postgres vector store

If your Postgres database supports [AWS Identity and Access Management (IAM) database authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html), omit the password, and add `enable_iam_db_auth=True` to the connection string query parameters:

```
postgresql://graphrag@mydbcluster.cluster-123456789012.us-west-2.rds.amazonaws.com:5432/postgres?enable_iam_db_auth=True
```

You will need to create a database user, and [grant the `rds_iam` role](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html#UsingWithRDS.IAMDBAuth.DBAccounts.PostgreSQL) to use IAM authentication. 


