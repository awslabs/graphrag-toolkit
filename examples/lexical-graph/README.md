## Lexical Graph Examples

### Notebooks

  - [**00-Setup**](./notebooks/00-Setup.ipynb) – Installs the lexical-graph package and additional dependencies.
  - [**01-Combined Extract and Build**](./notebooks/01-Combined-Extract-and-Build.ipynb) – An example of [performing continuous ingest](../../docs/lexical-graph/indexing.md#continous-ingest) using the `LexicalGraphIndex.extract_and_build()` method.
  - [**02-Separate Extract and Build**](./notebooks/02-Separate-Extract-and-Build.ipynb) – An example of [running the extract and build stages separately](../../docs/lexical-graph/indexing.md#run-the-extract-and-build-stages-separately), with intermediate chunks persisted to the local filesystem using a `FileBasedChunks` object.
  - [**03-Advanced Construction**](./notebooks/03-Advanced-Construction.ipynb) – An example of [advanced graph construction](../../docs/lexical-graph/indexing.md#advanced-graph-construction).
  - [**04-Querying**](./notebooks/04-Querying.ipynb) – Examples of [querying the graph](../../docs/lexical-graph/querying.md) using the `LexicalGraphQueryEngine` with either the `TraversalBasedRetriever` or `SemanticGuidedRetriever`.
  
#### Environment variables

The notebooks assume that the [graph store and vector store connections](../../docs/lexical-graph/storage-model.md) are stored in `GRAPH_STORE` and `VECTOR_STORE` environment variables. 

If you are running these notebooks via the Cloudformation template below, a `.env` file containing these variables will already have been installed in the Amazon SageMaker environment. If you are running these notebooks in a separate environment, you will need to populate these two environment variables.

### Cloudformation templates

 - [`graphrag-toolkit-neptune-db-opensearch-serverless.json`](./cloudformation-templates/graphrag-toolkit-neptune-db-opensearch-serverless.json) creates the following lexical-graph environment:
   - Amazon VPC with three private subnets, one public subnet, and an internet gateway
   - Amazon Neptune Database cluster with a single Neptune serverless instance
   - Amazon OpenSearch Serverless collection with a public endpoint
   - Amazon SageMaker notebook
 - [`graphrag-toolkit-neptune-db-aurora-postgres.json`](./cloudformation-templates/graphrag-toolkit-neptune-db-aurora-postgres.json) creates the following lexical-graph environment:
   - Amazon VPC with three private subnets, one public subnet, and an internet gateway
   - Amazon Neptune Database cluster with a single Neptune serverless instance
   - Amazon Aurora Postgres Database cluster with a single serverless instance
   - Amazon SageMaker notebook
 
Charges apply.

#### Amazon Bedrock foundation model access

The SageMaker notebook's IAM role policy includes permissions that allow the following models to be invoked:

- `anthropic.claude-3-5-sonnet-20240620-v1:0` via the `us.anthropic.claude-3-5-sonnet-20240620-v1:0` [inference profile](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles.html).
- `cohere.embed-english-v3`

You must run the CloudFormation stack in a region containing these models, and must [enable access](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) to these models before running the notebook examples.

#### Adding additional IAM permissions

The CloudFormation stack includes an input parameter, `IamPolicyArn`, that allows you to add an additional IAM policy to the GraphRAG client IAM role created by the stack. Use this parameter to add a custom policy containing permissions to additional resources that you want to use, such as specific Amazon S3 buckets, or additional Amazon Bedrock foundation models.

#### Installing example notebooks

The CloudFormation stack includes an input parameter, `ExampleNotebooksURL` that specifies the URL of a zip file containing the lexical-graph example notebooks. By default this parameter is set to:

```
https://github.com/awslabs/graphrag-toolkit/releases/latest/download/lexical-graph-examples.zip
```

Set this parameter blank if you do not want to install the notebooks in your environment.
