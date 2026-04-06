# graphrag-toolkit Integration Tests

This project contains integration tests for the [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit). The tests work by spinning up one of the graphrag-toolkit [quickstart environments](https://github.com/awslabs/graphrag-toolkit/tree/main/examples/lexical-graph/cloudformation-templates) using a CloudFormation template, installing the [lexical-graph source code](https://github.com/awslabs/graphrag-toolkit/tree/main/lexical-graph/src) and the test scripts on a SageMaker notebook, running the tests, and publishing the results to S3.

## Prerequisites

- An AWS account with permissions to create CloudFormation stacks, SageMaker notebooks, and related resources
- AWS CLI configured with appropriate credentials
- A clone of the [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit) repository on your local machine
- An Amazon S3 bucket in the AWS Region where you intend to run the tests

## Getting Started

1. Clone this repository to your local machine
2. Clone the [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit) repository to a separate location on your local machine
3. Create an Amazon S3 bucket in the AWS Region where you intend to run the tests
4. (Optional) Create an Amazon SNS topic and email subscription in the same AWS Region for test result notifications
5. Create a `.env` file in the project root based on `env.template`
6. Trigger the tests:

   ```bash
   sh build-tests.sh --test-file lexical.short
   ```

7. Run `sh build-tests.sh --help` to see all available test parameters

## How It Works

The `build-tests.sh` script runs a CloudFormation stack that creates a graphrag-toolkit environment and deploys and runs a suite of integration tests.

The tests start running automatically as soon as the stack completes.

The **Outputs** tab of the base stack includes two links:

- **SagemakerNotebook** — The SageMaker notebook where the tests are running. Click this link to open the notebook. All test artifacts are in the `graphrag-toolkit` directory.
- **TestResults** — Opens the S3 console so that you can review the test results. As each test completes, its results are written to S3.

On the notebook itself you can inspect the `test-results` and `test-logs` directories, and the `screenlog` from the screen session in which the tests are running.

If you open a terminal, you can connect to the screen session:

```bash
screen -r
```

## Test Suites

Test names are in the form `<module>.<ClassName>`, based on files in the `test-scripts/graphrag_toolkit_tests` directory. For example: `extract.ExtractToFileSystem`.

Tests can be supplied via a file (using the `--test-file` option), or as a quoted, space-separated list of test names (using the `--test` option).

### Available Test Suites

| File | Description |
|------|-------------|
| `lexical.short` | Short-running lexical graph tests (extraction, build, query, etc.) |
| `lexical.long` | Long-running lexical graph tests (batch extraction, batch build, multi-hop queries) |
| `lexical.versioning` | Versioning-related tests |
| `byokg.all` | Bring-your-own-knowledge-graph tests |

### Examples

Run all short-running lexical graph tests in the default environment (Neptune Database + OpenSearch Serverless):

```bash
sh build-tests.sh --test-file lexical.short
```

Run short-running tests in a Neptune Database + Aurora PostgreSQL environment:

```bash
sh build-tests.sh --test-file lexical.short --env-type neptune-db-postgresql
```

Run all BYOKG tests with Neptune Analytics:

```bash
sh build-tests.sh --test-file byokg.all --env-type neptune-graph
```

Run a mix of lexical and BYOKG tests in a Neptune Analytics environment:

```bash
sh build-tests.sh --env-type neptune-graph --test 'extract.ExtractToFileSystem byokg_setup.LoadBYOKGGraph'
```

## Running Tests from the SageMaker Notebook

You can run tests directly from the SageMaker notebook. Use the **SagemakerNotebook** link from the CloudFormation **Outputs** tab to open the notebook. Then, from the **New** dropdown in Jupyter, open a terminal:

```bash
cd ~/SageMaker/graphrag-toolkit/
source ./.env
source ./.env.testing
sh run_test_suite.sh --test extract_and_build.ExtractAndBuild query.TraversalBasedQuery
```

The `--test` parameter accepts a space-separated list of test names. You can also supply a `--skip-setup` flag to skip reinstalling dependencies after the first run.

## Supported Environment Types

| Environment Type | Graph Store | Vector Store |
|-----------------|-------------|--------------|
| `neptune-db-aoss` (default) | Neptune Database | OpenSearch Serverless |
| `neptune-db-postgresql` | Neptune Database | Aurora PostgreSQL |
| `neptune-db-s3vectors` | Neptune Database | S3 Vectors |
| `neptune-graph` | Neptune Analytics | (built-in) |
| `neptune-graph-aoss` | Neptune Analytics | OpenSearch Serverless |
| `neptune-graph-postgresql` | Neptune Analytics | Aurora PostgreSQL |
| `neptune-graph-s3vectors` | Neptune Analytics | S3 Vectors |
| `neo4j-aoss` | Neo4j | OpenSearch Serverless |

## Test Architecture Overview

1. **`build-tests.sh`** (local machine) — Zips test assets and publishes them to S3, then triggers the CloudFormation template
2. **`graphrag-toolkit-tests.json`** (CloudFormation) — Creates IAM policies for testing and selects the correct quickstart template
3. **Quickstart CloudFormation template** (CloudFormation) — Creates the test environment (VPC, graph store, vector store, SageMaker notebook)
4. **`run_test_suite.sh`** (SageMaker notebook) — Downloads and installs dependencies, then starts the Python test runner
5. **`test_suite.py`** (SageMaker notebook) — Waits for stack completion, runs tests, publishes results to S3, and optionally sends SNS notifications

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
