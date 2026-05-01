#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
get_toml_value() {
    # Takes three parameters:
    # - TOML file path ($1)
    # - section ($2)
    # - the key ($3)
    # 
    # It first gets the section using the get_section function
    # Then it finds the key within that section
    # using grep and cut.

    local file="$1"
    local section="$2"
    local key="$3"

    get_section() {
        # Function to get the section from a TOML file
        # Takes two parameters:
        # - TOML file path ($1)
        # - section name ($2)
        # 
        # It uses sed to find the section
        # A section is terminated by a line with [ in pos 0 or the end of file.

        local file="$1"
        local section="$2"

        sed -n "/^\[$section\]/,/^\[/p" "$file" | sed '$d'
    }
        
    get_section "$file" "$section" | grep "^$key " | cut -d "=" -f2- | tr -d ' "'
} 

if [[ "$#" -gt 0 ]]; then
	if [[ "$1" == "--help" ]]; then
	  echo "Usage: build-tests.sh [options]"
		echo ""
		echo "where options include:"
		echo ""
		echo "  --test-file <comma-separated list of files containing test names>"
		echo "  --test <quoted, space-separated list of test names>"
		echo "  --toolkit-dir <graphrag-toolkit source directory>"
		echo "  --lexical-graph-install <lexical graph install URI>"
		echo "  --byokg-rag-install <byokg rag install URI>"
		echo "  --bucket <bucket name>"
		echo "  --region <AWS region>"
		echo "  --env-type <environment type enum>"
		echo "             Allowed values:"
		echo "               neptune-db-aoss"
		echo "               neptune-db-postgresql"
    echo "               neptune-db-s3vectors"
		echo "               neptune-graph"
		echo "               neptune-graph-aoss"
		echo "               neptune-graph-postgresql"
    echo "               neptune-graph-s3vectors"
		echo "               neo4j-aoss"
		echo "  --description <test description>"
		echo "  --neptune-engine-version <Neptune engine version, e.g. 1.4.6.2>"
    echo "  --neptune-instance-type <Neptune instance type>"
		echo "             Allowed values:"
		echo "               db.serverless"
		echo "               db.r8g.large"
		echo "               db.r8g.xlarge"
		echo "               db.r8g.2xlarge"
		echo "               db.r8g.4xlarge"
    echo "  --notebook-instance-type <Notebook instance type>"
		echo "             Allowed values:"
		echo "               ml.m5.xlarge"
		echo "               ml.m5.2xlarge"
		echo "               ml.m5.4xlarge"
		echo "               ml.p3.2xlarge"
    echo "  --opensearch-engine <engine enum>"
		echo "             Allowed values:"
		echo "               nmslib"
		echo "               faiss"
		echo "  --db-password <database password>"
		echo "  --topic <SNS topic name>"
		echo "  --extraction-llm <Model id or profile name>"
    echo "  --response-llm <Model id or profile name>"
    echo "  --embeddings-model <Embeddings model id>"
    echo "  --embeddings-dimensions <Embeddings dimensions>"
    echo "  --lexical-graph-wheel <path to local .whl file to upload to S3 and install>"
    echo "  --ssh-cidr <SSH CIDR block (default: auto-detected IP/32, use 0.0.0.0/0 for open access)>"
    echo "  --prev-stack <Previous stack name or ID>"
		echo "  --delete-on-pass"
		echo "  --fail-fast"
		echo "  --dry-run"
	  exit 0
	fi
fi

source ./.env

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
STACK_SUFFIX=$(date +%s)
GRAPH_NAME="gr-$STACK_SUFFIX"
TEST_DESCRIPTION="graphrag-toolkit integration test"
TESTS=""
PREV_STACK_NAME=""

if [[ -z "$ENV_TYPE" ]]; then
	ENV_TYPE="neptune-db-aoss"
fi

if [[ -z "$NEPTUNE_ENGINE_VERSION" ]]; then
	NEPTUNE_ENGINE_VERSION="1.4.7.0"
fi

if [[ -z "$OPENSEARCH_ENGINE" ]]; then
	OPENSEARCH_ENGINE="faiss"
fi

if [[ -z "$DELETE_ON_PASS" ]]; then
	DELETE_ON_PASS="False"
fi

if [[ -z "$TEST_MODE" ]]; then
	TEST_MODE="all"
fi

if [[ -z "$FAIL_FAST" ]]; then
	FAIL_FAST="False"
fi

if [[ -z "$TEST_EXTRACTION_LLM" ]]; then
	TEST_EXTRACTION_LLM="us.anthropic.claude-sonnet-4-6"
fi

if [[ -z "$TEST_RESPONSE_LLM" ]]; then
	TEST_RESPONSE_LLM="us.anthropic.claude-sonnet-4-6"
fi

if [[ -z "$NEPTUNE_INSTANCE_TYPE" ]]; then
	NEPTUNE_INSTANCE_TYPE="db.r8g.large"
fi

if [[ -z "$NOTEBOOK_INSTANCE_TYPE" ]]; then
	NOTEBOOK_INSTANCE_TYPE="ml.m5.xlarge"
fi

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --test-file) TEST_FILE="$2"; shift ;;
        --test) TESTS="$2"; shift ;;
				--lexical-graph-install) LEXICAL_GRAPH_INSTALL_URI="$2"; shift ;;
				--lexical-graph-wheel) LEXICAL_GRAPH_WHEEL="$2"; shift ;;
				--byokg-rag-install) BYOKG_RAG_INSTALL_URI="$2"; shift ;;
        --bucket) BUCKET_NAME="$2"; shift ;;
        --region) REGION_NAME="$2"; shift ;;
				--env-type) ENV_TYPE="$2"; shift ;;
				--description) TEST_DESCRIPTION="$2"; shift ;;
				--neptune-engine-version) NEPTUNE_ENGINE_VERSION="$2"; shift ;;
        --neptune-instance-type) NEPTUNE_INSTANCE_TYPE="$2"; shift ;;
        --notebook-instance-type) NOTEBOOK_INSTANCE_TYPE="$2"; shift ;;
        --opensearch-engine) OPENSEARCH_ENGINE="$2"; shift ;;
        --db-password) DB_PASSWORD="$2"; shift ;;
				--topic) TOPIC="$2"; shift ;;
				--extraction-llm) TEST_EXTRACTION_LLM="$2"; shift ;;
        --response-llm) TEST_RESPONSE_LLM="$2"; shift ;;
        --embeddings-model) EMBEDDINGS_MODEL="$2"; shift ;;
        --embeddings-dimensions) EMBEDDINGS_DIMENSIONS="$2"; shift ;;
				--toolkit-dir) GRAPHRAG_TOOLKIT_DIR="$2"; shift ;;
        --ssh-cidr) SSHCIDR="$2"; shift ;;
        --prev-stack) PREV_STACK_NAME="$2"; shift ;;
				--delete-on-pass) DELETE_ON_PASS=True ;;
				--fail-fast) FAIL_FAST=True ;;
				--dry-run) DRY_RUN=True ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [[ -z "$SSHCIDR" ]]; then
  echo "Auto-detecting public IPv4 address..."
  MY_IP=$(curl -4 -s --max-time 10 ifconfig.co)
  if [[ -z "$MY_IP" ]]; then
    echo "ERROR: Failed to auto-detect public IPv4 address. Please specify --ssh-cidr manually."
    exit 1
  fi
  SSHCIDR="$MY_IP/32"
  echo "Detected IP: $MY_IP — SSH will be restricted to $SSHCIDR"
fi

if [[ -z "$DB_PASSWORD" ]]; then
	DB_PASSWORD="p!$(uuidgen)"
fi

S3_PREFIX="graphrag-toolkit-tests/$GRAPH_NAME"
S3_URL_ROOT="https://$BUCKET_NAME.s3.amazonaws.com/$S3_PREFIX"
S3_ROOT="s3://$BUCKET_NAME/$S3_PREFIX"
S3_RESULTS_BUCKET="$BUCKET_NAME"
S3_RESULTS_PREFIX="$S3_PREFIX/results"
GRAPHRAG_TOOLKIT_S3_URI="$S3_ROOT/packages/graphrag-toolkit.zip"
TOPIC_ARN=""

if [[ "$TOPIC" ]]; then
	TOPIC_ARN="arn:aws:sns:$REGION_NAME:$ACCOUNT_ID:$TOPIC"
fi

if [[ "$TEST_FILE" ]]; then
  files=$(echo $TEST_FILE | tr "," "\n")
  for f in $files
  do
  	while IFS= read -r line || [[ -n "$line" ]]; do
  			if [[ "$TESTS" ]]; then
  				TESTS+=" $line"
  			else
  				TESTS+="$line"
  			fi
  	done < $f
  done
fi

BATCH_INFERENCE_ROLE="arn:aws:iam::$ACCOUNT_ID:role/graphrag-toolkit/$GRAPH_NAME-batch-inference-role"

pushd $GRAPHRAG_TOOLKIT_DIR
find . -name '*.DS_Store' -type f -delete
popd

rm -rf temp
mkdir temp
rm -rf target
mkdir target

pushd temp

toolkit_version=$(get_toml_value "$GRAPHRAG_TOOLKIT_DIR/lexical-graph/pyproject.toml" "project" "version")
current_timestamp=$(date +%s000)

mkdir -p graphrag/assets/packages
mkdir graphrag-toolkit
mkdir lexical-graph-examples

if [[ "$LEXICAL_GRAPH_WHEEL" ]]; then
    if [[ "$LEXICAL_GRAPH_INSTALL_URI" ]]; then
        echo "ERROR: --lexical-graph-wheel and --lexical-graph-install are mutually exclusive."
        exit 1
    fi
    if [[ ! -f "$LEXICAL_GRAPH_WHEEL" ]]; then
        echo "ERROR: Wheel file not found: $LEXICAL_GRAPH_WHEEL"
        exit 1
    fi
    if [[ "$LEXICAL_GRAPH_WHEEL" != *.whl ]]; then
        echo "ERROR: --lexical-graph-wheel expects a .whl file, got: $LEXICAL_GRAPH_WHEEL"
        exit 1
    fi
    WHEEL_FILENAME=$(basename "$LEXICAL_GRAPH_WHEEL")
    echo "Copying wheel $WHEEL_FILENAME into packages for S3 upload..."
    cp "$LEXICAL_GRAPH_WHEEL" graphrag/assets/packages/"$WHEEL_FILENAME"
    LEXICAL_GRAPH_INSTALL_URI="$S3_ROOT/packages/$WHEEL_FILENAME"
fi

if [[ -z "$LEXICAL_GRAPH_INSTALL_URI" ]]; then
    # Only copy source code to test notebook if install URI not supplied
	cp -r $GRAPHRAG_TOOLKIT_DIR/lexical-graph/src/* graphrag-toolkit
fi

cp -r $GRAPHRAG_TOOLKIT_DIR/lexical-graph-contrib/* graphrag-toolkit
cp -r $GRAPHRAG_TOOLKIT_DIR/byokg-rag/src/* graphrag-toolkit
cp -r $GRAPHRAG_TOOLKIT_DIR/examples/lexical-graph/notebooks/* lexical-graph-examples
cp -r $GRAPHRAG_TOOLKIT_DIR/examples/byokg-rag/* lexical-graph-examples
cp -r ./../test-scripts/* lexical-graph-examples
cp -r ./../source-data/* lexical-graph-examples

echo "__version__ = '$toolkit_version.$current_timestamp'" >> ./graphrag-toolkit/graphrag_toolkit/lexical_graph/_version.py

echo "export GRAPHRAG_TOOLKIT_S3_URI=$GRAPHRAG_TOOLKIT_S3_URI" >> lexical-graph-examples/.env.testing
echo "export S3_RESULTS_BUCKET=$S3_RESULTS_BUCKET" >> lexical-graph-examples/.env.testing
echo "export S3_RESULTS_PREFIX=$S3_RESULTS_PREFIX" >> lexical-graph-examples/.env.testing
echo "export APPLICATION_ID=$GRAPH_NAME" >> lexical-graph-examples/.env.testing
echo "export TOPIC_ARN=$TOPIC_ARN" >> lexical-graph-examples/.env.testing
echo "export AWS_REGION_NAME=$REGION_NAME" >> lexical-graph-examples/.env.testing
echo "export NEPTUNE_ENGINE_VERSION=$NEPTUNE_ENGINE_VERSION" >> lexical-graph-examples/.env.testing
echo "export NEPTUNE_INSTANCE_TYPE=$NEPTUNE_INSTANCE_TYPE" >> lexical-graph-examples/.env.testing
echo "export NOTEBOOK_INSTANCE_TYPE=$NOTEBOOK_INSTANCE_TYPE" >> lexical-graph-examples/.env.testing
echo "export OPENSEARCH_ENGINE=$OPENSEARCH_ENGINE" >> lexical-graph-examples/.env.testing
echo "export TEST_DESCRIPTION='$TEST_DESCRIPTION'" >> lexical-graph-examples/.env.testing
echo "export DELETE_ON_PASS=$DELETE_ON_PASS" >> lexical-graph-examples/.env.testing
echo "export DELETE_STACK_ROLE=$DELETE_STACK_ROLE" >> lexical-graph-examples/.env.testing
echo "export BATCH_INFERENCE_ROLE=$BATCH_INFERENCE_ROLE" >> lexical-graph-examples/.env.testing
echo "export FAIL_FAST=$FAIL_FAST" >> lexical-graph-examples/.env.testing
echo "export TEST_EXTRACTION_LLM=$TEST_EXTRACTION_LLM" >> lexical-graph-examples/.env.testing
echo "export TEST_RESPONSE_LLM=$TEST_RESPONSE_LLM" >> lexical-graph-examples/.env.testing
echo "export INCLUDE_CLASSIFICATION_IN_ENTITY_ID=False" >> lexical-graph-examples/.env.testing
if [[ "$TESTS" ]]; then
	echo "export TESTS='$TESTS'" >> lexical-graph-examples/.env.testing
fi
if [[ "$LEXICAL_GRAPH_INSTALL_URI" ]]; then
	echo "export LEXICAL_GRAPH_INSTALL_URI='$LEXICAL_GRAPH_INSTALL_URI'" >> lexical-graph-examples/.env.testing
fi
if [[ "$BYOKG_RAG_INSTALL_URI" ]]; then
	echo "export BYOKG_RAG_INSTALL_URI='$BYOKG_RAG_INSTALL_URI'" >> lexical-graph-examples/.env.testing
fi
if [[ "$EMBEDDINGS_MODEL" ]]; then
	echo "export EMBEDDINGS_MODEL='$EMBEDDINGS_MODEL'" >> lexical-graph-examples/.env.testing
fi
if [[ "$EMBEDDINGS_DIMENSIONS" ]]; then
	echo "export EMBEDDINGS_DIMENSIONS='$EMBEDDINGS_DIMENSIONS'" >> lexical-graph-examples/.env.testing
fi

zip -r graphrag-toolkit.zip graphrag-toolkit # zip under directory

pushd lexical-graph-examples
zip -r ../lexical-graph-examples.zip .  # add files and subdir direct to zip
popd

mv graphrag-toolkit.zip graphrag/assets/packages/graphrag-toolkit.zip
mv lexical-graph-examples.zip graphrag/assets/packages/lexical-graph-examples.zip

popd

cp -r cloudformation-templates temp/graphrag/assets/
cp $GRAPHRAG_TOOLKIT_DIR/examples/lexical-graph/cloudformation-templates/*.json temp/graphrag/assets/cloudformation-templates/
pushd temp
zip -r graphrag.zip graphrag
cp graphrag.zip ../target/graphrag.zip
popd

rm -rf temp

pushd target
unzip graphrag.zip
cd graphrag
aws s3 cp assets/ $S3_ROOT --recursive --region "$REGION_NAME"
popd

echo ""
echo "----------------------------------------------------"
echo "GRAPHRAG_TOOLKIT_DIR     : $GRAPHRAG_TOOLKIT_DIR"
echo "LEXICAL_GRAPH_WHEEL      : $LEXICAL_GRAPH_WHEEL"
echo "LEXICAL_GRAPH_INSTALL_URI: $LEXICAL_GRAPH_INSTALL_URI"
echo "BYOKG_RAG_INSTALL_URI    : $BYOKG_RAG_INSTALL_URI"
echo "ENV_TYPE                 : $ENV_TYPE"
echo "S3_PREFIX                : $S3_PREFIX"
echo "S3_URL_ROOT              : $S3_URL_ROOT"
echo "S3_ROOT                  : $S3_ROOT"
echo "S3_RESULTS_BUCKET        : $S3_RESULTS_BUCKET"
echo "S3_RESULTS_PREFIX        : $S3_RESULTS_PREFIX"
echo "GRAPHRAG_TOOLKIT_S3_URI  : $GRAPHRAG_TOOLKIT_S3_URI"
echo "TOPIC_ARN                : $TOPIC_ARN"
echo "TEST_DESCRIPTION         : $TEST_DESCRIPTION"
echo "DELETE_ON_PASS           : $DELETE_ON_PASS"
echo "NEPTUNE_ENGINE_VERSION   : $NEPTUNE_ENGINE_VERSION"
echo "NEPTUNE_INSTANCE_TYPE    : $NEPTUNE_INSTANCE_TYPE"
echo "NOTEBOOK_INSTANCE_TYPE   : $NOTEBOOK_INSTANCE_TYPE"
echo "OPENSEARCH_ENGINE        : $OPENSEARCH_ENGINE"
echo "FAIL_FAST                : $FAIL_FAST"
echo "BATCH_INFERENCE_ROLE     : $BATCH_INFERENCE_ROLE"
echo "DELETE_STACK_ROLE        : $DELETE_STACK_ROLE"
echo "TEST_EXTRACTION_LLM      : $TEST_EXTRACTION_LLM"
echo "TEST_RESPONSE_LLM        : $TEST_RESPONSE_LLM"
echo "EMBEDDINGS_MODEL         : $EMBEDDINGS_MODEL"
echo "EMBEDDINGS_DIMENSIONS    : $EMBEDDINGS_DIMENSIONS"
echo "SSHCIDR                  : $SSHCIDR"
echo "TESTS                    : $TESTS"
echo "PREV_STACK_NAME"         : $PREV_STACK_NAME
echo "----------------------------------------------------"
echo ""

if [[ -z "$DRY_RUN" ]]; then

  validate_model() {
    local label="$1"
    local model_id="$2"
    local prefix="${model_id%%.*}"

    echo "Validating $label: $model_id (region: $REGION_NAME)..."

    if [[ "$prefix" == "us" || "$prefix" == "eu" || "$prefix" == "au" || "$prefix" == "jp" || "$prefix" == "global" ]]; then
      if ! aws bedrock get-inference-profile --inference-profile-identifier "$model_id" --region "$REGION_NAME" > /dev/null 2>&1; then
        echo ""
        echo "ERROR: $label model '$model_id' is not available in region '$REGION_NAME'."
        echo "  The inference profile could not be found or is not accessible."
        echo "  Check the latest supported models: https://docs.aws.amazon.com/bedrock/latest/userguide/model-cards.html"
        exit 1
      fi
    else
      if ! aws bedrock get-foundation-model --model-identifier "$model_id" --region "$REGION_NAME" > /dev/null 2>&1; then
        echo ""
        echo "ERROR: $label model '$model_id' is not available in region '$REGION_NAME'."
        echo "  The foundation model could not be found or is not accessible."
        echo "  Check the latest supported models: https://docs.aws.amazon.com/bedrock/latest/userguide/model-cards.html"
        exit 1
      fi
    fi

    echo "✓ $label: $model_id"
  }

  validate_model "Extraction LLM" "$TEST_EXTRACTION_LLM"
  validate_model "Response LLM" "$TEST_RESPONSE_LLM"

  if [[ "$PREV_STACK_NAME" ]]; then
    echo "Deleting previous stack: $PREV_STACK_NAME"
    aws cloudformation delete-stack --stack-name "$PREV_STACK_NAME" --region $REGION_NAME
    STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$PREV_STACK_NAME" --region $REGION_NAME | jq -r ".Stacks[0].StackStatus")
    while [[ "$STACK_STATUS" != "DELETE_COMPLETE" ]]
    do
      echo "Waiting for prev stack to be deleted ($STACK_STATUS), sleeping 10 seconds"
      sleep 10
      STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$PREV_STACK_NAME" --region $REGION_NAME | jq -r ".Stacks[0].StackStatus")
    done
  fi
	aws cloudformation create-stack --stack-name "$GRAPH_NAME-tests" \
	  --template-url "$S3_URL_ROOT/cloudformation-templates/graphrag-toolkit-tests.json" \
	  --parameters \
		ParameterKey=ApplicationId,ParameterValue="$GRAPH_NAME" \
	  ParameterKey=S3Bucket,ParameterValue="$BUCKET_NAME" \
		ParameterKey=EnvType,ParameterValue="$ENV_TYPE" \
		ParameterKey=TopicArn,ParameterValue="$TOPIC_ARN" \
		ParameterKey=DeleteStackRoleArn,ParameterValue="$DELETE_STACK_ROLE" \
		ParameterKey=NeptuneEngineVersion,ParameterValue="$NEPTUNE_ENGINE_VERSION" \
    ParameterKey=NeptuneInstanceType,ParameterValue="$NEPTUNE_INSTANCE_TYPE" \
    ParameterKey=NotebookInstanceType,ParameterValue="$NOTEBOOK_INSTANCE_TYPE" \
		ParameterKey=IamPolicyArn,ParameterValue="$ADDITIONAL_IAM_POLICY_ARN" \
		ParameterKey=SSHCIDR,ParameterValue="$SSHCIDR" \
		ParameterKey=DbPassword,ParameterValue="$DB_PASSWORD" \
	  --capabilities CAPABILITY_NAMED_IAM \
	  --tags \
	    Key=ApplicationName,Value="graphrag-toolkit test" \
	    Key=GraphName,Value=$GRAPH_NAME \
	    Key=S3Location,Value="$S3_ROOT/results/" \
			Key=Description,Value="$TEST_DESCRIPTION" \
			Key=EnvType,Value="$ENV_TYPE" \
	  --region $REGION_NAME
fi




