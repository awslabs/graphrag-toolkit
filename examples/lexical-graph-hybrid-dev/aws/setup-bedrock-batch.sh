#!/bin/bash

# Script to set up S3 bucket, IAM role, policies, and DynamoDB table for GraphRAG
# Usage: ./setup-graphrag.sh [profile_name]

PROFILE="${1:-""}"

# Build conditional profile flag
PROFILE_ARGS=""
if [ -n "$PROFILE" ]; then
    PROFILE_ARGS="--profile ${PROFILE}"
fi

# Check if AWS credentials are available
check_aws_credentials() {
    if ! aws sts get-caller-identity ${PROFILE_ARGS} &>/dev/null; then
        echo "Error: No valid AWS credentials found"
        if [ -n "$PROFILE" ]; then
            echo "If using AWS SSO, please run 'aws sso login --profile ${PROFILE}'"
            echo "If using traditional credentials, please configure AWS CLI with 'aws configure --profile ${PROFILE}'"
        else
            echo "If using AWS SSO, please run 'aws sso login'"
            echo "If using traditional credentials, please configure AWS CLI with 'aws configure'"
        fi
        exit 1
    fi
}

# Get account details safely
get_account_details() {
    ACCOUNT_ID=$(aws sts get-caller-identity ${PROFILE_ARGS} --query Account --output text)
    if [ -z "$ACCOUNT_ID" ]; then
        echo "Error: Could not determine AWS Account ID"
        exit 1
    fi

    REGION=$(aws configure get region ${PROFILE_ARGS})
    if [ -z "$REGION" ]; then
        echo "Error: Could not determine AWS Region"
        exit 1
    fi

    # For SSO users, get the role name they're using
    CURRENT_ROLE=$(aws sts get-caller-identity ${PROFILE_ARGS} --query Arn --output text | grep -o 'AWSReservedSSO_[^/]*' || echo "")
}

# Configuration variables
check_aws_credentials
get_account_details

APPLICATION_ID="graphrag-toolkit"
BUCKET_NAME="graphrag-toolkit-${ACCOUNT_ID}"  # Using account ID to ensure uniqueness
ROLE_NAME="${BATCH_ROLE_NAME:-bedrock-batch-inference-role}"
POLICY_NAME="bedrock-batch-inference-policy"
MODEL_ID="anthropic.claude-v2"  # Example model ID, adjust as needed
TABLE_NAME="graphrag-toolkit-batch-table"

# Create S3 bucket with error handling
echo "Creating S3 bucket ${BUCKET_NAME}..."
if ! aws s3api head-bucket --bucket "${BUCKET_NAME}" ${PROFILE_ARGS} 2>/dev/null; then
    if [[ "${REGION}" == "us-east-1" ]]; then
        aws s3api create-bucket \
            --bucket "${BUCKET_NAME}" \
            --region "${REGION}" \
            ${PROFILE_ARGS} || exit 1
    else
        aws s3api create-bucket \
            --bucket "${BUCKET_NAME}" \
            --region "${REGION}" \
            --create-bucket-configuration LocationConstraint="${REGION}" \
            ${PROFILE_ARGS} || exit 1
    fi
    echo "Bucket created successfully"
else
    echo "Bucket ${BUCKET_NAME} already exists"
fi

# Create DynamoDB table with error handling
echo "Creating DynamoDB table ${TABLE_NAME}..."
if ! aws dynamodb describe-table --table-name "${TABLE_NAME}" ${PROFILE_ARGS} &>/dev/null; then
    aws dynamodb create-table \
        --table-name "${TABLE_NAME}" \
        --attribute-definitions \
            AttributeName=collection_id,AttributeType=S \
            AttributeName=completion_date,AttributeType=S \
            AttributeName=reader_type,AttributeType=S \
        --key-schema \
            AttributeName=collection_id,KeyType=HASH \
            AttributeName=completion_date,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST \
        --global-secondary-indexes \
            "[{
                \"IndexName\": \"reader_type-index\",
                \"KeySchema\": [
                    {\"AttributeName\": \"reader_type\", \"KeyType\": \"HASH\"},
                    {\"AttributeName\": \"completion_date\", \"KeyType\": \"RANGE\"}
                ],
                \"Projection\": {\"ProjectionType\": \"ALL\"}
            }]" \
        --region "${REGION}" \
        ${PROFILE_ARGS} || exit 1
    echo "Waiting for DynamoDB table to become active..."
    aws dynamodb wait table-exists \
        --table-name "${TABLE_NAME}" \
        --region "${REGION}" \
        ${PROFILE_ARGS} || exit 1
    echo "DynamoDB table created successfully"
else
    echo "DynamoDB table ${TABLE_NAME} already exists"
fi

# Create trust policy for the service role
echo "Creating trust policy..."
cat << EOF > trust-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "bedrock.amazonaws.com"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "aws:SourceAccount": "${ACCOUNT_ID}"
                },
                "ArnEquals": {
                    "aws:SourceArn": "arn:aws:bedrock:${REGION}:${ACCOUNT_ID}:model-invocation-job/*"
                }
            }
        }
    ]
}
EOF

# Create service role permissions policy
echo "Creating service role permissions policy..."
cat << EOF > role-permissions-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel"
            ],
            "Resource": "arn:aws:bedrock:${REGION}::foundation-model/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:ResourceAccount": ["${ACCOUNT_ID}"]
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:Scan"
            ],
            "Resource": "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${TABLE_NAME}",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceAccount": ["${ACCOUNT_ID}"]
                }
            }
        }
    ]
}
EOF

# Create IAM identity permissions policy
echo "Creating identity permissions policy..."
cat << EOF > identity-permissions-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:CreateModelInvocationJob",
                "bedrock:GetModelInvocationJob",
                "bedrock:ListModelInvocationJobs",
                "bedrock:StopModelInvocationJob"
            ],
            "Resource": [
                "arn:aws:bedrock:${REGION}::foundation-model/${MODEL_ID}",
                "arn:aws:bedrock:${REGION}:${ACCOUNT_ID}:model-invocation-job/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:Scan"
            ],
            "Resource": "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${TABLE_NAME}"
        }
    ]
}
EOF

# Create the IAM role with error handling
echo "Creating IAM role ${ROLE_NAME}..."
if ! aws iam get-role --role-name "${ROLE_NAME}" ${PROFILE_ARGS} &>/dev/null; then
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document file://trust-policy.json \
        ${PROFILE_ARGS} || exit 1
    echo "Role created successfully"
else
    echo "Role ${ROLE_NAME} already exists"
fi

# Create and attach the service role policy
echo "Creating and attaching service role policy..."
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

if ! aws iam get-policy --policy-arn "${POLICY_ARN}" ${PROFILE_ARGS} &>/dev/null; then
    aws iam create-policy \
        --policy-name "${POLICY_NAME}" \
        --policy-document file://role-permissions-policy.json \
        ${PROFILE_ARGS} || exit 1
    echo "Policy created successfully"
else
    echo "Policy ${POLICY_NAME} already exists"
fi

# Attach policy to role
aws iam attach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}" \
    ${PROFILE_ARGS} || exit 1

# Create the identity permissions policy
IDENTITY_POLICY_NAME="bedrock-batch-identity-policy"
IDENTITY_POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${IDENTITY_POLICY_NAME}"

if ! aws iam get-policy --policy-arn "${IDENTITY_POLICY_ARN}" ${PROFILE_ARGS} &>/dev/null; then
    aws iam create-policy \
        --policy-name "${IDENTITY_POLICY_NAME}" \
        --policy-document file://identity-permissions-policy.json \
        ${PROFILE_ARGS} || exit 1
    echo "Identity policy created successfully"
else
    echo "Identity policy ${IDENTITY_POLICY_NAME} already exists"
fi

# Clean up temporary files
rm -f trust-policy.json role-permissions-policy.json identity-permissions-policy.json

# Upload S3 prompt files for S3PromptProvider (used by notebook 04)
echo "Uploading prompt files to S3..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Extract prompt text from JSON and upload as .txt
python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
print(data['variants'][0]['templateConfiguration']['text']['text'], end='')
" "${SCRIPT_DIR}/system_prompt.json" | aws s3 cp - "s3://${BUCKET_NAME}/prompts/system_prompt.txt" --content-type text/plain ${PROFILE_ARGS}

python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
print(data['variants'][0]['templateConfiguration']['text']['text'], end='')
" "${SCRIPT_DIR}/user_prompt.json" | aws s3 cp - "s3://${BUCKET_NAME}/prompts/user_prompt.txt" --content-type text/plain ${PROFILE_ARGS}

echo "Prompt files uploaded to s3://${BUCKET_NAME}/prompts/"

echo "Setup complete!"
echo "Bucket name: ${BUCKET_NAME}"
echo "DynamoDB Table ARN: arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${TABLE_NAME}"
echo "Role ARN: arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "Service Role Policy ARN: ${POLICY_ARN}"
echo "Identity Policy ARN: ${IDENTITY_POLICY_ARN}"

if [ -n "$CURRENT_ROLE" ]; then
    echo ""
    echo "NOTE: You are using AWS SSO with role: ${CURRENT_ROLE}"
    echo "To complete setup, you need to:"
    echo "1. Go to AWS IAM Identity Center"
    echo "2. Find your Permission Set"
    echo "3. Add the identity policy (${IDENTITY_POLICY_ARN}) to your Permission Set"
else
    # Auto-attach identity policy to the caller's IAM role
    CALLER_ARN=$(aws sts get-caller-identity ${PROFILE_ARGS} --query Arn --output text)
    CALLER_ROLE=$(echo "$CALLER_ARN" | sed 's|.*assumed-role/||;s|.*role/||' | cut -d/ -f1)
    if [ -n "$CALLER_ROLE" ]; then
        echo ""
        echo "Attaching identity policy to your IAM role: ${CALLER_ROLE}..."
        aws iam attach-role-policy \
            --role-name "${CALLER_ROLE}" \
            --policy-arn "${IDENTITY_POLICY_ARN}" \
            ${PROFILE_ARGS} && echo "Identity policy attached successfully" \
            || echo "WARNING: Could not attach identity policy. Attach it manually to your IAM role."
    else
        echo ""
        echo "NOTE: You are using traditional IAM credentials"
        echo "Make sure to attach the identity policy to your IAM user or role"
    fi
fi
