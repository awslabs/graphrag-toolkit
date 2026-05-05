#!/bin/bash

# Usage:
# ./create_prompt_role.sh --role-name my-bedrock-prompt-role --profile my-aws-profile

set -e

# Default values
ROLE_NAME=""
PROFILE_OPTION=""

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --role-name)
            ROLE_NAME="$2"
            shift
            ;;
        --profile)
            PROFILE_OPTION="--profile $2"
            shift
            ;;
        *)
            echo "Unknown parameter passed: $1"
            exit 1
            ;;
    esac
    shift
done

if [[ -z "$ROLE_NAME" ]]; then
    echo "Error: --role-name is required"
    exit 1
fi

TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "bedrock.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
)

# Create the role
echo "Creating IAM role '$ROLE_NAME' for Bedrock..."
aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" \
    "$PROFILE_OPTION"

# Attach managed policy (adjust if using custom permissions)
echo "Attaching managed policy (AmazonBedrockFullAccess)..."
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name BedrockPromptMinimalPolicy \
  --policy-document file://bedrock-prompt-policy.json \
  "$PROFILE_OPTION"

echo "Done. Role ARN:"
aws iam get-role --role-name "$ROLE_NAME" --query "Role.Arn" --output text "$PROFILE_OPTION"
