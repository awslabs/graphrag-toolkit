#!/bin/bash

# Script to update a CloudFormation stack with the modified template
# Supports optional --profile argument for AWS CLI profile
# Usage: ./update-stack.sh [--profile <profile_name>]

# Configuration variables
STACK_NAME="AWS-GraphRAG-01"  # Replace with your stack name
TEMPLATE_FILE="graphrag-toolkit-neptune-db-aurora-postgres-existing-vpc.json"  # Path to your updated template file
S3_BUCKET_ARN="arn:aws:s3:::my-rag-extract-bucket"  # Replace with your S3 bucket ARN
REGION="us-east-1"  # Replace with your AWS region
PARAMETERS_FILE="parameters.json"  # Temporary parameters file
AWS_PROFILE="master"  # Will be set if --profile is provided

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --profile)
      AWS_PROFILE="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown argument: $1"
      echo "Usage: $0 [--profile <profile_name>]"
      exit 1
      ;;
  esac
done

# Function to run AWS CLI commands with optional profile
run_aws_command() {
  if [ -n "$AWS_PROFILE" ]; then
    aws --profile "$AWS_PROFILE" "$@"
  else
    aws "$@"
  fi
}

# Create parameters file
cat > $PARAMETERS_FILE << EOL
[
  {
    "ParameterKey": "VPCId",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "SubnetId1",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "SubnetId2",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "SubnetId3",
    "ParameterValue": ""
  },
  {
    "ParameterKey": "NotebookSubnetId",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "ApplicationId",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "NeptuneDbInstanceType",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "MinNCU",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "MaxNCU",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "EnableAuditLog",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "PostgresDbInstanceType",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "MinACU",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "MaxACU",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "IamPolicyArn",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "NotebookInstanceType",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "ExampleNotebooksURL",
    "UsePreviousValue": true
  },
  {
    "ParameterKey": "GraphRAGS3BucketArn",
    "ParameterValue": "$S3_BUCKET_ARN"
  }
]
EOL

# Verify template file exists
if [ ! -f "$TEMPLATE_FILE" ]; then
  echo "Error: Template file $TEMPLATE_FILE not found."
  exit 1
fi

# Verify AWS CLI is configured and profile is valid
if ! run_aws_command sts get-caller-identity > /dev/null 2>&1; then
  echo "Error: AWS CLI is not configured, or the profile '$AWS_PROFILE' is invalid."
  exit 1
fi

# Update the CloudFormation stack
echo "Updating CloudFormation stack $STACK_NAME in region $REGION..."
run_aws_command cloudformation update-stack \
  --stack-name "$STACK_NAME" \
  --template-body file://"$TEMPLATE_FILE" \
  --parameters file://"$PARAMETERS_FILE" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$REGION"

# Check if the update command was successful
if [ $? -eq 0 ]; then
  echo "Stack update initiated successfully."
  echo "Monitoring stack update events..."
  run_aws_command cloudformation wait stack-update-complete --stack-name "$STACK_NAME" --region "$REGION"
  if [ $? -eq 0 ]; then
    echo "Stack update completed successfully."
  else
    echo "Stack update failed or was rolled back. Check CloudFormation events for details:"
    run_aws_command cloudformation describe-stack-events --stack-name "$STACK_NAME" --region "$REGION" --query "StackEvents[?ResourceStatus=='UPDATE_FAILED' || ResourceStatus=='UPDATE_ROLLBACK_COMPLETE'].{Resource:ResourceType,Status:ResourceStatus,Reason:ResourceStatusReason}" --output table
    exit 1
  fi
else
  echo "Error: Failed to initiate stack update. Check error message above or CloudFormation logs."
  exit 1
fi

# Clean up parameters file
rm -f "$PARAMETERS_FILE"
echo "Parameters file $PARAMETERS_FILE deleted."

# Instructions for verification
echo "Update complete. To verify the changes:"
echo "1. Access the SageMaker notebook and run 'echo \$LOCAL_EXTRACT_S3' in a terminal to check the variable."
echo "2. Check '/home/ec2-user/SageMaker/graphrag-toolkit/.env' for the LOCAL_EXTRACT_S3 variable."
echo "3. Test S3 access with 'aws s3 ls \$LOCAL_EXTRACT_S3'."
echo "4. Check stack outputs with 'aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION --query \"Stacks[0].Outputs\"'"