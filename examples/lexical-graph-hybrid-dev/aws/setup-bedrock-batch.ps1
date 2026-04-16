# Usage: .\setup-graphrag.ps1 [-Profile <aws_profile>]
param(
    [string]$Profile = ""
)

# Build conditional profile args for splatting
$ProfileArgs = @()
if ($Profile) {
    $ProfileArgs = @("--profile", $Profile)
}

function Check-AwsCredentials {
    if (-not (aws sts get-caller-identity @ProfileArgs -ErrorAction SilentlyContinue)) {
        Write-Host "Error: No valid AWS credentials found"
        if ($Profile) {
            Write-Host "If using AWS SSO, run: aws sso login --profile $Profile"
            Write-Host "If using traditional credentials, run: aws configure --profile $Profile"
        } else {
            Write-Host "If using AWS SSO, run: aws sso login"
            Write-Host "If using traditional credentials, run: aws configure"
        }
        exit 1
    }
}

function Get-AccountDetails {
    $global:AccountId = aws sts get-caller-identity @ProfileArgs --query Account --output text
    if (-not $AccountId) {
        Write-Host "Error: Could not determine AWS Account ID"
        exit 1
    }

    $global:Region = aws configure get region @ProfileArgs
    if (-not $Region) {
        Write-Host "Error: Could not determine AWS Region"
        exit 1
    }

    $global:CurrentRole = aws sts get-caller-identity @ProfileArgs --query Arn --output text | Select-String -Pattern 'AWSReservedSSO_[^/]+' | ForEach-Object { $_.Matches.Value }
}

Check-AwsCredentials
Get-AccountDetails

$ApplicationId = "graphrag-toolkit"
$BucketName = "graphrag-toolkit-$AccountId"
$RoleName = "bedrock-batch-inference-role"
$PolicyName = "bedrock-batch-inference-policy"
$ModelId = "anthropic.claude-v2"
$TableName = "graphrag-toolkit-batch-table"

# Create S3 bucket
Write-Host "Creating S3 bucket $BucketName..."
if (-not (aws s3api head-bucket --bucket $BucketName @ProfileArgs -ErrorAction SilentlyContinue)) {
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $BucketName --region $Region @ProfileArgs
    } else {
        aws s3api create-bucket --bucket $BucketName --region $Region --create-bucket-configuration LocationConstraint=$Region @ProfileArgs
    }
    Write-Host "Bucket created successfully"
} else {
    Write-Host "Bucket $BucketName already exists"
}

# Create DynamoDB table
Write-Host "Creating DynamoDB table $TableName..."
if (-not (aws dynamodb describe-table --table-name $TableName @ProfileArgs -ErrorAction SilentlyContinue)) {
    aws dynamodb create-table `
        --table-name $TableName `
        --attribute-definitions `
            AttributeName=collection_id,AttributeType=S `
            AttributeName=completion_date,AttributeType=S `
            AttributeName=reader_type,AttributeType=S `
        --key-schema `
            AttributeName=collection_id,KeyType=HASH `
            AttributeName=completion_date,KeyType=RANGE `
        --billing-mode PAY_PER_REQUEST `
        --global-secondary-indexes "[{`"IndexName`": `"reader_type-index`", `"KeySchema`": [{`"AttributeName`": `"reader_type`", `"KeyType`": `"HASH`"}, {`"AttributeName`": `"completion_date`", `"KeyType`": `"RANGE`"}], `"Projection`": {`"ProjectionType`": `"ALL`"}}]" `
        --region $Region `
        @ProfileArgs

    Write-Host "Waiting for DynamoDB table to become active..."
    aws dynamodb wait table-exists --table-name $TableName --region $Region @ProfileArgs
    Write-Host "DynamoDB table created successfully"
} else {
    Write-Host "DynamoDB table $TableName already exists"
}

# Write IAM policy JSON files
@"
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
                    "aws:SourceAccount": "$AccountId"
                },
                "ArnEquals": {
                    "aws:SourceArn": "arn:aws:bedrock:$Region:$AccountId:model-invocation-job/*"
                }
            }
        }
    ]
}
"@ | Set-Content -Encoding UTF8 trust-policy.json

@"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["bedrock:InvokeModel"],
            "Resource": "arn:aws:bedrock:${Region}::foundation-model/*"
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
            "Resource": [
                "arn:aws:s3:::$BucketName",
                "arn:aws:s3:::$BucketName/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:ResourceAccount": ["$AccountId"]
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:Scan"],
            "Resource": "arn:aws:dynamodb:$Region:$AccountId:table/$TableName",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceAccount": ["$AccountId"]
                }
            }
        }
    ]
}
"@ | Set-Content -Encoding UTF8 role-permissions-policy.json

@"
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
                "arn:aws:bedrock:$Region::foundation-model/$ModelId",
                "arn:aws:bedrock:$Region:$AccountId:model-invocation-job/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": ["iam:PassRole"],
            "Resource": "arn:aws:iam::$AccountId:role/$RoleName"
        },
        {
            "Effect": "Allow",
            "Action": ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:Scan"],
            "Resource": "arn:aws:dynamodb:$Region:$AccountId:table/$TableName"
        }
    ]
}
"@ | Set-Content -Encoding UTF8 identity-permissions-policy.json

# Create IAM role and attach policy
Write-Host "Creating IAM role $RoleName..."
if (-not (aws iam get-role --role-name $RoleName @ProfileArgs -ErrorAction SilentlyContinue)) {
    aws iam create-role --role-name $RoleName --assume-role-policy-document file://trust-policy.json @ProfileArgs
    Write-Host "Role created successfully"
} else {
    Write-Host "Role $RoleName already exists"
}

$PolicyArn = "arn:aws:iam::$AccountId:policy/$PolicyName"
if (-not (aws iam get-policy --policy-arn $PolicyArn @ProfileArgs -ErrorAction SilentlyContinue)) {
    aws iam create-policy --policy-name $PolicyName --policy-document file://role-permissions-policy.json @ProfileArgs
    Write-Host "Policy created successfully"
} else {
    Write-Host "Policy $PolicyName already exists"
}

aws iam attach-role-policy --role-name $RoleName --policy-arn $PolicyArn @ProfileArgs

# Create identity policy
$IdentityPolicyName = "bedrock-batch-identity-policy"
$IdentityPolicyArn = "arn:aws:iam::$AccountId:policy/$IdentityPolicyName"
if (-not (aws iam get-policy --policy-arn $IdentityPolicyArn @ProfileArgs -ErrorAction SilentlyContinue)) {
    aws iam create-policy --policy-name $IdentityPolicyName --policy-document file://identity-permissions-policy.json @ProfileArgs
    Write-Host "Identity policy created successfully"
} else {
    Write-Host "Identity policy $IdentityPolicyName already exists"
}

# Clean up temp files
Remove-Item trust-policy.json, role-permissions-policy.json, identity-permissions-policy.json -Force

# Upload S3 prompt files for S3PromptProvider (used by notebook 04)
Write-Host "Uploading prompt files to S3..."
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

python3 -c @"
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
print(data['variants'][0]['templateConfiguration']['text']['text'], end='')
"@ "$ScriptDir/system_prompt.json" | aws s3 cp - "s3://$BucketName/prompts/system_prompt.txt" --content-type text/plain --region $Region @ProfileArgs

python3 -c @"
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
print(data['variants'][0]['templateConfiguration']['text']['text'], end='')
"@ "$ScriptDir/user_prompt.json" | aws s3 cp - "s3://$BucketName/prompts/user_prompt.txt" --content-type text/plain --region $Region @ProfileArgs

Write-Host "Prompt files uploaded to s3://$BucketName/prompts/"

# Summary
Write-Host "`nSetup complete!"
Write-Host "Bucket: $BucketName"
Write-Host "DynamoDB Table: arn:aws:dynamodb:$Region:$AccountId:table/$TableName"
Write-Host "Role ARN: arn:aws:iam::$AccountId:role/$RoleName"
Write-Host "Policy ARN: $PolicyArn"
Write-Host "Identity Policy ARN: $IdentityPolicyArn"

if ($CurrentRole) {
    Write-Host "`nNOTE: You are using AWS SSO with role: $CurrentRole"
    Write-Host "To complete setup, go to IAM Identity Center and attach the identity policy to the Permission Set."
} else {
    Write-Host "`nNOTE: You are using traditional IAM credentials."
    Write-Host "Ensure the identity policy is attached to your IAM user or role."
}
