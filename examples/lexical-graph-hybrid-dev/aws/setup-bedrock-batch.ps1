# Usage: .\setup-graphrag.ps1 [-Profile <aws_profile>]
param(
    [string]$Profile = "padmin"
)

function Check-AwsCredentials {
    if (-not (aws sts get-caller-identity --profile $Profile -ErrorAction SilentlyContinue)) {
        Write-Host "Error: No valid AWS credentials found for profile $Profile"
        Write-Host "If using AWS SSO, run: aws sso login --profile $Profile"
        Write-Host "If using traditional credentials, run: aws configure --profile $Profile"
        exit 1
    }
}

function Get-AccountDetails {
    $global:AccountId = aws sts get-caller-identity --profile $Profile --query Account --output text
    if (-not $AccountId) {
        Write-Host "Error: Could not determine AWS Account ID"
        exit 1
    }

    $global:Region = aws configure get region --profile $Profile
    if (-not $Region) {
        Write-Host "Error: Could not determine AWS Region"
        exit 1
    }

    $global:CurrentRole = aws sts get-caller-identity --profile $Profile --query Arn --output text | Select-String -Pattern 'AWSReservedSSO_[^/]+' | ForEach-Object { $_.Matches.Value }
}

Check-AwsCredentials
Get-AccountDetails

$ApplicationId = "graphrag-toolkit"
$BucketName = "local-rag-extract-$AccountId"
$RoleName = "bedrock-batch-inference-role"
$PolicyName = "bedrock-batch-inference-policy"
$ModelId = "anthropic.claude-v2"
$TableName = "$ApplicationId-GraphRAGCollections"

# Create S3 bucket
Write-Host "Creating S3 bucket $BucketName..."
if (-not (aws s3api head-bucket --bucket $BucketName --profile $Profile -ErrorAction SilentlyContinue)) {
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $BucketName --region $Region --profile $Profile
    } else {
        aws s3api create-bucket --bucket $BucketName --region $Region --create-bucket-configuration LocationConstraint=$Region --profile $Profile
    }
    Write-Host "Bucket created successfully"
} else {
    Write-Host "Bucket $BucketName already exists"
}

# Create DynamoDB table
Write-Host "Creating DynamoDB table $TableName..."
if (-not (aws dynamodb describe-table --table-name $TableName --profile $Profile -ErrorAction SilentlyContinue)) {
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
        --profile $Profile

    Write-Host "Waiting for DynamoDB table to become active..."
    aws dynamodb wait table-exists --table-name $TableName --region $Region --profile $Profile
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
if (-not (aws iam get-role --role-name $RoleName --profile $Profile -ErrorAction SilentlyContinue)) {
    aws iam create-role --role-name $RoleName --assume-role-policy-document file://trust-policy.json --profile $Profile
    Write-Host "Role created successfully"
} else {
    Write-Host "Role $RoleName already exists"
}

$PolicyArn = "arn:aws:iam::$AccountId:policy/$PolicyName"
if (-not (aws iam get-policy --policy-arn $PolicyArn --profile $Profile -ErrorAction SilentlyContinue)) {
    aws iam create-policy --policy-name $PolicyName --policy-document file://role-permissions-policy.json --profile $Profile
    Write-Host "Policy created successfully"
} else {
    Write-Host "Policy $PolicyName already exists"
}

aws iam attach-role-policy --role-name $RoleName --policy-arn $PolicyArn --profile $Profile

# Create identity policy
$IdentityPolicyName = "bedrock-batch-identity-policy"
$IdentityPolicyArn = "arn:aws:iam::$AccountId:policy/$IdentityPolicyName"
if (-not (aws iam get-policy --policy-arn $IdentityPolicyArn --profile $Profile -ErrorAction SilentlyContinue)) {
    aws iam create-policy --policy-name $IdentityPolicyName --policy-document file://identity-permissions-policy.json --profile $Profile
    Write-Host "Identity policy created successfully"
} else {
    Write-Host "Identity policy $IdentityPolicyName already exists"
}

# Clean up temp files
Remove-Item trust-policy.json, role-permissions-policy.json, identity-permissions-policy.json -Force

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
