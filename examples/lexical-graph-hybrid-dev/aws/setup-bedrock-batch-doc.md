
# Bedrock Batch Inference Setup Script Documentation

This script automates the provisioning of the necessary AWS resources to perform **Batch Model Invocation** jobs with Amazon Bedrock.

---

## What the Script Does

1. **Checks AWS Credentials**  
   Validates that the AWS CLI is authenticated using either:
   - SSO (e.g., `aws sso login`)
   - or static credentials (via `aws configure`)

   Use `--profile <name>` for a specific AWS credentials profile, or set the `AWS_PROFILE` environment variable.

2. **Retrieves AWS Account and Region Info**  
   Using the AWS profile, the script resolves:
   - `ACCOUNT_ID`
   - `REGION`
   - (Optional) Current SSO role being used

3. **Creates an S3 Bucket**  
   Creates a bucket named `graphrag-toolkit-<ACCOUNT_ID>` for uploading input/output files used in batch jobs.

4. **Creates an IAM Role for Bedrock (Execution Role)**  
   - Name: `bedrock-batch-inference-role`
   - Trusts the `bedrock.amazonaws.com` service
   - Permissions:  
     Allows access to the newly created S3 bucket.

5. **Creates an IAM Identity Policy**  
   - Name: `bedrock-batch-identity-policy`
   - Grants permission to:
     - Create, List, Get, and Stop Bedrock model invocation jobs
     - Pass the execution role to Bedrock

6. **Attaches Policies to Role/User**  
   - Attaches the role permissions to the `bedrock-batch-inference-role`
   - Prints instructions to attach the identity policy manually depending on credential type

7. **Cleanup**  
   Temporary policy files are deleted from the local directory.

---

## Output Resources

| Resource | Description |
|---------|-------------|
| S3 Bucket | `graphrag-toolkit-<ACCOUNT_ID>` |
| IAM Role | `bedrock-batch-inference-role` |
| IAM Role Policy | Grants S3 access for batch inference |
| IAM Identity Policy | Grants permission to submit and manage Bedrock batch jobs |

---

## Usage

```bash
bash setup-bedrock-batch.sh your-profile
```

If no profile is specified, it defaults to `default`.

---

## Manual IAM Setup Required (SSO Users)

If you're using AWS SSO, the script will print:
```
NOTE: You are using AWS SSO with role: <SSO_ROLE>
To complete setup, you need to:
1. Go to AWS IAM Identity Center
2. Find your Permission Set
3. Add the identity policy (arn:aws:iam::<ACCOUNT_ID>:policy/bedrock-batch-identity-policy) to your Permission Set
```

If you're using static credentials, you must manually attach the identity policy to the user/role.

---

## Related Policies

### Trust Policy (Role)
```json
{
  "Principal": {
    "Service": "bedrock.amazonaws.com"
  },
  "Condition": {
    "StringEquals": {
      "aws:SourceAccount": "<ACCOUNT_ID>"
    },
    "ArnEquals": {
      "aws:SourceArn": "arn:aws:bedrock:<REGION>:<ACCOUNT_ID>:model-invocation-job/*"
    }
  }
}
```

### Role Policy (S3 Access)
```json
{
  "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
  "Resource": [
    "arn:aws:s3:::graphrag-toolkit-<ACCOUNT_ID>",
    "arn:aws:s3:::graphrag-toolkit-<ACCOUNT_ID>/*"
  ]
}
```

### Identity Policy (Bedrock Access)
```json
{
  "Action": [
    "bedrock:CreateModelInvocationJob",
    "bedrock:GetModelInvocationJob",
    "bedrock:ListModelInvocationJobs",
    "bedrock:StopModelInvocationJob",
    "iam:PassRole"
  ]
}
```

---

## Prerequisites

- AWS CLI installed
- AWS credentials configured for the profile (via SSO or `aws configure`)
- Sufficient permissions to:
  - Create IAM roles and policies
  - Create S3 buckets
