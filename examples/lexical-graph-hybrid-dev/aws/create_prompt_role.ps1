# Usage:
# .\create_prompt_role.ps1 -RoleName "my-bedrock-prompt-role" -Profile "my-aws-profile"

param (
    [Parameter(Mandatory = $true)]
    [string]$RoleName,

    [string]$Profile
)

if (-not $RoleName) {
    Write-Host "Error: --role-name is required"
    exit 1
}

$profileArgs = @()
if ($Profile) {
    $profileArgs = @("--profile", $Profile)
}

# Define the trust policy
$trustPolicy = @"
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
"@

# Write to temporary trust policy file
$tempTrustPolicyFile = "trust-policy-temp.json"
$trustPolicy | Set-Content -Encoding UTF8 $tempTrustPolicyFile

# Create the IAM role
Write-Host "Creating IAM role '$RoleName' for Bedrock..."
aws iam create-role `
    --role-name $RoleName `
    --assume-role-policy-document file://$tempTrustPolicyFile `
    @profileArgs

# Attach inline policy (assumes bedrock-prompt-policy.json is in same directory)
Write-Host "Attaching inline policy (BedrockPromptMinimalPolicy)..."
aws iam put-role-policy `
    --role-name $RoleName `
    --policy-name "BedrockPromptMinimalPolicy" `
    --policy-document file://bedrock-prompt-policy.json `
    @profileArgs

# Get the role ARN
$roleArn = aws iam get-role `
    --role-name $RoleName `
    --query "Role.Arn" `
    --output text `
    @profileArgs

Write-Host "`nDone. Role ARN:"
Write-Host $roleArn

# Cleanup
Remove-Item $tem
