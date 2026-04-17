# Usage:
# .\create_custom_prompt.ps1 <prompt_json_file> <region> [aws_profile]

param(
    [Parameter(Mandatory = $true)]
    [string]$PromptJson,

    [Parameter(Mandatory = $true)]
    [string]$Region,

    [string]$AwsProfile
)

if (-not (Test-Path $PromptJson)) {
    Write-Host "Error: JSON file '$PromptJson' not found."
    exit 1
}

Write-Host "Creating prompt from JSON file: $PromptJson"

$cmd = @(
    "aws", "bedrock-agent", "create-prompt",
    "--region", $Region,
    "--cli-input-json", "file://$PromptJson"
)

if ($AwsProfile) {
    $cmd += @("--profile", $AwsProfile)
}

& $cmd

Write-Host "Prompt created successfully."
