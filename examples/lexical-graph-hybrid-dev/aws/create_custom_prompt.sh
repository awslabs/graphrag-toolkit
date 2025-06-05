#!/bin/bash

# Usage:
# ./create_custom_prompt.sh <prompt_json_file> <region> [aws_profile]

set -e

PROMPT_JSON="$1"
REGION="$2"
AWS_PROFILE="$3"

if [[ -z "$PROMPT_JSON" || -z "$REGION" ]]; then
  echo "Usage: $0 <prompt_json_file> <region> [aws_profile]"
  exit 1
fi

if [[ ! -f "$PROMPT_JSON" ]]; then
  echo "Error: JSON file '$PROMPT_JSON' not found."
  exit 1
fi

# Build AWS CLI command
CMD=(aws bedrock-agent create-prompt --region "$REGION" --cli-input-json file://"$PROMPT_JSON")
if [[ -n "$AWS_PROFILE" ]]; then
  CMD+=(--profile "$AWS_PROFILE")
fi

echo "Creating prompt from JSON file: $PROMPT_JSON"
"${CMD[@]}"
echo "Prompt created successfully."
