@echo off
setlocal

REM Usage: create_custom_prompt.bat <prompt_json_file> <region> [aws_profile]

set "PROMPT_JSON=%~1"
set "REGION=%~2"
set "AWS_PROFILE=%~3"

if "%PROMPT_JSON%"=="" (
  echo Usage: %~nx0 ^<prompt_json_file^> ^<region^> [aws_profile]
  exit /b 1
)

if "%REGION%"=="" (
  echo Usage: %~nx0 ^<prompt_json_file^> ^<region^> [aws_profile]
  exit /b 1
)

if not exist "%PROMPT_JSON%" (
  echo Error: JSON file "%PROMPT_JSON%" not found.
  exit /b 1
)

echo Creating prompt from JSON file: %PROMPT_JSON%

if "%AWS_PROFILE%"=="" (
  aws bedrock-agent create-prompt --region %REGION% --cli-input-json file://%PROMPT_JSON%
) else (
  aws bedrock-agent create-prompt --region %REGION% --cli-input-json file://%PROMPT_JSON% --profile %AWS_PROFILE%
)

echo Prompt created successfully.
