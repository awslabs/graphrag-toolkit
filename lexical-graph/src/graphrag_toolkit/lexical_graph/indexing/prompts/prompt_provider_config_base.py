# prompt_provider_config_base.py
from pydantic import BaseModel
from typing import Optional

class FilePromptProviderConfig(BaseModel):
    """
    Configuration model for file-based prompt providers.

    This class defines the required fields for specifying system and user prompt file names.
    """
    system_prompt_file: str
    user_prompt_file: str

class S3PromptProviderConfig(BaseModel):
    """
    Configuration model for S3-based prompt providers.

    This class defines the required fields for specifying the S3 bucket, key, and optional region for prompt storage.
    """
    bucket: str
    key: str
    region: Optional[str] = None

class BedrockPromptProviderConfig(BaseModel):
    """
    Configuration model for Bedrock-based prompt providers.

    This class defines the required field for specifying the Bedrock prompt ARN.
    """
    prompt_arn: str
