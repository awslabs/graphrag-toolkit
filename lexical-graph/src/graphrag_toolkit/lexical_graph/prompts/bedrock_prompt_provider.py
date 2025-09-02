# graphrag_toolkit/lexical_graph/prompts/bedrock_prompt_provider.py
# SPDX-License-Identifier: Apache-2.0

import json
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import BedrockPromptProviderConfig
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class BedrockPromptProvider(PromptProvider):
    """Provides prompt templates from AWS Bedrock using specified ARNs and versions.
    
    This class loads and returns system and user prompt templates from AWS Bedrock, 
    based on configuration provided at initialization.
    """

    def __init__(self, config: BedrockPromptProviderConfig):
        self.config = config

        logger.info(
            f"[Prompt Debug] Using BedrockPromptProvider with:\n"
            f"  system_prompt_arn={config.system_prompt_arn} "
            f"(resolved={config.resolved_system_prompt_arn}, version={config.system_prompt_version})\n"
            f"  user_prompt_arn={config.user_prompt_arn} "
            f"(resolved={config.resolved_user_prompt_arn}, version={config.user_prompt_version})\n"
            f"  region={config.aws_region}, profile={config.aws_profile}"
        )

    def _load_prompt(self, prompt_arn: str, version: str = None) -> str:
        """Loads a prompt template from AWS Bedrock using the given ARN and version.

        Args:
            prompt_arn: The ARN of the prompt to load.
            version: The version of the prompt to load (optional).

        Returns:
            The text of the loaded prompt template.

        Raises:
            RuntimeError: If the prompt or its text cannot be found or loaded.
        """
        try:
            kwargs = {"promptIdentifier": prompt_arn}
            if version:
                kwargs["promptVersion"] = version

            response = self.config.bedrock.get_prompt(**kwargs)

            variants = response.get("variants", [])
            if not variants:
                raise RuntimeError(f"No variants found for prompt: {prompt_arn}")

            text = variants[0].get("templateConfiguration", {}).get("text", {}).get("text")
            if not text:
                raise RuntimeError(f"Prompt text not found for: {prompt_arn}")

            return text.strip()

        except Exception as e:
            logger.error(f"Failed to load prompt for {prompt_arn}: {str(e)}")
            raise RuntimeError(f"Could not load prompt from Bedrock: {prompt_arn}") from e

    def get_system_prompt(self) -> str:
        """Retrieves the system prompt template from AWS Bedrock.

        Returns:
            The text of the system prompt template.
        """
        return self._load_prompt(
            self.config.resolved_system_prompt_arn,
            self.config.system_prompt_version,
        )

    def _load_aws_template(self) -> str:
        """
        Loads AWS template from S3 if available (BedrockPromptProvider uses S3 for templates).
        
        Returns:
            JSON string of the AWS template, or empty string if not found.
        """
        if not self.config.aws_template_s3_bucket or not self.config.aws_template_s3_key:
            return ""
            
        try:
            logger.info(f"[Template Debug] Loading AWS template from S3: s3://{self.config.aws_template_s3_bucket}/{self.config.aws_template_s3_key}")
            
            s3_client = self.config.s3
            response = s3_client.get_object(Bucket=self.config.aws_template_s3_bucket, Key=self.config.aws_template_s3_key)
            template_content = response["Body"].read().decode("utf-8")
            # Validate it's valid JSON
            json.loads(template_content)
            return template_content
        except Exception as e:
            logger.warning(f"[Template Debug] Could not load AWS template: {e}")
            return ""

    def get_user_prompt(self) -> str:
        """Retrieves the user prompt template from AWS Bedrock with template substitutions applied.

        Returns:
            The text of the user prompt template.
        """
        user_prompt = self._load_prompt(
            self.config.resolved_user_prompt_arn,
            self.config.user_prompt_version,
        )
        
        # Handle AWS template substitution
        if '{aws_template_structure}' in user_prompt:
            aws_template = self._load_aws_template()
            if aws_template:
                # Pretty format the JSON template
                template_obj = json.loads(aws_template)
                formatted_template = json.dumps(template_obj, indent=2)
                user_prompt = user_prompt.replace('{aws_template_structure}', formatted_template)
                logger.info("[Template Debug] AWS template substituted in user prompt")
            else:
                # Remove the placeholder if template not found
                user_prompt = user_prompt.replace('{aws_template_structure}', 'AWS remediation template (template file not found)')
                logger.warning("[Template Debug] AWS template placeholder removed - template not found")
        
        return user_prompt
