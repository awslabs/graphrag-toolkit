import os
import json
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import FilePromptProviderConfig
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class FilePromptProvider(PromptProvider):
    """
    Loads system and user prompts from the local filesystem using a config object.
    """

    def __init__(self, config: FilePromptProviderConfig, system_prompt_file: str = "system_prompt.txt", user_prompt_file: str = "user_prompt.txt"):
        """
        Initializes the FilePromptProvider with a configuration and prompt file names.

        Args:
            config: The configuration object specifying the base path for prompt files.
            system_prompt_file: The filename for the system prompt (default is "system_prompt.txt").
            user_prompt_file: The filename for the user prompt (default is "user_prompt.txt").

        Raises:
            NotADirectoryError: If the provided base path does not exist or is not a directory.
        """
        if not os.path.isdir(config.base_path):
            raise NotADirectoryError(f"Invalid or non-existent directory: {config.base_path}")
        self.config = config
        self.system_prompt_file = system_prompt_file
        self.user_prompt_file = user_prompt_file

        logger.info(f"[Prompt Debug] Initialized FilePromptProvider")
        logger.info(f"[Prompt Debug] Base path: {self.config.base_path}")
        logger.info(f"[Prompt Debug] System prompt file: {self.system_prompt_file}")
        logger.info(f"[Prompt Debug] User prompt file: {self.user_prompt_file}")

    def _load_prompt(self, filename: str) -> str:
        """
        Loads the contents of a prompt file from the configured base path.

        Args:
            filename: The name of the prompt file to load.

        Returns:
            The contents of the prompt file as a string.

        Raises:
            FileNotFoundError: If the prompt file does not exist.
            OSError: If the file cannot be read.
        """
        path = os.path.join(self.config.base_path, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Prompt file not found: {path}")
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().rstrip()
        except OSError as e:
            raise OSError(f"Failed to read prompt file {path}: {str(e)}") from e

    def get_system_prompt(self) -> str:
        """
        Returns the contents of the system prompt file.

        Returns:
            The contents of the system prompt file as a string.
        """
        return self._load_prompt(self.system_prompt_file)

    def _load_aws_template(self) -> str:
        """
        Loads AWS template from local filesystem if available.
        
        Returns:
            JSON string of the AWS template, or empty string if not found.
        """
        if not self.config.aws_template_file:
            return ""
            
        try:
            template_path = os.path.join(self.config.base_path, self.config.aws_template_file)
            logger.info(f"[Template Debug] Loading AWS template from file: {template_path}")
            
            if not os.path.exists(template_path):
                logger.warning(f"[Template Debug] AWS template file not found: {template_path}")
                return ""
                
            with open(template_path, "r", encoding="utf-8") as f:
                template_content = f.read()
                # Validate it's valid JSON
                json.loads(template_content)
                return template_content
        except Exception as e:
            logger.warning(f"[Template Debug] Could not load AWS template: {e}")
            return ""

    def get_user_prompt(self) -> str:
        """
        Returns the contents of the user prompt file with template substitutions applied.

        Returns:
            The contents of the user prompt file as a string.
        """
        user_prompt = self._load_prompt(self.user_prompt_file)
        
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
