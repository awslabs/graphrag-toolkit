import boto3
from abc import ABC, abstractmethod
import time

from ..utils import color_print

class BaseGenerator(ABC):
    """
    Base class that implements the LLMs used by GraphRAG.
    """
    def __init__(self):
        pass

    @abstractmethod
    def generate(self):
        raise NotImplementedError("generate method is not implemented")

class BedrockGenerator(BaseGenerator):
    """
    LLMs implemented with Bedrock APIs.
    
    Attributes:
        model_name (str): The name or ID of the Bedrock model to use for generating responses.
        max_tokens (int): The maximum number of new tokens to generate in the response.
        system_prompt (str): The system prompt to provide to the language model.
    """
    def __init__(self, model_name="anthropic.claude-3-7-sonnet-20250219-v1:0", region_name="us-west-2", prefill=False, max_tokens = 4096, max_retries = 10, inference_config=None, reasoning_config=None):
        super().__init__()
        self.model_name = model_name
        self.max_new_tokens = max_tokens
        self.prefill = prefill
        self.max_retries = max_retries
        self.region_name = region_name
        self.inference_config = inference_config
        self.reasoning_config = reasoning_config

    def generate(self, prompt, system_prompt = "You are a helpful AI assistant.",  few_shot_examples=None):
        """
        LLM Generation function
        
        Attributes:
            prompt (str): The propmt to provide to the language model
            system_prompt (str): The system prompt to provide to the language model.
            few_shot_examples (str): few shot demonstrations for in-context learning
        """
        response = generate_llm_response(self.region_name, self.model_name, system_prompt, prompt, self.max_new_tokens, self.max_retries, self.inference_config, self.reasoning_config)
        if "Failed due to other reasons." in response:
            raise Exception(f"{response}")
        return response
        
def generate_llm_response(region_name, model_id, system_prompt, query, max_tokens, max_retries, inference_config=None, reasoning_config=None):
    bedrock_runtime = boto3.client("bedrock-runtime", region_name=region_name)
    
    #TODO: add few shot examples and pre-fill if needed
    messages = []

    user_message = {'role': 'user', "content": [{"text": query}]}
    messages.append(user_message)

    # Build inference config - use provided config or default
    if inference_config is None:
        inference_config = {"maxTokens": max_tokens}
    else:
        # Ensure maxTokens is set if not provided in custom config
        if "maxTokens" not in inference_config:
            inference_config["maxTokens"] = max_tokens

    # Prepare converse parameters
    converse_params = {
        "messages": messages,
        "modelId": model_id,
        "system": [{"text": system_prompt}],
        "inferenceConfig": inference_config
    }
    
    # Add reasoning config if provided (for models that support reasoning)
    if reasoning_config is not None:
        converse_params["additionalModelRequestFields"] = reasoning_config

    
        
    for attempt in range(max_retries):
        try:
            response = bedrock_runtime.converse(**converse_params)
            # content[0] is reasoning content is applicable
            return response['output']["message"]["content"][-1]['text']

        except Exception as e:
            if 'Too many requests' in str(e) or \
                'Model has timed out' in str(e) or \
                ' Read timeout on' in str(e):
                color_print(f"Too many requests", "yellow")
                time.sleep(30)
            elif 'blocked by content filtering policy' in str(e):
                max_retries = 3
            else:
                color_print(f"WARNING: Request failed due to other reasons: {e}", "red")
                return f"{e} [Error] Failed due to other reasons."

        # Retry logic
        if attempt > 0 and attempt%3 == 0:
            color_print(f"Attempt {attempt + 1} failed, retrying...", "yellow")
        time.sleep(30)  # Optional: wait before retrying

    # If all attempts fail, return an empty string or a specific message
    color_print(f"All {max_retries} attempts failed. Failed to generate a response.", "red")
    return "Failed to generate a response after multiple attempts."
