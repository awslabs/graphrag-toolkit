import boto3
import json
from abc import ABC, abstractmethod
import xmltodict
import os
import sys
# Add the parent directory to the Python path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from utils import color_print
import time

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
    def __init__(self, model_name="anthropic.claude-3-sonnet-20240229-v1:0", region_name="us-west-2", prefill=False, max_tokens = 2048, max_retries = 10):
        super().__init__()
        self.model_name = model_name
        self.max_new_tokens = 2048
        self.prefill = prefill
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.region_name = region_name

    def generate(self, prompt, system_prompt = "You are a helpful AI assistant.",  few_shot_examples=None, xml_answer = False):
        """
        LLM Generation function
        
        Attributes:
            prompt (str): The propmt to provide to the language model
            system_prompt (str): The system prompt to provide to the language model.
            few_shot_examples (str): few shot demonstrations for in-context learning
            parse_xml (boolean): whether the llm output is in xml format
        """
        response = generate_llm_response(self.region_name, self.model_name, system_prompt, prompt, self.prefill, self.max_tokens, few_shot_examples, self.max_retries)
        if "Failed due to other reasons." in response:
            raise Exception("LLM call failed due to credentials or other reasons.")
        if xml_answer:
            return parse_xml_answer(response), response
        else:
            return response
        
def generate_llm_response(region_name, model_id, system_prompt, query, prefill, max_tokens, few_shot_examples, max_retries):
    bedrock_runtime = boto3.client("bedrock-runtime", region_name=region_name)
    if "anthropic" in model_id:
        messages = []
        if few_shot_examples:
            for example in few_shot_examples:
                # Each example should be structured as a user-assistant pair
                # Assuming each example is a dictionary with keys 'user' and 'assistant'
                user_message = {'role': 'user', 'content': example['user']}
                assistant_message = {'role': 'assistant', 'content': example['assistant']}
                messages.append(user_message)
                messages.append(assistant_message)
        user_message = {'role': 'user', 'content': query}
        messages.append(user_message)
        if prefill:
            assistant_prefill = {'role': 'assistant', 'content': '<answer>\n<answer_part>\n<text>'}
            messages.append(assistant_prefill)

        body=json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": messages,
                "temperature": 0
            }  
        )

    if "meta" in model_id:
        prompt = f"""
            <|begin_of_text|>
            <|start_header_id|>system<|end_header_id|>
            {system_prompt}
            <|eot_id|>
        """
        prompt += f"""
            <|begin_of_text|>
            <|start_header_id|>user<|end_header_id|>
            {query}
            <|eot_id|>
            <|start_header_id|>assistant<|end_header_id|>
        """

        body = json.dumps(
            {
                "prompt": prompt,
                "max_gen_len": max_tokens,
            }
        )

    if "mistral" in model_id:
        
        prompt = f"\n\n<<SYS>>{system_prompt}<<SYS>>"
        prompt += f"""
            <s>[INST]
            {query}
            [/INST]
        """

        body = json.dumps(
            {
                "prompt": prompt,
                "max_tokens": max_tokens,
            }
        )
        
    for attempt in range(max_retries):
        try:
            response = bedrock_runtime.invoke_model(body=body, modelId=model_id)
            response_body = json.loads(response.get('body').read())

            if "anthropic" in model_id:
                # Check if 'content' key exists and is a list with at least one element
                if 'content' in response_body and len(response_body['content']) > 0:
                    response = response_body['content'][0].get('text', '')  # Get 'text' safely
                    if response:  # If response is not empty
                        return response
                    else:
                        print("Warning: 'text' is empty in the response body")
                else:
                    print("Warning: 'content' key is missing or empty in the response body")

            elif "meta" in model_id:
                if 'generation' in response_body:
                    response = response_body['generation']
                    if response:
                        return response
                    else:
                        print("Warning: 'generation' is empty in the response body")
                else:
                    print("Warning: 'generation' key is missing in the response body")
            
            elif "mistral" in model_id:
                outputs = response_body.get('outputs', [])
                if len(outputs) > 0 and 'text' in outputs[0]:
                    response = outputs[0]['text']
                    if response:
                        return response
                    else:
                        print("Warning: 'text' is empty in the response body")
                else:
                    print("Warning: 'outputs' key is missing or empty in the response body")

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
                return "Failed due to other reasons."

        # Retry logic
        if attempt > 0 and attempt%3 == 0:
            color_print(f"Attempt {attempt + 1} failed, retrying...", "yellow")
        time.sleep(30)  # Optional: wait before retrying

    # If all attempts fail, return an empty string or a specific message
    color_print(f"All {max_retries} attempts failed. Failed to generate a response.", "red")
    return "Failed to generate a response after multiple attempts."