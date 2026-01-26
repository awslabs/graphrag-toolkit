import yaml
import os.path as osp
from colorama import Fore, Style
import re
import string


def load_yaml(file_path):
    file_path =  file_path if file_path.startswith('/') else osp.join(osp.dirname(osp.abspath(__file__)), file_path)
    with open(file_path, 'r') as file:
        content = yaml.safe_load(file)
    return content

def color_print(text, color):
    print(getattr(Fore, color.upper()) + Style.BRIGHT + text + Style.RESET_ALL)

def parse_response(response, pattern):

    if not isinstance(response, str):
        return []

    match = re.search(pattern, response, flags=re.DOTALL)
    matched = []
    if match:
        graph_text = match.group(1)
        for to_match in graph_text.strip().split('\n'):
            if to_match != "":
                matched.append(to_match)

    return matched

def count_tokens(text: str) -> int:
    """
    Estimate token count for the given text.
    Uses a simple approximation: ~4 characters per token.
    
    Args:
        text: The text to count tokens for
        
    Returns:
        int: Estimated token count
    """
    if not text:
        return 0
    return len(text) // 4

def validate_input_length(input_text: str, max_tokens: int = 32000, input_name: str = "input") -> None:
    """
    Validate that input does not exceed the maximum token limit.
    
    Args:
        input_text: The input text to validate
        max_tokens: Maximum allowed tokens (default: 32000)
        input_name: Name of the input for error message (default: "input")
        
    Raises:
        ValueError: If input exceeds the maximum token limit
    """
    if not input_text:
        return
        
    token_count = count_tokens(input_text)
    
    if token_count > max_tokens:
        raise ValueError(
            f"{input_name} exceeds maximum token limit. "
            f"Provided: ~{token_count} tokens, Maximum: {max_tokens} tokens. "
            f"Please reduce the length of your {input_name}."
        )
